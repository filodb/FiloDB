package filodb.http

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCodes => Codes}
import akka.http.scaladsl.server.Directives._
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport

import filodb.coordinator.NodeClusterActor.InternalServiceError
import filodb.coordinator.v2.{ClusterCardinalities, GetClusterCardinalities}
import filodb.core.DatasetRef
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityStore}
import filodb.http.apiv1.HttpSchema
import filodb.query.TsCardinalities

/**
 * One (ws, ns, metric) group and its cluster-wide cardinality counts.
 *
 * Count names match the TsCardinalitiesSamplV2 shape used by the existing
 * /api/v2/metering/cardinality/timeseries API, so callers can share a decoder.
 */
final case class HttpCardinality(group: Map[String, String], cardinality: Map[String, Long])

/**
 * Serves cluster-wide time series cardinality without going through the QueryActor, the query
 * planner, or the query scheduler. Reads the per-shard cardinality stores directly via a
 * scatter to every node's NewNodeCoordinatorActor, and merges the result here.
 *
 * Only available with ClusteringV2 - the scatter relies on the deterministic ordinal-to-shard
 * assignment in FiloDbClusterDiscovery.
 */
class CardinalityRoute(coordinatorActor: ActorRef,
                       v2ClusterEnabled: Boolean,
                       settings: HttpSettings) extends FiloRoute with StrictLogging {
  import FailFastCirceSupport._
  import io.circe.generic.auto._

  import HttpSchema._
  import filodb.coordinator.client.Client._

  private val clusterType = settings.filoSettings.config.getString("cluster-type")
  // Cardinality on a downsample cluster is a long term count. The raw cluster tracks
  // active/billable/shortTerm instead. Same fork as TsCardExec.
  private val isDownsample = clusterType.toLowerCase.contains("downsample")

  val route = pathPrefix("api" / "v1" / "cardinality") {
    // GET /api/v1/cardinality/<dataset>/timeseries?numGroupByFields=2&_ws_=..&_ns_=..
    //   &limit=..&sortBy=active|total&allowPartial=false
    //
    // Returns ws/ns (or metric) level cardinality for the whole cluster in a single call.
    // Fails with 503 when any shard is not active, since the counts would be an undercount.
    path(Segment / "timeseries") { dataset =>
      get {
        parameter(("numGroupByFields".as[Int], "_ws_".as[String].?, "_ns_".as[String].?,
                   "limit".as[Int].?, "sortBy".as[String].?, "allowPartial".as[Boolean].?))
        { (numGroupByFields, ws, ns, limit, sortBy, allowPartial) =>
          if (!v2ClusterEnabled) {
            complete(Codes.NotImplemented ->
              httpErr("NotSupported", "Cardinality API requires the ClusteringV2 shard assignment strategy"))
          } else {
            // prefix is positional: ws, then ns. An ns without a ws is not a valid prefix.
            val prefix = (ws, ns) match {
              case (Some(w), Some(n)) => Seq(w, n)
              case (Some(w), None)    => Seq(w)
              case _                  => Nil
            }
            badRequest(numGroupByFields, ws, ns, prefix) match {
              case Some(err) => complete(Codes.BadRequest -> httpErr("BadArgument", err))
              case None =>
                val cmd = GetClusterCardinalities(DatasetRef.fromDotString(dataset), prefix, numGroupByFields)
                val partialOk = allowPartial.contains(true)
                onSuccess(asyncAsk(coordinatorActor, cmd, settings.queryAskTimeout)) {
                  case cc: ClusterCardinalities if cc.missingShards.nonEmpty && !partialOk =>
                    complete(Codes.ServiceUnavailable -> httpErr("IncorrectCount",
                      s"Cardinality count for dataset $dataset would be incorrect: " +
                      s"${cc.missingShards.size} shard(s) not active or unreachable " +
                      s"[${cc.missingShards.mkString(",")}]. " +
                      s"Retry, or pass allowPartial=true to accept an undercount."))
                  case cc: ClusterCardinalities =>
                    complete(httpList(present(cc, dataset, limit, sortBy)))
                  case InternalServiceError(msg) =>
                    complete(Codes.ServiceUnavailable -> httpErr("InternalServerError", msg))
                  case other =>
                    logger.error(s"Unexpected response to $cmd: $other")
                    complete(Codes.InternalServerError ->
                      httpErr("InternalServerError", s"Unexpected response for dataset $dataset"))
                }
            }
          }
        }
      }
    }
  }

  /**
   * Mirrors the require()s on the TsCardinalities logical plan, so this API rejects the same
   * prefix/depth combinations the PromQL path does.
   */
  private def badRequest(numGroupByFields: Int,
                         ws: Option[String],
                         ns: Option[String],
                         prefix: Seq[String]): Option[String] = {
    if (ws.isEmpty && ns.isDefined) {
      Some("_ns_ cannot be specified without _ws_")
    } else if (numGroupByFields < 1 || numGroupByFields > 3) {
      Some("numGroupByFields must lie on [1, 3]")
    } else if (numGroupByFields < prefix.size) {
      Some("numGroupByFields must indicate a depth at least as deep as the given prefix")
    } else if (numGroupByFields == 3 && prefix.size < 2) {
      Some("cannot group at the metric level without both _ws_ and _ns_")
    } else None
  }

  /**
   * Turns merged records into the response shape: names the prefix fields, applies the
   * downsample count remap, then sorts and truncates.
   */
  private def present(cc: ClusterCardinalities,
                      dataset: String,
                      limit: Option[Int],
                      sortBy: Option[String]): Seq[HttpCardinality] = {
    val byTotal = sortBy.forall(_ != "active")
    val sorted = cc.cardinalities.sortBy { r =>
      -(if (byTotal) r.value.tsCount else r.value.activeTsCount)
    }
    sorted.take(limit.getOrElse(CardinalityStore.MAX_RESULT_SIZE)).map(toHttpCardinality(_, dataset))
  }

  private def toHttpCardinality(rec: CardinalityRecord, dataset: String): HttpCardinality = {
    val group = TsCardinalities.SHARD_KEY_LABELS.take(rec.prefix.size)
      .zip(rec.prefix).toMap + ("_type_" -> dataset)
    // NOTE: the downsample cluster stores its count in tsCount, but from a user's point of view
    // that is the longterm count. Keep this consistent with TsCardExec.
    val counts =
      if (isDownsample) {
        Map("active" -> 0L, "billable" -> 0L, "shortTerm" -> 0L, "longTerm" -> rec.value.tsCount)
      } else {
        Map("active" -> rec.value.activeTsCount,
            "billable" -> rec.value.billableTsCount,
            "shortTerm" -> rec.value.tsCount,
            "longTerm" -> 0L)
      }
    HttpCardinality(group, counts)
  }
}

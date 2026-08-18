package filodb.http

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCodes => Codes}
import akka.http.scaladsl.server.Directives._
import com.typesafe.scalalogging.StrictLogging
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.circe.Printer

import filodb.coordinator.NodeClusterActor.InternalServiceError
import filodb.coordinator.client.IngestionCommands.UnknownDataset
import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.coordinator.v2.{ClusterCardinalities, GetClusterCardinalities}
import filodb.core.DatasetRef
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}
import filodb.core.query.QueryContext
import filodb.http.apiv1.HttpSchema
import filodb.memory.format.RowReader
import filodb.prometheus.parse.Parser
import filodb.query.{MetadataSuccessResponse, QueryError, QueryResult, TsCardinalities, TsCardinalitiesSamplV2}
import filodb.query.exec.TsCardExec
import filodb.query.exec.TsCardExec.RowData

/**
 * Serves time series cardinality with the same request/response semantics as the user facing
 * /api/v2/metering/cardinality/timeseries API in filodb-query-service, so that callers (and the
 * MetadataRemoteExec decoder) need no special casing.
 *
 * Two paths are exposed, and they are expected to return identical results:
 *
 *  /promql/<dataset>/api/v2/metering/cardinality/timeseries
 *      DIRECT - scatters to every node's cardinality store, bypassing the QueryActor, the query
 *      planner and the query scheduler. Requires ClusteringV2.
 *
 *  /promql/<dataset>/api/v2/metering/query/cardinality/timeseries
 *      QUERY - builds a TsCardinalities logical plan and hands it to the QueryActor, which
 *      materializes one TsCardExec per shard and reduces them. The original path.
 *
 * NOTE on `datasets`: the user facing API accepts a CSV of raw/aggregated/recording-rules to fan
 * out across clusters. That is a query-service concern - here the dataset comes from the path, so
 * the parameter is not accepted.
 *
 * NOTE on downsample: neither path reaches the downsample cluster. Both answer from this cluster's
 * own shards only, so `longTerm` is always 0 in the response.
 */
class CardinalityRoute(coordinatorActor: ActorRef,
                       v2ClusterEnabled: Boolean,
                       settings: HttpSettings) extends FiloRoute with StrictLogging {
  import FailFastCirceSupport._
  import io.circe.generic.auto._

  import HttpSchema._
  import filodb.coordinator.client.Client._

  private val clusterType = settings.filoSettings.config.getString("cluster-type")

  implicit val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  val route = pathPrefix("promql" / Segment) { dataset =>
    // NOTE: the /query/ path is declared first - akka-http path matching is order sensitive and
    // "cardinality" / "timeseries" would otherwise not reach it.
    path("api" / "v2" / "metering" / "query" / "cardinality" / "timeseries") {
      get {
        parameter(("match[]".as[String], "numGroupByFields".as[Int], "verbose".as[Boolean].?)) {
          (matcher, numGroupByFields, _) =>
            withPrefix(matcher, numGroupByFields) { prefix =>
              val ref = DatasetRef.fromDotString(dataset)
              // NOTE: no overrideClusterName, so this stays on the QueryActor's own
              // SingleClusterPlanner ("raw"). It deliberately does NOT go through
              // LongTimeRangePlanner, which would also materialize a downsample plan.
              val lp = TsCardinalities(prefix, numGroupByFields)
              val cmd = LogicalPlan2Query(ref, lp, queryContextForCardinality())
              onSuccess(asyncAsk(coordinatorActor, cmd, settings.queryAskTimeout)) {
                case qr: QueryResult =>
                  val recs = qr.result.flatMap(_.rows().map(rowDataToRecord).toSeq)
                  complete(MetadataSuccessResponse(present(recs, dataset)))
                case qe: QueryError =>
                  logger.error(s"QueryError for $cmd", qe.t)
                  complete(Codes.InternalServerError -> httpErr(qe.t.getClass.getName, qe.t.getMessage))
                case UnknownDataset =>
                  complete(Codes.NotFound -> httpErr("DatasetUnknown", s"Dataset $dataset is not registered"))
                case other =>
                  logger.error(s"Unexpected response to $cmd: $other")
                  complete(Codes.InternalServerError ->
                    httpErr("InternalServerError", s"Unexpected response for dataset $dataset"))
              }
            }
        }
      }
    } ~
    path("api" / "v2" / "metering" / "cardinality" / "timeseries") {
      get {
        // allowPartial is an addition over the user facing API: without it, a cluster with any
        // non-active shard is refused rather than answered with an undercount.
        parameter(("match[]".as[String], "numGroupByFields".as[Int], "verbose".as[Boolean].?,
                   "allowPartial".as[Boolean].?)) {
          (matcher, numGroupByFields, _, allowPartial) =>
            if (!v2ClusterEnabled) {
              complete(Codes.NotImplemented -> httpErr("NotSupported",
                "Direct cardinality path requires the ClusteringV2 shard assignment strategy. " +
                "Use api/v2/metering/query/cardinality/timeseries instead."))
            } else {
              withPrefix(matcher, numGroupByFields) { prefix =>
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
                    complete(MetadataSuccessResponse(present(cc.cardinalities, dataset),
                      partial = if (cc.missingShards.isEmpty) None else Some(true),
                      message = if (cc.missingShards.isEmpty) None
                                else Some(s"shards not counted: ${cc.missingShards.mkString(",")}")))
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
   * Turns the match[] selector into a shard key prefix, using the same parser and the same
   * ordering rules as filodb-query-service: the equal-label map is read in SHARD_KEY_LABELS order
   * (_ws_, _ns_, __name__) and must not have gaps, so _ns_ cannot be given without _ws_.
   */
  private def withPrefix(matcher: String, numGroupByFields: Int)
                        (inner: Seq[String] => akka.http.scaladsl.server.Route)
                        : akka.http.scaladsl.server.Route = {
    val fieldMap =
      try Parser.queryToEqualLabelMap(matcher)
      catch { case e: Exception =>
        logger.debug(s"could not parse match[]=$matcher", e)
        Map.empty[String, String]
      }
    // take shard key labels while present, so {_ns_="x"} with no _ws_ yields an empty prefix
    val prefix = TsCardinalities.SHARD_KEY_LABELS.map(fieldMap.get).takeWhile(_.isDefined).flatten

    badRequest(numGroupByFields, fieldMap, prefix, matcher) match {
      case Some(err) => complete(Codes.BadRequest -> httpErr("BadArgument", err))
      case None      => inner(prefix)
    }
  }

  /**
   * Mirrors the require()s on the TsCardinalities logical plan plus the query-service validation,
   * so both paths reject the same inputs before any work is dispatched.
   */
  private def badRequest(numGroupByFields: Int,
                         fieldMap: Map[String, String],
                         prefix: Seq[String],
                         matcher: String): Option[String] = {
    if (fieldMap.isEmpty && prefix.isEmpty && matcher.replaceAll("[{} ]", "").nonEmpty) {
      Some(s"match[] is not a valid selector of equality matchers: $matcher")
    } else if (prefix.size != fieldMap.size) {
      Some("match[] shard key labels must be contiguous: _ns_ requires _ws_, " +
           "and __name__ requires both _ws_ and _ns_")
    } else if (numGroupByFields < 1 || numGroupByFields > 3) {
      Some("numGroupByFields must lie on [1, 3]")
    } else if (numGroupByFields < prefix.size) {
      Some("numGroupByFields must indicate a depth at least as deep as the match[] prefix")
    } else if (numGroupByFields == 3 && prefix.size < 2) {
      Some("cannot group at the metric level without both _ws_ and _ns_ in match[]")
    } else None
  }

  private def queryContextForCardinality(): QueryContext = {
    val partition = settings.filoSettings.config.getString("partition")
    if (partition.isEmpty) QueryContext()
    else QueryContext(traceInfo = Map(TsCardExec.FILODB_PARTITION_KEY -> partition))
  }

  /**
   * Reverses TsCardExec's group encoding ("ws,ns,metric,dataset") back into a CardinalityRecord so
   * the query path shares the presentation code with the direct path.
   */
  private def rowDataToRecord(rr: RowReader): CardinalityRecord = {
    val data = RowData.fromRowReader(rr)
    // the group is the prefix values plus a trailing dataset name
    val prefix = data.group.toString.split(TsCardExec.PREFIX_DELIM).dropRight(1).toSeq
    CardinalityRecord(-1, prefix,
      CardinalityValue(tsCount = data.counts.shortTerm, activeTsCount = data.counts.active,
        billableTsCount = data.counts.billable, childrenCount = 0, childrenQuota = 0))
  }

  private def present(records: Seq[CardinalityRecord], dataset: String): Seq[TsCardinalitiesSamplV2] =
    records.map { rec =>
      val group = TsCardinalities.SHARD_KEY_LABELS.take(rec.prefix.size).zip(rec.prefix).toMap
      // NOTE: unlike the user facing API, neither of these endpoints reaches the downsample
      // cluster, so there is no longterm count to report and no downsample -> longTerm remap
      // (TsCardExec does that only when serving a downsample cluster). longTerm is always 0.
      val counts = Map("active" -> rec.value.activeTsCount,
                       "billable" -> rec.value.billableTsCount,
                       "shortTerm" -> rec.value.tsCount,
                       "longTerm" -> 0L)
      // `dataset` is the user facing cluster flavour (raw/aggregated/recordingrules) and `_type` is
      // the internal filodb dataset name - MetadataRemoteExec reads _type to rebuild the group key.
      TsCardinalitiesSamplV2(group = group, cardinality = counts, dataset = clusterType, _type = dataset)
    }
}

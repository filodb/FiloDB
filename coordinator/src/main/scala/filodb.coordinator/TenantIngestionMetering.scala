package filodb.coordinator

import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.Scheduler.Implicits.{global => scheduler}
import monix.reactive.Observable

import filodb.coordinator.client.Client
import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.core.DatasetRef
import filodb.query.{QueryError, QueryResult, TsCardinalities}

object TenantIngestionMetering {
  protected val METRIC_ACTIVE = "active_timeseries_by_tenant"
  protected val METRIC_TOTAL = "total_timeseries_by_tenant"
  protected val PARALLELISM = 8  // number of datasets queried in parallel

  // QueryThrottle args
  protected val INTERVAL_DIFF = FiniteDuration(5L, TimeUnit.MINUTES)
  protected val LOOKBACK = 10
  protected val TIMEOUT_THRESHOLD = 0.15
}

/**
 * Periodically queries a node for all namespace cardinalities.
 * Kamon gauges are updated with the response data.
 *
 * The intent is to publish a low-cardinality metric such that namespace
 * cardinality queries can be efficiently answered.
 *
 * @param dsIterProducer produces an iterator to step through all datasets.
 * @param coordActorProducer produces a single actor to ask a query. Actors are
 *          queried in the order they're returned from this function.
 */
class TenantIngestionMetering(settings: FilodbSettings,
                              dsIterProducer: () => Iterator[DatasetRef],
                              coordActorProducer: () => ActorRef) extends StrictLogging{
  import TenantIngestionMetering._

  private val clusterType = settings.config.getString("cluster-type")
  private var queryAskTimeSec = -1L  // unix time of the most recent query ask
  private val queryThrottle = new QueryThrottle(
    FiniteDuration(settings.config.getDuration("metering-query-interval").toSeconds,
      TimeUnit.SECONDS),
    INTERVAL_DIFF,
    TIMEOUT_THRESHOLD,
    LOOKBACK)

  // immediately begin periodically querying for / publishing cardinality data
  queryAndSchedule()

  // scalastyle:off method.length
  /**
   * For each dataset, asks a Coordinator with a TsCardinalities LogicalPlan.
   * A publish job is sob is scheduled for each response, and the next batch of
   *   queries is scheduled after all responses are processed/published.
   */
  private def queryAndSchedule() : Unit = {
    import filodb.query.exec.TsCardExec._

    // Nil prefix in order to query all client-owned workspaces;
    // numGroupByFields = 2 to group by (ws, ns)
    val tsCardQuery = TsCardinalities(Nil, 2)

    // use this later to find total elapsed time
    queryAskTimeSec = java.time.Clock.systemUTC().instant().getEpochSecond

    Observable.fromIterator(dsIterProducer()).mapAsync(PARALLELISM){ dsRef =>
      // Asynchronously query each dataset; store (dsRef, queryResult) pairs
      Task{
        val qres = Client.actorAsk(
          coordActorProducer(),
          LogicalPlan2Query(dsRef, tsCardQuery),
          queryThrottle.getInterval()){
            case t: Try[Any] => t
          }
        (dsRef, qres)
      }
    }.foreach { case (dsRef, qres) => qres match {
      // process the query results one-at-a-time (prevents the need for locks in QueryThrottle)
      case Success(qresp) =>
        queryThrottle.recordOnTime()
        qresp match {
          case QueryResult(_, _, rv, _, _, _) =>
            rv.foreach(_.rows().foreach { rr =>
              // publish a cardinality metric for each namespace
              val data = RowData.fromRowReader(rr)
              val prefix = data.group.toString.split(PREFIX_DELIM)
              val tags = Map(
                "metric_ws" -> prefix(0),
                "metric_ns" -> prefix(1),
                "dataset" -> dsRef.dataset,
                "cluster_type" -> clusterType)
              Kamon.gauge(METRIC_ACTIVE).withTags(TagSet.from(tags)).update(data.counts.active.toDouble)
              Kamon.gauge(METRIC_TOTAL).withTags(TagSet.from(tags)).update(data.counts.total.toDouble)
            })
          case QueryError(_, _, t) => logger.warn("QueryError: " + t.getMessage)
        }
      case Failure(t) =>
        logger.warn("Failure: " + t.getMessage)
        if (t.isInstanceOf[TimeoutException]) {
          queryThrottle.recordTimeout()
        } else {
          queryThrottle.recordOnTime()
        }
      // required to compile
      case _ => throw new IllegalArgumentException("should never reach here; attempted to match: " + qres)
      }
    }.onComplete { _ =>
      // Schedule the next query batch at the beginning of the next interval.
      // Note: this "batch delay" setup is intended to keep the TIM config simple; only metering-query-interval
      //   needs to be configured. But it assumes the time required to sequentially process each
      //   response is negligible with respect to metering-query-interval.
      val elapsedSec = java.time.Clock.systemUTC().instant().getEpochSecond - queryAskTimeSec
      scheduler.scheduleOnce(
        math.max(0, queryThrottle.getInterval().toSeconds - elapsedSec),
        TimeUnit.SECONDS,
        () => queryAndSchedule())
    }
  }
  // scalastyle:on method.length
}

package filodb.coordinator

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.tag.TagSet
import monix.execution.Scheduler.Implicits.{global => scheduler}

import filodb.coordinator.client.Client
import filodb.coordinator.client.QueryCommands.LogicalPlan2Query
import filodb.core.DatasetRef
import filodb.query.{QueryError, QueryResult, TsCardinalities}

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
case class TenantIngestionMetering(settings: FilodbSettings,
                                   dsIterProducer: () => Iterator[DatasetRef],
                                   coordActorProducer: () => ActorRef) extends StrictLogging{

  private val ASK_TIMEOUT = FiniteDuration(
    settings.config.getDuration("metering-query-interval").toSeconds,
    TimeUnit.SECONDS)
  private val SCHED_INIT_DELAY = ASK_TIMEOUT  // time until first job is scheduled
  private val SCHED_DELAY = ASK_TIMEOUT  // time between all jobs after the first

  private val CLUSTER_TYPE = settings.config.getString("cluster-type")

  private val METRIC_ACTIVE = "tsdb_metering_active_timeseries"
  private val METRIC_TOTAL = "tsdb_metering_total_timeseries"

  def schedulePeriodicPublishJob() : Unit = {
    // NOTE: the FiniteDuration overload of scheduleWithFixedDelay
    //  does not work. Unsure why, but that's why these FiniteDurations are
    //  awkwardly parsed into seconds.
    scheduler.scheduleWithFixedDelay(
      SCHED_INIT_DELAY.toSeconds,
      SCHED_DELAY.toSeconds,
      TimeUnit.SECONDS,
      () => queryAndSchedulePublish())
  }

  /**
   * For each dataset, ask a Coordinator with a TsCardinalities LogicalPlan.
   * Schedules a job to publish the Coordinator's response.
   */
  private def queryAndSchedulePublish() : Unit = {
    import filodb.query.exec.TsCardExec._
    val numGroupByFields = 2  // group cardinalities at the second level (i.e. ws & ns)
    val prefix = Nil  // query for cardinalities regardless of first-level name (i.e. ws name)
    dsIterProducer().foreach { dsRef =>
      val fut = Client.asyncAsk(
        coordActorProducer(),
        LogicalPlan2Query(dsRef, TsCardinalities(prefix, numGroupByFields)),
        ASK_TIMEOUT)
      fut.onComplete {
        case Success(QueryResult(_, _, rv, _, _, _)) =>
          rv.foreach(_.rows().foreach{ rr =>
            // publish a cardinality metric for each namespace
            val data = RowData.fromRowReader(rr)
            val prefix = data.group.toString.split(PREFIX_DELIM)
            val tags = Map("metric_ws" -> prefix(0),
                           "metric_ns" -> prefix(1),
                           "dataset" -> dsRef.dataset,
                           "cluster_type" -> CLUSTER_TYPE)
            Kamon.gauge(METRIC_ACTIVE).withTags(TagSet.from(tags)).update(data.counts.active.toDouble)
            Kamon.gauge(METRIC_TOTAL).withTags(TagSet.from(tags)).update(data.counts.total.toDouble)
          })
        case Success(QueryError(_, _, t)) => logger.warn("QueryError: " + t.getMessage)
        case Failure(t) => logger.warn("Failure: " + t.getMessage)
        // required to compile
        case _ => throw new IllegalArgumentException("should never reach here; attempted to match: " + fut)
      }
    }
  }
}

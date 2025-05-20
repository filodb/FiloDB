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
import filodb.core.query.QueryContext
import filodb.query.{QueryError, QueryResult, TsCardinalities}
import filodb.query.exec.TsCardExec._

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
case class TenantIngestionMetering(
                                    settings: FilodbSettings,
                                    dsIterProducer: () => Iterator[DatasetRef],
                                    coordActorProducer: () => ActorRef) extends StrictLogging {

  private val ASK_TIMEOUT = FiniteDuration(
    settings.config.getDuration("metering-query-interval").toSeconds,
    TimeUnit.SECONDS)
  private val SCHED_INIT_DELAY = ASK_TIMEOUT  // time until first job is scheduled
  private val SCHED_DELAY = ASK_TIMEOUT  // time between all jobs after the first

  private val CLUSTER_TYPE = settings.config.getString("cluster-type")
  private val FILODB_PARTITION = settings.config.getString("partition")

  private val METRIC_ACTIVE = "tsdb_metering_active_timeseries"
  private val METRIC_TOTAL = "tsdb_metering_total_timeseries"
  private val METRIC_LONGTERM = "tsdb_metering_longterm_timeseries"
  // Quota-aligned metrics
  private val METRIC_SAMPLES_INGESTED = "tsdb_metering_samples_ingested_per_min"
  private val METRIC_QUERY_BYTES_SCANNED = "tsdb_metering_query_samples_scanned_per_min"
  private val METRIC_RETAINED_TIMESERIES = "tsdb_metering_retained_timeseries"

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

  private def getQueryContextForMetering(): QueryContext = {
    if (FILODB_PARTITION.isEmpty) QueryContext()
    else {
      val traceInfoMap = Map( FILODB_PARTITION_KEY -> FILODB_PARTITION)
      QueryContext(traceInfo = traceInfoMap)
    }
  }

  private def publishMetrics(dsRef: DatasetRef, ws: String, ns: String, data: RowData): Unit = {
    val tags = Map(
      "metric_ws" -> ws,
      "metric_ns" -> ns,
      "dataset" -> Option(dsRef.dataset).getOrElse("unknown"),
      "cluster_type" -> CLUSTER_TYPE,
      "_ws_" -> ws,
      "_ns_" -> ns
    )

    if (CLUSTER_TYPE == "downsample") {
      Kamon.gauge(METRIC_LONGTERM).withTags(TagSet.from(tags)).update(data.counts.longTerm.toDouble)
    }
    else {
      Kamon.gauge(METRIC_ACTIVE).withTags(TagSet.from(tags)).update(data.counts.active.toDouble)
      Kamon.gauge(METRIC_TOTAL).withTags(TagSet.from(tags)).update(data.counts.shortTerm.toDouble)

      // Update retained timeseries count - directly maps to active timeseries
      Kamon.gauge(METRIC_RETAINED_TIMESERIES)
        .withTags(TagSet.from(tags))
        .update(data.counts.active.toDouble)

      // Update samples ingested per minute based on active timeseries and ingestion resolution
      val ingestResolutionMillis = settings.config.getLong("ingest-resolution-millis")
      val samplesPerMin = data.counts.active * (60000.0 / ingestResolutionMillis)
      Kamon.gauge(METRIC_SAMPLES_INGESTED)
        .withTags(TagSet.from(tags))
        .update(samplesPerMin)

      // Update query bytes scanned per minute based on active timeseries and configured max data per shard
      val maxDataPerShardQuery = settings.config.getBytes("max-data-per-shard-query").longValue()
      val avgBytesPerTs = maxDataPerShardQuery / 1000000 // Convert to MB for better readability
      val MegabytesScannedPerMin = data.counts.active * avgBytesPerTs
      Kamon.gauge(METRIC_QUERY_BYTES_SCANNED)
        .withTags(TagSet.from(tags))
        .update(MegabytesScannedPerMin)

      logger.debug(s"Published quota metrics for ws=$ws ns=$ns: " +
      s"retained=${data.counts.active}, " +
      s"samples_per_min=${samplesPerMin}, " +
      s"bytes_scanned_per_min=${MegabytesScannedPerMin}")
    }
  }

  private def handleQueryResponse(dsRef: DatasetRef, response: Any): Unit = response match {
    case QueryResult(_, _, rv, _, _, _, _) =>
      rv.foreach(_.rows().foreach { rr =>
        val data = RowData.fromRowReader(rr)
        val prefix = data.group.toString.split(PREFIX_DELIM)
        val ws = prefix(0)
        val ns = prefix(1)
        publishMetrics(dsRef, ws, ns, data)
      })
    case QueryError(_, _, t) => logger.warn("QueryError: " + t.getMessage)
    case _ => throw new IllegalArgumentException("should never reach here; attempted to match: " + response)
  }

  /**
   * For each dataset, ask a Coordinator with a TsCardinalities LogicalPlan.
   * Schedules a job to publish the Coordinator's response.
   */
  private def queryAndSchedulePublish() : Unit = {
    val numGroupByFields = 2  // group cardinalities at the second level (i.e. ws & ns)
    val prefix = Nil  // query for cardinalities regardless of first-level name (i.e. ws name)
    dsIterProducer().foreach { dsRef =>
      val fut = Client.asyncAsk(
        coordActorProducer(),
        LogicalPlan2Query(
          dsRef,
          TsCardinalities(prefix, numGroupByFields, overrideClusterName = CLUSTER_TYPE),
          getQueryContextForMetering()
        ),
        ASK_TIMEOUT)
      fut.onComplete {
        case Success(result) => handleQueryResponse(dsRef, result)
        case Failure(t) => logger.warn("Failure: " + t.getMessage)
      }
    }
  }
}
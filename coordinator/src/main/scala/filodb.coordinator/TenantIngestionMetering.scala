package filodb.coordinator

import java.util.concurrent.{TimeoutException, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock

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

object QueryThrottle {
  // currently just add this diff to the delay if the timeout rate exceeds THRESHOLD
  val DELAY_DIFF = FiniteDuration(5L, TimeUnit.MINUTES)
  // number of past query timeouts/non-timeouts to consider
  val LOOKBACK = 10
  // {non-timeouts-in-lookback-window} / LOOKBACK < THRESHOLD will adjust the delay
  val THRESHOLD = 0.85
}

/**
 * Throttles the TenantIngestionMetering query rate according to the ratio of timeouts to non-timeouts.
 *
 * @param queryDelay the initial delay between each query. This is the duration to be adjusted.
 */
class QueryThrottle(queryDelay: FiniteDuration) extends StrictLogging {
  import QueryThrottle._

  private var delay: FiniteDuration = queryDelay.copy()
  private val lock = new ReentrantReadWriteLock()

  // these track timeouts for the past LOOKBACK queries
  private var bits = (1 << LOOKBACK) - 1
  private var ibit = 0

  /**
   * Sets the next lookback bit and increments ibit.
   */
  private def setNextBit(bit: Boolean): Unit = {
    val bitVal = if (bit) 1 else 0
    bits = bits & ~(1 << ibit)      // zero the bit
    bits = bits | (bitVal << ibit)  // 'or' in the new bit
    ibit = ibit + 1
    if (ibit == LOOKBACK) {
      ibit = 0
    }
  }

  /**
   * Updates the delay according to the timeout:non-timeoout ratio.
   */
  private def updateDelay(): Unit = {
    val successRate = Integer.bitCount(bits).toDouble / LOOKBACK
    if (successRate < THRESHOLD) {
      delay = delay + DELAY_DIFF
      logger.info("too many timeouts; query delay extended to " + delay.toString())
      // reset the bits
      bits = (1 << LOOKBACK) - 1
    }
  }

  /**
   * Record a query timeout.
   */
  def recordTimeout(): Unit = {
    lock.writeLock().lock()
    setNextBit(false)
    updateDelay()
    lock.writeLock().unlock()
  }

  /**
   * Record a query non-timeout.
   */
  def recordOnTime(): Unit = {
    lock.writeLock().lock()
    setNextBit(true)
    updateDelay()
    lock.writeLock().unlock()
  }

  /**
   * Returns the current query delay.
   */
  def getDelay(): FiniteDuration = {
    lock.readLock().lock()
    val currDelay = delay.copy()
    lock.readLock().unlock()
    return currDelay
  }
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

  // time until first query executes
  private val SCHED_INIT_DELAY = FiniteDuration(
    settings.config.getDuration("metering-query-interval").toSeconds,
    TimeUnit.SECONDS)

  private val CLUSTER_TYPE = settings.config.getString("cluster-type")

  private val METRIC_ACTIVE = "active_timeseries_by_tenant"
  private val METRIC_TOTAL = "total_timeseries_by_tenant"

  private val queryLimiter = new QueryThrottle(SCHED_INIT_DELAY)

  def schedulePeriodicPublishJob() : Unit = {
    // NOTE: the FiniteDuration overload of scheduleWithFixedDelay
    //  does not work. Unsure why, but that's why these FiniteDurations are
    //  awkwardly parsed into seconds.
    scheduler.scheduleOnce(
      SCHED_INIT_DELAY.toSeconds,
      TimeUnit.SECONDS,
      () => queryAndSchedule())
  }

  /**
   * For each dataset, ask a Coordinator with a TsCardinalities LogicalPlan.
   * Schedules:
   *   (1) a job to publish the Coordinator's response, and
   *   (2) a job to execute the next query
   */
  private def queryAndSchedule() : Unit = {
    import filodb.query.exec.TsCardExec._
    val numGroupByFields = 2  // group cardinalities at the second level (i.e. ws & ns)
    val prefix = Nil  // query for cardinalities regardless of first-level name (i.e. ws name)
    dsIterProducer().foreach { dsRef =>
      val fut = Client.asyncAsk(
        coordActorProducer(),
        LogicalPlan2Query(dsRef, TsCardinalities(prefix, numGroupByFields)),
        queryLimiter.getDelay())
      fut.onComplete {
        case Success(qresp) =>
          queryLimiter.recordOnTime()
          qresp match {
            case QueryResult(_, _, rv, _, _, _) =>
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
            case QueryError(_, _, t) => logger.warn("QueryError: " + t.getMessage)
          }
        case Failure(t) =>
          logger.warn("Failure: " + t.getMessage)
          if (t.isInstanceOf[TimeoutException]) {
            queryLimiter.recordTimeout()
          } else {
            queryLimiter.recordOnTime()
          }
        // required to compile
        case _ => throw new IllegalArgumentException("should never reach here; attempted to match: " + fut)
      }
    }

    // schedule the next query
    scheduler.scheduleOnce(
      queryLimiter.getDelay().toSeconds,
      TimeUnit.SECONDS,
      () => queryAndSchedule())
  }
}

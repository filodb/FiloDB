package filodb.coordinator

import scala.concurrent.duration._

import filodb.core.DatasetRef
import filodb.core.metrics.FilodbMetrics

/**
 * A class to hold gauges and other metrics on shard health.
 * How many shards are active, recovering, or down?
 * The gauges continually collect more and more data.
 *
 * @param ref the DatasetRef that these shard health stats are for.  One set of stats per dataset.
 * @param shardMapFunc a function that should return the current ShardMapper for that dataset
 * @param reportingInterval the interval at which the shard health stats are gathered
 */
class ShardHealthStats(ref: DatasetRef,
                       reportingInterval: FiniteDuration = 5.seconds) {

  val numActive = FilodbMetrics.gauge(s"num-active-shards", Map("dataset" -> ref.toString))
  val numRecovering = FilodbMetrics.gauge(s"num-recovering-shards", Map("dataset" -> ref.toString))
  val numUnassigned = FilodbMetrics.gauge(s"num-unassigned-shards", Map("dataset" -> ref.toString))
  val numAssigned = FilodbMetrics.gauge(s"num-assigned-shards", Map("dataset" -> ref.toString))
  val numError = FilodbMetrics.gauge(s"num-error-shards", Map("dataset" -> ref.toString))
  val numStopped = FilodbMetrics.gauge(s"num-stopped-shards", Map("dataset" -> ref.toString))
  val numDown = FilodbMetrics.gauge(s"num-down-shards", Map("dataset" -> ref.toString))
  val numErrorReassignmentsDone = FilodbMetrics.counter(s"num-error-reassignments-done", Map("dataset" -> ref.toString))
  val numErrorReassignmentsSkipped = FilodbMetrics.counter(s"num-error-reassignments-skipped",
                                                           Map("dataset" -> ref.toString))

  def update(mapper: ShardMapper, skipUnassigned: Boolean = false): Unit = {
    numActive.update(mapper.statuses.count(_ == ShardStatusActive))
    numRecovering.update(mapper.statuses.count(_.isInstanceOf[ShardStatusRecovery]))
    numAssigned.update(mapper.statuses.count(_ == ShardStatusAssigned))
    if (!skipUnassigned) numUnassigned.update(mapper.statuses.count(_ == ShardStatusUnassigned))
    numError.update(mapper.statuses.count(_.isInstanceOf[ShardStatusError]))
    numStopped.update(mapper.statuses.count(_ == ShardStatusStopped))
    numDown.update(mapper.statuses.count(_ == ShardStatusDown))
  }

   /**
    * Stop collecting the metrics.  If this is not done then errors might get propagated and the code keeps running
    * forever and ever.
    */
   def reset(): Unit = {
     numActive.update(0)
     numRecovering.update(0)
     numUnassigned.update(0)
     numAssigned.update(0)
     numError.update(0)
     numStopped.update(0)
     numDown.update(0)
   }
}
package filodb.coordinator

import scala.concurrent.duration._

import kamon.Kamon

import filodb.core.DatasetRef

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
                       shardMapFunc: => ShardMapper,
                       reportingInterval: FiniteDuration = 5.seconds) {

  val numActive = Kamon.gauge(s"num-active-shards-$ref")
  val numRecovering = Kamon.gauge(s"num-recovering-shards-$ref")
  val numUnassigned = Kamon.gauge(s"num-unassigned-shards-$ref")
  val numAssigned = Kamon.gauge(s"num-assigned-shards-$ref")
  val numError = Kamon.gauge(s"num-error-shards-$ref")
  val numStopped = Kamon.gauge(s"num-stopped-shards-$ref")
  val numDown = Kamon.gauge(s"num-down-shards-$ref")
  val numErrorReassignmentsDone = Kamon.counter(s"num-error-reassignments-done-$ref")
  val numErrorReassignmentsSkipped = Kamon.counter(s"num-error-reassignments-skipped-$ref")

  def update(mapper: ShardMapper): Unit = {
    numActive.set(shardMapFunc.statuses.count(_ == ShardStatusActive))
    numRecovering.set(shardMapFunc.statuses.count(_.isInstanceOf[ShardStatusRecovery]))
    numUnassigned.set(shardMapFunc.statuses.count(_ == ShardStatusUnassigned))
    numAssigned.set(shardMapFunc.statuses.count(_ == ShardStatusAssigned))
    numError.set(shardMapFunc.statuses.count(_ == ShardStatusError))
    numStopped.set(shardMapFunc.statuses.count(_ == ShardStatusStopped))
    numDown.set(shardMapFunc.statuses.count(_ == ShardStatusDown))
  }

   /**
    * Stop collecting the metrics.  If this is not done then errors might get propagated and the code keeps running
    * forever and ever.
    */
   def reset(): Unit = {
     numActive.set(0)
     numRecovering.set(0)
     numUnassigned.set(0)
     numAssigned.set(0)
     numError.set(0)
     numStopped.set(0)
     numDown.set(0)
   }
}
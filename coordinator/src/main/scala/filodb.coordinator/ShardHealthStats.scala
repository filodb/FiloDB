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
                       reportingInterval: FiniteDuration = 5.seconds) {

  val numActive = Kamon.gauge(s"num-active-shards").withTag("dataset", ref.toString)
  val numRecovering = Kamon.gauge(s"num-recovering-shards").withTag("dataset", ref.toString)
  val numUnassigned = Kamon.gauge(s"num-unassigned-shards").withTag("dataset", ref.toString)
  val numAssigned = Kamon.gauge(s"num-assigned-shards").withTag("dataset", ref.toString)
  val numError = Kamon.gauge(s"num-error-shards").withTag("dataset", ref.toString)
  val numStopped = Kamon.gauge(s"num-stopped-shards").withTag("dataset", ref.toString)
  val numDown = Kamon.gauge(s"num-down-shards").withTag("dataset", ref.toString)
  val numErrorReassignmentsDone = Kamon.counter(s"num-error-reassignments-done")
                      .withTag("dataset", ref.toString)
  val numErrorReassignmentsSkipped = Kamon.counter(s"num-error-reassignments-skipped")
                      .withTag("dataset", ref.toString)

  def update(mapper: ShardMapper, skipUnassigned: Boolean = false): Unit = {
    numActive.update(mapper.statuses.count(_ == ShardStatusActive))
    numRecovering.update(mapper.statuses.count(_.isInstanceOf[ShardStatusRecovery]))
    numAssigned.update(mapper.statuses.count(_ == ShardStatusAssigned))
    if (!skipUnassigned) numUnassigned.update(mapper.statuses.count(_ == ShardStatusUnassigned))
    numError.update(mapper.statuses.count(_ == ShardStatusError))
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
package filodb.coordinator

import scala.concurrent.duration._

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
  import filodb.core.Perftools._

   val numActive = pollingGauge(s"num-active-shards-$ref",
     reportingInterval){ shardMapFunc.statuses.filter(_ == ShardStatusNormal).size }
   val numRecovering = pollingGauge(s"num-recovering-shards-$ref",
     reportingInterval){ shardMapFunc.statuses.filter(_.isInstanceOf[ShardStatusRecovery]).size }
   val numUnassigned = pollingGauge(s"num-unassigned-shards-$ref",
     reportingInterval){ shardMapFunc.statuses.filter(_ == ShardStatusUnassigned).size }
   val numAssigned = pollingGauge(s"num-assigned-shards-$ref",
     reportingInterval){ shardMapFunc.statuses.filter(_ == ShardStatusAssigned).size }
   val numError = pollingGauge(s"num-error-shards-$ref",
     reportingInterval){ shardMapFunc.statuses.filter(_ == ShardStatusError).size }
   val numStopped = pollingGauge(s"num-stopped-shards-$ref",
     reportingInterval){ shardMapFunc.statuses.filter(_ == ShardStatusStopped).size }
   val numDown = pollingGauge(s"num-down-shards-$ref",
     reportingInterval){ shardMapFunc.statuses.filter(_ == ShardStatusDown).size }

   /**
    * Stop collecting the metrics.  If this is not done then errors might get propagated and the code keeps running
    * forever and ever.
    */
   def shutdown(): Unit = {
     numActive.cleanup
     numRecovering.cleanup
     numUnassigned.cleanup
     numAssigned.cleanup
     numError.cleanup
     numStopped.cleanup
     numDown.cleanup
   }
}
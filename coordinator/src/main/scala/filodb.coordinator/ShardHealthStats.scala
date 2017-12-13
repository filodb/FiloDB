package filodb.coordinator

import scala.concurrent.duration._

import kamon.Kamon
import kamon.metric.instrument.Gauge

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
   val numActive = Kamon.metrics.gauge(s"num-active-shards-$ref",
     reportingInterval)(new Gauge.CurrentValueCollector {
      def currentValue: Long = shardMapFunc.statuses.filter(_ == ShardStatusNormal).size
     })
   val numRecovering = Kamon.metrics.gauge(s"num-recovering-shards-$ref",
     reportingInterval)(new Gauge.CurrentValueCollector {
      def currentValue: Long = shardMapFunc.statuses.filter(_.isInstanceOf[ShardStatusRecovery]).size
     })
   val numUnassigned = Kamon.metrics.gauge(s"num-unassigned-shards-$ref",
     reportingInterval)(new Gauge.CurrentValueCollector {
      def currentValue: Long = shardMapFunc.statuses.filter(_ == ShardStatusUnassigned).size
     })
   val numAssigned = Kamon.metrics.gauge(s"num-assigned-shards-$ref",
     reportingInterval)(new Gauge.CurrentValueCollector {
      def currentValue: Long = shardMapFunc.statuses.filter(_ == ShardStatusAssigned).size
     })
   val numError = Kamon.metrics.gauge(s"num-error-shards-$ref",
     reportingInterval)(new Gauge.CurrentValueCollector {
      def currentValue: Long = shardMapFunc.statuses.filter(_ == ShardStatusError).size
     })
   val numStopped = Kamon.metrics.gauge(s"num-stopped-shards-$ref",
     reportingInterval)(new Gauge.CurrentValueCollector {
      def currentValue: Long = shardMapFunc.statuses.filter(_ == ShardStatusStopped).size
     })
   val numDown = Kamon.metrics.gauge(s"num-down-shards-$ref",
     reportingInterval)(new Gauge.CurrentValueCollector {
      def currentValue: Long = shardMapFunc.statuses.filter(_ == ShardStatusDown).size
     })

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
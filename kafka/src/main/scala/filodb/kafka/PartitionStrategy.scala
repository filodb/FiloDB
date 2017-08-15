package filodb.kafka

import java.util.{Map => JMap}

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

import filodb.coordinator.ShardMapper

trait PartitionStrategy extends Partitioner {

  def configure(map: JMap[String, _]): Unit = {}

  def partition(topic: String,
                key: Any,
                keyBytes: Array[Byte],
                value: Any,
                valueBytes: Array[Byte],
                cluster: Cluster): Int

  override def close(): Unit = {}
}

abstract class ShardPartitionStrategy extends PartitionStrategy {
  var numShards = -1

  override def configure(map: JMap[String, _]): Unit = {
    numShards = map.get("number.of.shards").toString.toInt
    require(numShards > 0)
  }

  protected final val shardToNodeMappings = new ShardMapper(numShards)

  def partition(topic: String,
                key: Any, keyBytes: Array[Byte],
                value: Any, valueBytes: Array[Byte],
                cluster: Cluster): Int
}

/**
 * A really simple PartitionStrategy that obtains the desired partition or shard number from the key of the
 * message as a Long.
 */
class LongKeyPartitionStrategy extends PartitionStrategy {
  def partition(topic: String, key: Any, keyBytes: Array[Byte],
                value: Any, valueBytes: Array[Byte], cluster: Cluster): Int =
    key.asInstanceOf[java.lang.Long].toInt
}
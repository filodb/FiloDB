package filodb.kafka

import java.util
import java.util.concurrent.atomic.AtomicReference

import filodb.coordinator.ShardMapper
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

trait PartitionStrategy extends Partitioner {

  def configure(map: util.Map[String, _]): Unit

  def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int

  override def close(): Unit = {}
}

/** wip */
abstract class ShardPartitionStrategy extends PartitionStrategy {

  private val settings = new KafkaSettings()

  private val shardToNodeMappings = new AtomicReference[ShardMapper](new ShardMapper(settings.NumPartitions))

  override def configure(map: util.Map[String, _]): Unit = {}

  def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int
}

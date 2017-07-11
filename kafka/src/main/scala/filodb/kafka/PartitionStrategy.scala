package filodb.kafka

import java.util.{Map => JMap}

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

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

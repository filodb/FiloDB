package filodb.kafka

import java.util.{Map => JMap}

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class ClusterShardPartitioner extends Partitioner {
  import filodb.implicits._

  private val immutable: Map[String,AnyRef] = Map.empty

  override def configure(configs: JMap[String, _]): Unit = {
    configs.asImmutable
  }

  override def partition(topic: String,
                         key: java.lang.Object,
                         keyBytes: Array[Byte],
                         value: java.lang.Object,
                         valueBytes: Array[Byte],
                         cluster: Cluster): Int = {
    0
  }

  override def close(): Unit = {

  }
}

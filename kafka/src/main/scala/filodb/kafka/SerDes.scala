package filodb.kafka

import java.util.{Map => JMap}

/** Kafka-specific root serialization marker.
  * Example of ones for Protobuf:
  *
  * {{{
  *   class ProtoSerializer[A <: GeneratedMessageV3] extends Serializer[A] with KafkaSerializer {
  *      override def serialize(topic: String, data: A): Array[Byte] = data.toByteArray
  *   }
  *
  *   // Users must implement their version of this, configure it and supply it on the classpath
  *   abstract class ProtoDeserializer[A <: GeneratedMessageV3] extends Deserializer[A] with KafkaSerializer {
  *     override def deserialize(topic: String, data: Array[Byte]): A
  *   }
  *
  * }}}
  */
trait KafkaSerializer {
  def configure(configs: JMap[String, _], isKey: Boolean): Unit = {}
  def close(): Unit = {}
}

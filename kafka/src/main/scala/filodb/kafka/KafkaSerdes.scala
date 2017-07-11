package filodb.kafka

import java.util.{Map => JMap}

/** Kafka-specific helper and root serdes marker.
  * Example usage for a Protobuf:
  * {{{
  *   class ProtoSerializer[A <: GeneratedMessageV3] extends Serializer[A] with KafkaSerDes {
  *      override def serialize(topic: String, data: A): Array[Byte] = data.toByteArray
  *   }
  * }}}
  *
  * Users must implement their version of this, configure it and supply it on the classpath
  * {{{
  *   abstract class ProtoDeserializer[A <: GeneratedMessageV3] extends Deserializer[A] with KafkaSerDes {
  *     override def deserialize(topic: String, data: Array[Byte]): A
  *   }
  * }}}
  */
trait KafkaSerdes {
  def configure(configs: JMap[String, _], isKey: Boolean): Unit = {}
  def close(): Unit = {}
}

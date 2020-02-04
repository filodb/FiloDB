package filodb.kafka

import kamon.Kamon
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

import filodb.core.binaryrecord2.RecordContainer

object RecordContainerSerdeStats {
  val tsBatchSize = Kamon.histogram("kafka-container-size-bytes")
  val tsCount     = Kamon.counter("kafka-num-containers")
}

final class RecordContainerSerializer extends Serializer[RecordContainer] with KafkaSerdes {
  override def serialize(topic: String, data: RecordContainer): Array[Byte] =
    if (data.hasArray) data.array else data.trimmedArray
}

final class RecordContainerDeserializer extends Deserializer[RecordContainer] with KafkaSerdes {
  import RecordContainerSerdeStats._

  override def deserialize(topic: String, data: Array[Byte]): RecordContainer = {
    tsBatchSize.withoutTags.record(data.size)
    tsCount.withoutTags.increment
    RecordContainer(data)
  }
}

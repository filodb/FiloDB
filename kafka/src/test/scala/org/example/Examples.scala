package org.example

import org.apache.kafka.common.serialization._

import filodb.core.memstore.IngestRecord
import filodb.core.metadata.Dataset
import filodb.kafka.{KafkaSerdes, RecordConverter}
import filodb.memory.format._

/**
 * We want to convert an event with an instance which is the partition key and values
 */
final case class Event(instance: String, values: Seq[Any])

final class CustomSerializer extends Serializer[String] with KafkaSerdes {
  override def serialize(topic: String, data: String): Array[Byte] = (data + "-" + System.nanoTime).getBytes
}

final class CustomDeserializer extends Deserializer[String] with KafkaSerdes {
  override def deserialize(topic: String, data: Array[Byte]): String = new String(data)
}

class CustomRecordConverter(dataset: Dataset) extends RecordConverter {
  override def convert(event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord] = {
    event match {
      case e: Event => Seq(IngestRecord(RecordConverter.partReader(dataset.partitionColumns.head, e.instance),
                                        SeqRowReader(e.values),
                                        offset))
      case _        => Seq.empty[IngestRecord]
    }
  }
}

final class EventSerializer extends Serializer[Event] with KafkaSerdes {
  override def serialize(topic: String, data: Event): Array[Byte] = new String().getBytes
}

final class EventDeserializer extends Deserializer[Event] with KafkaSerdes {
  override def deserialize(topic: String, data: Array[Byte]): Event =
    Event("instance0", Seq(new String(data)))
}

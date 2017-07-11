package org.example

import org.apache.kafka.common.serialization._

import filodb.core.metadata.RichProjection
import filodb.kafka.{KafkaSerdes, RecordConverter}
import org.velvia.filo._

final case class Event(values: Seq[Any])

final class CustomSerializer extends Serializer[String] with KafkaSerdes {
  override def serialize(topic: String, data: String): Array[Byte] = (data + "-" + System.nanoTime).getBytes
}

final class CustomDeserializer extends Deserializer[String] with KafkaSerdes {
  override def deserialize(topic: String, data: Array[Byte]): String = new String(data)
}

class CustomRecordConverter extends RecordConverter {

  override def convert(proj: RichProjection, event: AnyRef, offset: Long): Seq[RowReader] = {
    event match {
      case e: Event => Seq(SeqRowReader(e.values))
      case _ => Seq.empty[RowReader]
    }
  }
}

final class EventSerializer extends Serializer[Event] with KafkaSerdes {
  override def serialize(topic: String, data: Event): Array[Byte] = new String().getBytes
}

final class EventDeserializer extends Deserializer[Event] with KafkaSerdes {
  override def deserialize(topic: String, data: Array[Byte]): Event =
    Event(Seq(new String(data)))
}

class SimpleRecordConverter extends RecordConverter {

  override def convert(proj: RichProjection, event: AnyRef, offset: Long): Seq[RowReader] = {
    event match {
      case e: String => Seq(SeqRowReader(e))
      case _ => Seq.empty[RowReader]
    }
  }
}


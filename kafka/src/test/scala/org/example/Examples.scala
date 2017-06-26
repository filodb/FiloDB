package org.example

import org.apache.kafka.common.serialization._

import filodb.core.memstore.IngestRecord
import filodb.core.metadata.{Column, RichProjection}
import filodb.kafka.{KafkaSerDes, RecordConverter, UtcTime}
import org.velvia.filo.{SchemaSeqRowReader, SeqRowReader}

final case class Event(values: Seq[Any])

final class CustomSerializer extends Serializer[String] with KafkaSerDes {
  override def serialize(topic: String, data: String): Array[Byte] = (data + "-" + UtcTime.now).getBytes
}

final class CustomDeserializer extends Deserializer[String] with KafkaSerDes {
  override def deserialize(topic: String, data: Array[Byte]): String = new String(data)
}

class CustomSerde() extends Serde[String] with KafkaSerDes {
  override def serializer(): Serializer[String] = new CustomSerializer
  override def deserializer(): Deserializer[String] = new CustomDeserializer
}

class CustomRecordConverter extends RecordConverter {
  import Column.ColumnType._

  override def convert(proj: RichProjection, event: AnyRef, offset: Long): Seq[IngestRecord] = {
    event match {
      case e: Event =>
        Seq(IngestRecord(
          SchemaSeqRowReader(e.values, Array(StringColumn.keyType.extractor)),
          SeqRowReader(e.values), offset))
      case other => // can handle any other types
        Seq.empty[IngestRecord]
    }
  }
}

class SimpleRecordConverter extends RecordConverter {
  import Column.ColumnType._

  override def convert(proj: RichProjection, event: AnyRef, offset: Long): Seq[IngestRecord] = {
    event match {
      case e: String =>
        Seq(IngestRecord(
          SchemaSeqRowReader(Seq(e), Array(StringColumn.keyType.extractor)),
          SeqRowReader(Seq(e)), offset))
      case other => // can handle any other types
        Seq.empty[IngestRecord]
    }
  }
}


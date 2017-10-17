package filodb.kafka

import scala.util.control.NonFatal

import filodb.coordinator.ConfigurableInstance
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.{Column, DataColumn, Dataset}
import org.velvia.filo.{SingleValueRowReader, RowReader, SchemaRoutingRowReader}

/**
 * Extend to create custom converters for event types.
 * The class should take a single constructor argument, which is the Dataset.
 * This allows the RecordConverter to use things like IngestRouting
 */
trait RecordConverter {
  /** Converts each inbound event received from the Kafka stream
    * to a FiloDB `IngestRecord`.
    *
    * @param event     the value of the Kafka ConsumerRecord
    * @param partition the Kafka topic partition the event was received by
    * @param offset    the Kafka topic partition's offset of the message
    */
  def convert(event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord]
}

object RecordConverter extends ConfigurableInstance {
  // Creates a SchemaRoutingRowReader suitable for IngestRecord from a single value and column definition
  def partReader(column: Column, value: Any): SchemaRoutingRowReader = {
    val extractors = Array[RowReader.TypedFieldExtractor[_]](column.columnType.keyType.extractor)
    SchemaRoutingRowReader(SingleValueRowReader(value), Array(0), extractors)
  }

  def apply(fqcn: String, dataset: Dataset): RecordConverter =
    createClass(fqcn)
      .flatMap { c: Class[_] => createInstance[RecordConverter](c, Seq((classOf[Dataset], dataset))) }
      .recover { case NonFatal(e) =>
        logger.error(s"Unable to instantiate IngestionConverter from $fqcn", e)
        throw e
      }.get
}

/** A simple converter for String types.   Produces a dummy partition key. */
final class StringRecordConverter(dataset: Dataset) extends RecordConverter {
  val fakePartCol = DataColumn(0, "dummy", Column.ColumnType.StringColumn)
  val partReader = RecordConverter.partReader(fakePartCol, "/0")

  override def convert(event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord] = {
    event match {
      case e: String => Seq(IngestRecord(partReader, SingleValueRowReader(e), offset))
      case _         => Seq.empty[IngestRecord]
    }
  }
}

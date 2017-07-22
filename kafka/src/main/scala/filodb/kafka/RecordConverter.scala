package filodb.kafka

import scala.util.control.NonFatal

import filodb.coordinator.ConfigurableInstance
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.RichProjection
import org.velvia.filo.SingleValueRowReader

/** Extend to create custom converters for even types. */
trait RecordConverter {

  /** Converts each inbound event received from the Kafka stream
    * to a FiloDB `IngestRecord`.
    *
    * @param proj      the user's data scheme projection
    * @param event     the value of the Kafka ConsumerRecord
    * @param partition the Kafka topic partition the event was received by
    * @param offset    the Kafka topic partition's offset of the message
    */
  def convert(proj: RichProjection, event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord]
}

object RecordConverter extends ConfigurableInstance {

  def apply(fqcn: String): RecordConverter =
    createClass(fqcn)
      .flatMap { c: Class[_] => createInstance[RecordConverter](c) }
      .recover { case NonFatal(e) =>
        logger.error(s"Unable to instantiate IngestionConverter from $fqcn", e)
        throw e
      }.get
}

/** A simple converter for String types. */
final class StringRecordConverter extends RecordConverter {

  override def convert(proj: RichProjection, event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord] = {
    event match {
      case e: String => Seq(IngestRecord(proj, SingleValueRowReader(e), offset))
      case _         => Seq.empty[IngestRecord]
    }
  }
}

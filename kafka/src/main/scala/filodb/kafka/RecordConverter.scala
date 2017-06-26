package filodb.kafka

import scala.util.control.NonFatal

import filodb.core.memstore.IngestRecord
import filodb.core.metadata.RichProjection
import filodb.coordinator.ConfigurableInstance

/** Extend to create custom converters for even types. */
trait RecordConverter {

  def convert(proj: RichProjection, event: AnyRef, offset: Long): Seq[IngestRecord]
}

object RecordConverter extends ConfigurableInstance {

  def apply(fqcn: String): RecordConverter =
    createClass(fqcn)
      .flatMap(c => createInstance[RecordConverter](c))
      .recover { case NonFatal(e) =>
        logger.error(s"Unable to instantiate IngestionConverter from $fqcn", e)
        throw e
      }.get
}
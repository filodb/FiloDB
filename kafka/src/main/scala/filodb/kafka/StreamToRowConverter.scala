package filodb.kafka

import filodb.core.memstore.IngestRecord

/** User implements, provides on classpath and configures.
  * Where `E` is the Kafka ConsumerRecord value, the message payload.
  *
  * {{{
  *   override def convert[E](event: E, offset: Long): Seq[IngestRecord] =
  *     event match {
  *       case e: CustomEvent =>
  *       val keyValueReader = CustomKVeader(e)
  *       e.yourFields.asScala.map { recordValue =>
  *        IngestRecord(keyValueReader, CustomRowReader(recordValue), offset)
  *      }
  *    case other =>
  *      Seq.empty[IngestRecord]
  *   }
  * }}}
  */
trait StreamToRowConverter {

  def convert[E](event: E, offset: Long): Seq[IngestRecord]

}
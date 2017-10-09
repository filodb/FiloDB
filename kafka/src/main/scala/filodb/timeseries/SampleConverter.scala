package filodb.timeseries

import com.typesafe.scalalogging.StrictLogging
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.Column.ColumnType
import filodb.core.metadata.RichProjection
import filodb.kafka.RecordConverter
import org.velvia.filo.{RowReader, SchemaRowReader, ZeroCopyUTF8String}

/**
  * Converts a time series sample in the following simple human-readable string format.
  * First field is a comma delimited sequence of tags as a key=value pair. Next the timestamp
  * millisecond followed by its double value.
  *
  * `key1=value1,key2=value2   someTimestampMs     someDoubleValue`
  *
  * Meant for local development testing only. Not optimized for performance.
  *
  */
class TimeseriesSampleConverter extends RecordConverter with StrictLogging {

  override def convert(proj: RichProjection, event: AnyRef, partition: Int, offset: Long): Seq[IngestRecord] = {
    event match {
      case e: String =>
        val fields =  e.split("\\s+")
        val pkReader = TimeseriesTagsRowReader(fields(0)) // fields.head is the tag values
        val dataReader = SampleRowReader(fields(1).toLong, fields(2).toDouble)
        Seq(IngestRecord(pkReader, dataReader, offset))

      case other =>
        Seq.empty[IngestRecord]
    }

  }

}

/**
  * This helps with extraction of fields into a map column type from
  * a string of the format `key1=value1,key2=value2`
  *
  */
final case class TimeseriesTagsRowReader(keyValues: String) extends SchemaRowReader {

  val extractors = Array[RowReader.TypedFieldExtractor[_]](ColumnType.MapColumn.keyType.extractor)

  val kvMap = keyValues.split(",").map{ kvs =>
    val kvList = kvs.split("=")
    ZeroCopyUTF8String(kvList(0)) -> ZeroCopyUTF8String(kvList(1))
  }.toMap

  final def getDouble(index: Int): Double = ???
  final def getLong(index: Int): Long = ???
  final def getString(index: Int): String = ???
  final def getAny(index: Int): Any = kvMap
  final def getBoolean(columnNo: Int): Boolean = ???
  final def getFloat(columnNo: Int): Float = ???
  final def getInt(columnNo: Int): Int = ???
  final def notNull(columnNo: Int): Boolean = true
}

final case class SampleRowReader(timestampMs: Long, value: Double) extends RowReader {
  final def getDouble(index: Int): Double = value
  final def getLong(index: Int): Long = timestampMs

  final def getString(index: Int): String = ???
  final def getAny(index: Int): Any = ???
  final def getBoolean(columnNo: Int): Boolean = ???
  final def getFloat(columnNo: Int): Float = ???
  final def getInt(columnNo: Int): Int = ???
  final def notNull(columnNo: Int): Boolean = true
}
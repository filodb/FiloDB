package filodb.core.query

import org.joda.time.DateTime

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.store.{ChunkScanMethod, RowKeyChunkScan}
import filodb.memory.format.{RowReader, ZeroCopyUTF8String}

/**
  * Some basic info about a single Partition
  */
final case class RangeVectorKey(labelValues: Seq[LabelValue],
                                sourceShards: Seq[Int]) {
  override def toString: String = s"/shard:${sourceShards.mkString(",")}/$labelValues"
}

case object RangeVectorKey {
  def apply(partitionKey: BinaryRecord, sourceShards: Seq[Int]): RangeVectorKey = ???
}

case class LabelValue(label: ZeroCopyUTF8String, value: ZeroCopyUTF8String, shard: Int) {
  override def toString: String = s"$label=$value"
}

/**
  * Represents a single result of any FiloDB Query.
  */
trait RangeVector extends java.io.Serializable {
  def key: RangeVectorKey
  def rows: Iterator[RowReader]

  /**
    * Pretty prints all the elements into strings.
    */
  def prettyPrint(schema: ResultSchema, formatTime: Boolean = true): String = {
    val curTime = System.currentTimeMillis
    key.toString + "\n\t" +
      rows.map {
        case br: BinaryRecord if br.isEmpty =>  "\t<empty>"
        case reader =>
          val firstCol = if (formatTime && schema.isTimeSeries) {
            val timeStamp = reader.getLong(0)
            s"${new DateTime(timeStamp).toString()} (${(curTime - timeStamp)/1000}s ago)"
          } else {
            reader.getAny(0).toString
          }
          (firstCol +: (1 until schema.length).map(reader.getAny(_).toString)).mkString("\t")
      }.mkString("\n\t") + "\n"
  }
}

final case class ChunkSetBackedRangeVector(key: RangeVectorKey,
                                           chunkMethod: ChunkScanMethod,
                                           ordering: Ordering[RowReader],
                                           readers: Iterator[ChunkSetReader]) extends RangeVector {

  private def rangedIterator(startKey: BinaryRecord, endKey: BinaryRecord): Iterator[RowReader] = {
    readers.flatMap { reader =>
      val (startRow, endRow) = reader.rowKeyRange(startKey, endKey, ordering)
      if (endRow < 0 || startRow >= reader.length) { Iterator.empty }
      else if (startRow == 0 && endRow == (reader.length - 1)) {
        reader.rowIterator()
      } else {
        reader.rowIterator().take(endRow + 1).drop(startRow)
      }
    }
  }

  /**
    * Returns an Iterator of RowReader over all the rows in each chunkset in order
    */
  private def allRowsIterator: Iterator[RowReader] = readers.flatMap(_.rowIterator())

  def rows: Iterator[RowReader] = {
    chunkMethod match {
      case range: RowKeyChunkScan => rangedIterator(range.startkey, range.endkey)
      case _ => allRowsIterator
    }
  }
}

final case class IteratorBackedRangeVector(key: RangeVectorKey, rows: Iterator[RowReader]) extends RangeVector
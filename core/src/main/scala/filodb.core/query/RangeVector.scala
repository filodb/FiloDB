package filodb.core.query

import org.joda.time.DateTime

import filodb.core.Types
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType._
import filodb.core.store.{ChunkScanMethod, RowKeyChunkScan}
import filodb.memory.MemFactory
import filodb.memory.format.{FastFiloRowReader, FiloVector, RowReader, ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.{vectors => bv, _}

/**
  * Identifier for a single RangeVector
  */
trait RangeVectorKey extends java.io.Serializable {
  def labelValues: Seq[LabelValue]
  def sourceShards: Seq[Int]
  override def toString: String = s"/shard:${sourceShards.mkString(",")}/$labelValues"
}

/**
  * Range Vector Key backed by a PartitionKey object.
  */
final class PartitionRangeVectorKey(partKey: BinaryRecord,
                                    partKeyCols: Seq[ColumnInfo],
                                    sourceShard: Int) extends RangeVectorKey {
  override def sourceShards: Seq[Int] = Seq(sourceShard)
  def labelValues: Seq[LabelValue] = {
    partKeyCols.zipWithIndex.flatMap { case (c, pos) =>
      c.colType match {
        case StringColumn => Seq(LabelValue(UTF8Str(c.name), partKey.filoUTF8String(pos)))
        case IntColumn    => Seq(LabelValue(UTF8Str(c.name), UTF8Str(partKey.getInt(pos).toString)))
        case LongColumn   => Seq(LabelValue(UTF8Str(c.name), UTF8Str(partKey.getLong(pos).toString)))
        case MapColumn    => partKey.as[Types.UTF8Map](pos).map { case (k, v) => LabelValue(k, v) }
        case _            => throw new UnsupportedOperationException("Not supported yet")
      }
    }
  }
}

case class LabelValue(label: UTF8Str, value: UTF8Str) {
  override def toString: String = s"$label=$value"
}

/**
  * Represents a single result of any FiloDB Query.
  */
trait RangeVector {
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
            s"${new DateTime(timeStamp).toString()} (${(curTime - timeStamp)/1000}s ago) $timeStamp"
          } else {
            reader.getAny(0).toString
          }
          (firstCol +: (1 until schema.length).map(reader.getAny(_).toString)).mkString("\t")
      }.mkString("\n\t") + "\n"
  }
}

final case class RawDataRangeVector(key: RangeVectorKey,
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

  def rows: Iterator[RowReader] = {
    chunkMethod match {
      case range: RowKeyChunkScan => rangedIterator(range.startkey, range.endkey)
      case _                      => readers.flatMap(_.rowIterator())
    }
  }
}

final class SerializableRangeVector(val key: RangeVectorKey,
                                    val parsers: Array[BinaryVector[_]],
                                    val numRows: Int) extends RangeVector with java.io.Serializable {
  override def rows: Iterator[RowReader] = {
    val reader = new FastFiloRowReader(parsers.map(_.asInstanceOf[FiloVector[_]]))
    new Iterator[RowReader] {
      private var i = 0
      final def hasNext: Boolean = i < numRows
      final def next: RowReader = {
        reader.setRowNo(i)
        i += 1
        reader
      }
    }
  }
}

object SerializableRangeVector {
  def apply(rv: RangeVector, cols: Seq[ColumnInfo]): SerializableRangeVector = {
    val memFactory = MemFactory.onHeapFactory
    val maxElements = 1000 // FIXME for some reason this isn't working if small
    val vectors: Array[BinaryAppendableVector[_]] = cols.toArray.map { col =>
      col.colType match {
        case IntColumn => bv.IntBinaryVector.appendingVector(memFactory, maxElements)
        case LongColumn => bv.LongBinaryVector.appendingVector(memFactory, maxElements)
        case DoubleColumn => bv.DoubleVector.appendingVector(memFactory, maxElements)
        case TimestampColumn => bv.LongBinaryVector.appendingVector(memFactory, maxElements)
        case StringColumn => bv.UTF8Vector.appendingVector(memFactory, maxElements)
        case _: Column.ColumnType => ???
      }
    }
    val rows = rv.rows
    var numRows = 0
    rows.foreach { row =>
      numRows += 1
      for { i <- 0 until vectors.size } {
        vectors(i).addFromReader(row, i)
      }
    }
    // TODO need to measure if optimize really helps or has a negative effect
    new SerializableRangeVector(rv.key, vectors.map(_.asInstanceOf[BinaryVector[_]]), numRows)
  }
}

final case class IteratorBackedRangeVector(key: RangeVectorKey,
                                           rows: Iterator[RowReader]) extends RangeVector
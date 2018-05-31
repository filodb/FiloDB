package filodb.core.query

import org.joda.time.DateTime

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.binaryrecord2.{MapItemConsumer, RecordSchema}
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType._
import filodb.core.store.{ChunkScanMethod, RowKeyChunkScan}
import filodb.memory.{MemFactory, UTF8StringMedium}
import filodb.memory.format.{FastFiloRowReader, FiloVector, RowReader, ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.{vectors => bv, _}

/**
  * Identifier for a single RangeVector
  */
trait RangeVectorKey extends java.io.Serializable {
  def labelValues: Map[UTF8Str, UTF8Str]
  def sourceShards: Seq[Int]
  override def toString: String = s"/shard:${sourceShards.mkString(",")}/$labelValues"
}

class SeqMapConsumer extends MapItemConsumer {
  val pairs = new collection.mutable.ArrayBuffer[(UTF8Str, UTF8Str)]
  def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
    val keyUtf8 = new UTF8Str(keyBase, keyOffset + 2, UTF8StringMedium.numBytes(keyBase, keyOffset))
    val valUtf8 = new UTF8Str(valueBase, valueOffset + 2, UTF8StringMedium.numBytes(valueBase, valueOffset))
    pairs += (keyUtf8 -> valUtf8)
  }
}

/**
  * Range Vector Key backed by a BinaryRecord v2 partition key, whic is basically a pointer to memory on or offheap.
  */
final case class PartitionRangeVectorKey(partBase: Array[Byte],
                                         partOffset: Long,
                                         partSchema: RecordSchema,
                                         partKeyCols: Seq[ColumnInfo],
                                         sourceShard: Int) extends RangeVectorKey {
  override def sourceShards: Seq[Int] = Seq(sourceShard)
  def labelValues: Map[UTF8Str, UTF8Str] = {
    partKeyCols.zipWithIndex.flatMap { case (c, pos) =>
      c.colType match {
        case StringColumn => Seq(UTF8Str(c.name) -> partSchema.asZCUTF8Str(partBase, partOffset, pos))
        case IntColumn    => Seq(UTF8Str(c.name) -> UTF8Str(partSchema.getInt(partBase, partOffset, pos).toString))
        case LongColumn   => Seq(UTF8Str(c.name) -> UTF8Str(partSchema.getLong(partBase, partOffset, pos).toString))
        case MapColumn    => val consumer = new SeqMapConsumer
                             partSchema.consumeMapItems(partBase, partOffset, pos, consumer)
                             consumer.pairs
        case _            => throw new UnsupportedOperationException("Not supported yet")
      }
    }.toMap
  }
  override def toString: String = s"/shard:$sourceShard/${partSchema.stringify(partBase, partOffset)}"
}

final case class CustomRangeVectorKey(labelValues: Map[UTF8Str, UTF8Str]) extends RangeVectorKey {
  val sourceShards: Seq[Int] = Nil
}

object CustomRangeVectorKey {
  def fromZcUtf8(str: UTF8Str): CustomRangeVectorKey = {
    CustomRangeVectorKey(str.asNewString.split("\u03BC").map(_.split("\u03C0")).filter(_.length == 2).map { lv =>
      ZeroCopyUTF8String(lv(0)) -> ZeroCopyUTF8String(lv(1))
    }.toMap)
  }

  def toZcUtf8(rvk: RangeVectorKey): UTF8Str = {
    // TODO can we optimize this further? Can we use a binary field in the row-reader ?
    val str = rvk.labelValues.toSeq.map(lv=>s"${lv._1.asNewString}\u03C0${lv._2.asNewString}").sorted.mkString("\u03BC")
    UTF8Str(str)
  }

  val emptyAsZcUtf8 = toZcUtf8(CustomRangeVectorKey(Map.empty))
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

  val rows: Iterator[RowReader] = {
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
  def apply(rv: RangeVector, cols: Seq[ColumnInfo], limit: Int): SerializableRangeVector = {
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
    rows.take(limit).foreach { row =>
      numRows += 1
      for { i <- vectors.indices } {
        vectors(i).addFromReader(row, i)
      }
    }
    // TODO need to measure if optimize really helps or has a negative effect
    new SerializableRangeVector(rv.key, vectors.map(_.optimize(memFactory)), numRows)
  }
}

final case class IteratorBackedRangeVector(key: RangeVectorKey,
                                           rows: Iterator[RowReader]) extends RangeVector
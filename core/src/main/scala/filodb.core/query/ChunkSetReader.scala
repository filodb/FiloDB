package filodb.core.query

import com.googlecode.javaewah.EWAHCompressedBitmap
import java.nio.ByteBuffer
import org.velvia.filo._
import scala.collection.mutable.BitSet

import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.core.store.{ChunkSet, ChunkSetInfo}
import filodb.core.Types.{ColumnId, PartitionKey}

/**
 * ChunkSetReader aggregates incoming chunks during query/read time and provides an Iterator[RowReader]
 * for iterating through the chunkset row-wise, or individual chunks can also be accessed.
 * It creates the FiloVectors once, when chunks are added, so that repeated gets of the Iterator avoid
 * the parsing and allocation overhead.
 */
class MutableChunkSetReader(info: ChunkSetInfo,
                            partition: PartitionKey,
                            skips: EWAHCompressedBitmap,
                            makers: Array[ChunkSetReader.VectorFactory])
extends ChunkSetReader(info, partition, skips, new Array[FiloVector[_]](makers.size)) {
  private final val bitset = new BitSet

  final def addChunk(colNo: Int, bytes: ByteBuffer): Unit = {
    vectors(colNo) = makers(colNo)(bytes, length)
    bitset += colNo
  }

  final def isFull: Boolean = bitset.size >= makers.size
}

/**
 * ChunkSetReader provides a way to iterate through FiloVectors as rows, including skipping of rows.
 */
class ChunkSetReader(val info: ChunkSetInfo,
                     val partition: PartitionKey,
                     skips: EWAHCompressedBitmap,
                     parsers: Array[FiloVector[_]]) {
  import ChunkSetReader._
  private final val len = info.numRows
  skips.setSizeInBits(len, false)

  def vectors: Array[FiloVector[_]] = parsers
  def length: Int = len

  /**
   * Iterates over rows in this chunkset, skipping over any rows defined in skiplist.
   * Creates new RowReaders every time, so that multiple calls could be made.
   */
  final def rowIterator(readerFactory: RowReaderFactory = DefaultReaderFactory): Iterator[RowReader] = {
    val reader = readerFactory(parsers)
    if (skips.isEmpty) {
      // This simplified iterator is MUCH faster than the skip-checking one
      new Iterator[RowReader] {
        private var i = 0
        final def hasNext: Boolean = i < len
        final def next: RowReader = {
          reader.setRowNo(i)
          i += 1
          reader
        }
      }
    } else {
      new Iterator[RowReader] {
        private final val rowNumIterator = skips.clearIntIterator()
        final def hasNext: Boolean = rowNumIterator.hasNext
        final def next: RowReader = {
          reader.setRowNo(rowNumIterator.next)
          reader
        }
      }
    }
  }
}

object ChunkSetReader {
  import ChunkSetInfo.emptySkips

  type VectorFactory = (ByteBuffer, Int) => FiloVector[_]
  type RowReaderFactory = Array[FiloVector[_]] => FiloRowReader

  val DefaultReaderFactory: RowReaderFactory = (vectors) => new FastFiloRowReader(vectors)

  type ColumnToMaker = Column => VectorFactory
  val defaultColumnToMaker: ColumnToMaker =
    (col: Column) => FiloVector.defaultVectorMaker(col.columnType.clazz)

  def apply(chunkSet: ChunkSet,
            partition: PartitionKey,
            schema: Seq[Column],
            skips: EWAHCompressedBitmap = emptySkips): ChunkSetReader = {
    val nameToPos = schema.zipWithIndex.map { case (c, i) => (c.name -> i) }.toMap
    val makers = schema.map(defaultColumnToMaker).toArray
    val reader = new MutableChunkSetReader(chunkSet.info, partition, skips, makers)
    chunkSet.chunks.foreach { case (colName, bytes) => reader.addChunk(nameToPos(colName), bytes) }
    reader
  }
}
package filodb.core.query

import com.googlecode.javaewah.EWAHCompressedBitmap
import java.nio.ByteBuffer
import org.velvia.filo._
import scala.collection.mutable.BitSet

import filodb.core.binaryrecord.{BinaryRecord, RecordSchema}
import filodb.core.metadata.Column
import filodb.core.metadata.Column.ColumnType
import filodb.core.store.{ChunkSet, ChunkSetInfo}
import filodb.core.Types.ColumnId

/**
 * ChunkSetReader aggregates incoming chunks during query/read time and provides an Iterator[RowReader]
 * for iterating through the chunkset row-wise, or individual chunks can also be accessed.
 * It creates the FiloVectors once, when chunks are added, so that repeated gets of the Iterator avoid
 * the parsing and allocation overhead.
 *
 * TODO: create readers for the chunks as they come in, instead of when rowIterator is called
 */
class ChunkSetReader(val info: ChunkSetInfo,
                     skips: EWAHCompressedBitmap,
                     makers: Array[ChunkSetReader.VectorFactory]) {
  import ChunkSetReader._

  private final val len = info.numRows
  private final val bitset = new BitSet
  private final val parsers = new Array[FiloVector[_]](makers.size)

  skips.setSizeInBits(len, false)

  final def addChunk(colNo: Int, bytes: ByteBuffer): Unit = {
    parsers(colNo) = makers(colNo)(bytes, len)
    bitset += colNo
  }

  def vectors: Array[FiloVector[_]] = parsers

  final def isFull: Boolean = bitset.size >= parsers.size

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
  import PartitionChunkIndex.emptySkips

  type VectorFactory = (ByteBuffer, Int) => FiloVector[_]
  type RowReaderFactory = Array[FiloVector[_]] => FiloRowReader

  val DefaultReaderFactory: RowReaderFactory = (vectors) => new FastFiloRowReader(vectors)

  type ColumnToMaker = Column => VectorFactory
  val defaultColumnToMaker: ColumnToMaker =
    (col: Column) => FiloVector.defaultVectorMaker(col.columnType.clazz)

  def apply(chunkSet: ChunkSet,
            schema: Seq[Column],
            skips: EWAHCompressedBitmap = emptySkips): ChunkSetReader = {
    val nameToPos = schema.zipWithIndex.map { case (c, i) => (c.name -> i) }.toMap
    val makers = schema.map(defaultColumnToMaker).toArray
    val reader = new ChunkSetReader(chunkSet.info, skips, makers)
    chunkSet.chunks.foreach { case (colName, bytes) => reader.addChunk(nameToPos(colName), bytes) }
    reader
  }
}
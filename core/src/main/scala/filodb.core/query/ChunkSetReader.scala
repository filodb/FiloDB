package filodb.core.query

import com.googlecode.javaewah.EWAHCompressedBitmap
import java.nio.ByteBuffer
import filodb.memory.format._
import scala.collection.mutable.BitSet

import filodb.core.metadata.Dataset
import filodb.core.store.{ChunkSet, ChunkSetInfo}
import filodb.core.Types.PartitionKey

/**
 * ChunkSetReader aggregates incoming chunks during query/read time and provides an Iterator[RowReader]
 * for iterating through the chunkset row-wise, or individual chunks can also be accessed.
 * It creates the FiloVectors once, when chunks are added, so that repeated gets of the Iterator avoid
 * the parsing and allocation overhead.
 */
class MutableChunkSetReader(info: ChunkSetInfo,
                            partition: PartitionKey,
                            skips: EWAHCompressedBitmap,
                            numChunks: Int)
extends ChunkSetReader(info, partition, skips, new Array[FiloVector[_]](numChunks)) {
  private final val bitset = new BitSet

  final def addChunk(colNo: Int, vector: FiloVector[_]): Unit = {
    vectors(colNo) = vector
    bitset += colNo
  }

  final def isFull: Boolean = bitset.size >= numChunks
}

/**
 * ChunkSetReader provides a way to iterate through FiloVectors as rows, including skipping of rows.
 * The partition is used by some aggregation functions to pull out partition information.
 */
class ChunkSetReader(val info: ChunkSetInfo,
                     val partition: PartitionKey,
                     val skips: EWAHCompressedBitmap,
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

  /**
   * Selects columns columnIDs from chunkSet, producing a ChunkSetReader
   */
  def apply(chunkSet: ChunkSet,
            dataset: Dataset,
            columnIDs: Seq[Int],
            skips: EWAHCompressedBitmap = emptySkips): ChunkSetReader = {
    val reader = new MutableChunkSetReader(chunkSet.info, chunkSet.partition, skips, chunkSet.chunks.length)
    chunkSet.chunks.zipWithIndex.foreach { case ((colID, buffer), pos) =>
      reader.addChunk(pos, dataset.vectorMakers(colID)(buffer, chunkSet.info.numRows))
    }
    reader
  }
}
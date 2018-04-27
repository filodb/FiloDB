package filodb.core.query

import java.nio.ByteBuffer

import scala.collection.mutable.BitSet

import com.googlecode.javaewah.EWAHCompressedBitmap

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.store.{timeUUID64, ChunkSet, ChunkSetInfo}
import filodb.core.Types.ChunkID
import filodb.memory.format._

/**
 * ChunkSetReader aggregates incoming chunks during query/read time and provides an Iterator[RowReader]
 * for iterating through the chunkset row-wise, or individual chunks can also be accessed.
 * It creates the FiloVectors once, when chunks are added, so that repeated gets of the Iterator avoid
 * the parsing and allocation overhead.
 */
class MutableChunkSetReader(info: ChunkSetInfo,
                            skips: EWAHCompressedBitmap,
                            numChunks: Int)
extends ChunkSetReader(info, skips, new Array[FiloVector[_]](numChunks)) {
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
                     val skips: EWAHCompressedBitmap,
                     parsers: Array[FiloVector[_]]) {
  import ChunkSetReader._
  private final val len = info.numRows
  skips.setSizeInBits(len, false)

  def vectors: Array[FiloVector[_]] = parsers
  def length: Int = len

  /**
   * Assuming the records are sorted in order of increasing rowKey, finds the range of row numbers
   * within this ChunkSetReader that contains the records for the range of row keys.
   * If the inputs are invalid (like key1 > key2) then (-1, -1) is returned.
   *
   * NOTE: the first chunks in a chunkset must correspond to row key columns.  For example, if rowkeys
   * is ["timestamp"], then the first chunk must be the timestamp vector.
   *
   * @param ordering an Ordering that compares RowReaders made from the vectors, consistent with row key vectors
   */
  final def rowKeyRange(key1: BinaryRecord, key2: BinaryRecord, ordering: Ordering[RowReader]): (Int, Int) = {
    if (key1 > key2) {
      (-1, -1)
    } else {
      // if firstKey/lastKey is empty it means we are working with write buffer based chunks, and need to
      // search all rows in the chunkset

      // check key1 vs firstKey
      val startRow =
        if (info.firstKey.isEmpty || key1 > info.firstKey) {
          binarySearchKeyChunks(parsers, len, ordering, key1)._1
        } else { 0 }

      // check key2 vs lastKey
      val endRow =
        if (info.lastKey.isEmpty || key2 < info.lastKey) {
          binarySearchKeyChunks(parsers, len, ordering, key2) match {
            // no match - binarySearch returns the row # _after_ the searched key.
            // So if key is less than the first item then 0 is returned since 0 is after the key.
            // Since this is the ending row inclusive we need to decrease row # - cannot include next row
            case (row, false) =>  row - 1
            // exact match - just return the row number
            case (row, true)  =>  row
          }
        } else { len - 1 }
      (startRow, endRow)
    }
  }

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
    val reader = new MutableChunkSetReader(chunkSet.info, skips, chunkSet.chunks.length)
    chunkSet.chunks.zipWithIndex.foreach { case ((colID, buffer), pos) =>
      reader.addChunk(pos, dataset.vectorMakers(colID)(buffer, chunkSet.info.numRows))
    }
    reader
  }

  /**
   * Easiest way to create a ChunkSetReader for testing from a set of vectors
   */
  def fromVectors(vectors: Array[FiloVector[_]],
                  chunkID: ChunkID = timeUUID64,
                  firstKey: BinaryRecord = BinaryRecord.empty,
                  lastKey: BinaryRecord = BinaryRecord.empty): ChunkSetReader = {
    require(vectors.size > 0, s"Cannot pass in an empty set of vectors")
    val info = ChunkSetInfo(chunkID, vectors(0).length, firstKey, lastKey)
    new ChunkSetReader(info, emptySkips, vectors)
  }

  /**
   * Does a binary search through the vectors representing row keys in a segment, finding the position
   * equal to the given key or just greater than the given key, if the key is not matched
   * (ie where the nonmatched item would be inserted).
   * Note: we take advantage of the fact that row keys cannot have null values, so no need to null check.
   *
   * @param reader a FiloRowReader based on an array of FiloVectors. First vectors must be row keys.
   * @param chunkLen the number of items in each FiloVector
   * @param ordering an Ordering that compares RowReaders made from the vectors, consistent with row key vectors
   * @param key    a RowReader representing the key to search for.  Must also have rowKeyColumns elements.
   * @return (position, true if exact match is found)  position might be equal to the number of rows in chunk
   *            if exact match not found and item compares greater than last item
   */
  // NOTE/TODO: The binary search algo below could be turned into a tail-recursive one, but be sure to do
  // a benchmark comparison first.  This is definitely in the critical path and we don't want a slowdown.
  // OTOH a tail recursive probably won't be the bottleneck.
  def binarySearchKeyChunks(reader: FiloRowReader,
                            chunkLen: Int,
                            ordering: Ordering[RowReader],
                            key: RowReader): (Int, Boolean) = {
    var len = chunkLen
    var first = 0
    while (len > 0) {
      val half = len >>> 1
      val middle = first + half
      reader.setRowNo(middle)
      val comparison = ordering.compare(reader, key)
      if (comparison == 0) {
        return (middle, true)
      } else if (comparison < 0) {
        first = middle + 1
        len = len - half - 1
      } else {
        len = half
      }
    }
    (first, ordering.equiv(reader, key))
  }

  /**
   * An alternative of the above where we wrap a FastFiloRowReader around a set of vectors for convenience.
   * @param vectors an array of FiloVectors, must start with row key columns according to the ordering
   * @param ordering an Ordering that compares RowReaders made from the vectors, consistent with row key vectors
   * @param key    a RowReader representing the key to search for.  Must also have rowKeyColumns elements.
   * @return (position, true if exact match is found)  position might be equal to the number of rows in chunk
   *            if exact match not found and item compares greater than last item
   */
  def binarySearchKeyChunks(vectors: Array[FiloVector[_]],
                            chunkLen: Int,
                            ordering: Ordering[RowReader],
                            key: RowReader): (Int, Boolean) = {
    val reader = new FastFiloRowReader(vectors)
    binarySearchKeyChunks(reader, chunkLen, ordering, key)
  }
}
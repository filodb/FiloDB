package filodb.core.datastore2

import org.velvia.filo.{BuilderEncoder, ColumnParser}
import java.nio.ByteBuffer
import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * A SegmentRowIndex stores a sorted index that maps primary keys to (chunkID, row #) -- basically the
 * location within each columnar chunk where that primary key resides.  Iterating through the index
 * allows one to read out data in PK sorted order.
 */
trait SegmentRowIndex {
  import Types._

  // Separate iterators are defined to avoid Tuple2 object allocation
  def chunkIdIterator: Iterator[ChunkID]
  def rowNumIterator: Iterator[Int]
}

/**
 * Used during the columnar chunk flush process to quickly update a rowIndex, and merge it with what exists
 * on disk already
 */
class UpdatableSegmentRowIndex[K : PrimaryKeyHelper] extends SegmentRowIndex {
  import Types._

  implicit val ordering = implicitly[PrimaryKeyHelper[K]].ordering
  var index = TreeMap[K, (ChunkID, Int)]()

  def update(key: K, chunkID: ChunkID, rowNum: Int): Unit = {
    index = index + (key -> (chunkID -> rowNum))
  }

  def update(other: UpdatableSegmentRowIndex[K]): Unit = {
    index = index ++ other.index
  }

  def chunkIdIterator: Iterator[ChunkID] = index.valuesIterator.map(_._1)
  def rowNumIterator: Iterator[Int] = index.valuesIterator.map(_._2)

  /**
   * Serializes the data in the row index.  NOTE: The primary keys are not serialized, since it is
   * assumed they exist in a different column.
   * @returns two binary Filo vectors, first one for the chunkIds, and second one for the rowNums.
   */
  def serialize(): (ByteBuffer, ByteBuffer) =
    (BuilderEncoder.seqToBuffer(chunkIdIterator.toSeq),
     BuilderEncoder.seqToBuffer(rowNumIterator.toSeq))
}

object UpdatableSegmentRowIndex {
  def apply[K](chunkIdsBuffer: ByteBuffer, rowNumsBuffer: ByteBuffer): UpdatableSegmentRowIndex[K] = ???
}

/**
 * A SegmentRowIndex which is optimized for reads from disk/memory.  Directly extracts the chunkIds
 * and rowNumbers from Filo binary vectors - does not waste time/memory constructing a TreeMap, and also
 * does not need the primary keys.  For this reason, it cannot handle merges and is really only appropriate
 * for query/read time.
 */
class BinarySegmentRowIndex(chunkIdsBuffer: ByteBuffer,
                            rowNumsBuffer: ByteBuffer) extends SegmentRowIndex {
  import Types._
  import ColumnParser._

  private val chunkIds = ColumnParser.parse[ChunkID](chunkIdsBuffer)
  private val rowNums = ColumnParser.parse[Int](rowNumsBuffer)

  def chunkIdIterator: Iterator[ChunkID] = chunkIds.toIterator
  def rowNumIterator: Iterator[Int] = rowNums.toIterator
}
package filodb.core.columnstore

import org.velvia.filo.{FiloVector, VectorBuilder, VectorReader}
import java.nio.ByteBuffer
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable.{ArrayBuffer, HashMap}

import filodb.core._

/**
 * A ChunkRowMap stores a sorted index that maps sort keys to (chunkID, row #) -- basically the
 * location within each columnar chunk where that sort key resides.  Iterating through the index
 * allows one to read out data in projection sorted order.
 */
trait ChunkRowMap {
  import filodb.core.Types._

  // Separate iterators are defined to avoid Tuple2 object allocation
  def chunkIdIterator: Iterator[ChunkID]
  def rowNumIterator: Iterator[Int]

  // Returns the next chunkId to be used for new chunks.  Usually just an increasing counter.
  def nextChunkId: ChunkID

  def isEmpty: Boolean

  /**
   * Serializes the data in the row index.  NOTE: The primary keys are not serialized, since it is
   * assumed they exist in a different column.
   * @return two binary Filo vectors, first one for the chunkIds, and second one for the rowNums.
   */
  def serialize(): (ByteBuffer, ByteBuffer)
}

/**
 * Used during the columnar chunk flush process to quickly update a rowIndex, and merge it with what exists
 * on disk already
 */
class UpdatableChunkRowMap[K: SortKeyHelper] extends ChunkRowMap {
  import filodb.core.Types._

  implicit val ordering = implicitly[SortKeyHelper[K]].ordering
  var index = TreeMap[K, (ChunkID, Int)]()
  var nextChunkId: ChunkID = 0

  def update(key: K, chunkID: ChunkID, rowNum: Int): Unit = {
    index = index + (key -> (chunkID -> rowNum))
    if (chunkID >= nextChunkId) nextChunkId = (chunkID + 1)
  }

  //scalastyle:off
  def ++(items: Seq[(K, (ChunkID, Int))]): UpdatableChunkRowMap[K] = {
    //scalastyle:on
    val newCRMap = new UpdatableChunkRowMap[K]
    newCRMap.index = this.index ++ items
    newCRMap.nextChunkId = Math.max(this.nextChunkId,
                                    items.map(_._2._1).max + 1)
    newCRMap
  }

  //scalastyle:off
  def ++(otherTree: SortedMap[K, (ChunkID, Int)]): UpdatableChunkRowMap[K] =
    ++(otherTree.toSeq)
  //scalastyle:on

  def chunkIdIterator: Iterator[ChunkID] = index.valuesIterator.map(_._1)
  def rowNumIterator: Iterator[Int] = index.valuesIterator.map(_._2)

  def isEmpty: Boolean = index.isEmpty

  def serialize(): (ByteBuffer, ByteBuffer) =
    (VectorBuilder(chunkIdIterator.toSeq).toFiloBuffer,
     VectorBuilder(rowNumIterator.toSeq).toFiloBuffer)
}

object UpdatableChunkRowMap {
  import filodb.core.Types._

  def apply[K: SortKeyHelper](items: Seq[(K, (ChunkID, Int))]): UpdatableChunkRowMap[K] =
    (new UpdatableChunkRowMap[K]) ++ items
}

/**
 * A ChunkRowMap which is optimized for reads from disk/memory.  Directly extracts the chunkIds
 * and rowNumbers from Filo binary vectors - does not waste time/memory constructing a TreeMap, and also
 * does not need the primary keys.  For this reason, it cannot handle merges and is really only appropriate
 * for query/read time.
 */
class BinaryChunkRowMap(chunkIdsBuffer: ByteBuffer,
                        rowNumsBuffer: ByteBuffer,
                        val nextChunkId: Types.ChunkID) extends ChunkRowMap {
  import filodb.core.Types._
  import VectorReader._

  val chunkIds = FiloVector[ChunkID](chunkIdsBuffer)
  val rowNums = FiloVector[Int](rowNumsBuffer)

  def chunkIdIterator: Iterator[ChunkID] = chunkIds.toIterator
  def rowNumIterator: Iterator[Int] = rowNums.toIterator
  def isEmpty: Boolean = chunkIds.length == 0
  def serialize(): (ByteBuffer, ByteBuffer) = (chunkIdsBuffer, rowNumsBuffer)
}
package filodb.core.datastore2

import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * A SegmentRowIndex stores a sorted index that maps primary keys to (chunkID, row #) -- basically the
 * location within each columnar chunk where that primary key resides.  Iterating through the index
 * allows one to read out data in PK sorted order.
 */
trait SegmentRowIndex[K] {
  import Types._

  // Separate iterators are defined to avoid Tuple2 object allocation
  def chunkIdIterator: Iterator[ChunkID]
  def rowNumIterator: Iterator[Int]
}

/**
 * Used during the columnar chunk flush process to quickly update a rowIndex, and merge it with what exists
 * on disk already
 */
class UpdatableSegmentRowIndex[K : PrimaryKeyHelper] extends SegmentRowIndex[K] {
  import Types._

  implicit val ordering = implicitly[PrimaryKeyHelper[K]].ordering
  var index = TreeMap[K, (ChunkID, Int)]()

  def update(key: K, chunkID: ChunkID, rowNum: Int): Unit = {
    index = index + (key -> (chunkID -> rowNum))
  }

  def chunkIdIterator: Iterator[ChunkID] = index.valuesIterator.map(_._1)
  def rowNumIterator: Iterator[Int] = index.valuesIterator.map(_._2)

}

// TODO: Add a separate SegmentRowIndex which is optimized for reads from disk/memory.  Basically
// This should store the chunkIDs and row #'s as separate Filo binary vectors.  This can then be iterated
// very quickly without needing to construct a TreeMap in memory.
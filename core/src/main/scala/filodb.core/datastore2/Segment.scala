package filodb.core.datastore2

import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * A Segment represents columnar chunks for a given table, partition, range of keys, and columns.
 * It also contains an index to help read data out in sorted order.
 * For more details see [[doc/sorted_chunk_merge.md]].
 */
trait Segment[K] {
  import Types._

  val keyRange: KeyRange[K]
  val index: SegmentRowIndex[K]

  def addChunk(id: ChunkID, column: String, bytes: Chunk): Unit
  def addChunks(id: ChunkID, chunks: Map[String, Chunk]): Unit
  def getChunks(column: String): Seq[(ChunkID, Chunk)]
  def getColumns: collection.Set[String]
}

class GenericSegment[K](val keyRange: KeyRange[K],
                        val index: SegmentRowIndex[K]) extends Segment[K] {
  import Types._

  val chunkIds = ArrayBuffer[ChunkID]()
  val chunks = new HashMap[String, HashMap[ChunkID, Chunk]]

  def addChunk(id: ChunkID, column: String, bytes: Chunk): Unit = {
    if (!(chunkIds contains id)) chunkIds += id
    val columnChunks = chunks.getOrElseUpdate(column, new HashMap[ChunkID, Chunk])
    columnChunks(id) = bytes
  }

  def addChunks(id: ChunkID, chunks: Map[String, Chunk]): Unit = {
    for { (col, chunk) <- chunks } { addChunk(id, col, chunk) }
  }

  def getChunks(column: String): Seq[(ChunkID, Chunk)] =
    chunkIds.map { id => (id, chunks(column)(id)) }

  def getColumns: collection.Set[String] = chunks.keySet
}

// TODO: add a Segment class that helps take in a bunch of rows and serializes them into chunks,
// also correctly updating the rowIndex and chunkID.
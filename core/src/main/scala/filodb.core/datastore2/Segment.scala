package filodb.core.datastore2

import java.nio.ByteBuffer
import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * A Segment represents columnar chunks for a given dataset, partition, range of keys, and columns.
 * It also contains an index to help read data out in sorted order.
 * For more details see [[doc/sorted_chunk_merge.md]].
 */
trait Segment[K] {
  import Types._

  val keyRange: KeyRange[K]
  val index: SegmentRowIndex

  protected val helper: PrimaryKeyHelper[K]
  def segmentId: ByteBuffer = helper.toBytes(keyRange.start)
  def dataset: TableName    = keyRange.dataset
  def partition: PartitionKey = keyRange.partition

  override def toString: String = s"Segment($dataset : $partition / ${keyRange.start})"

  def addChunk(id: ChunkID, column: String, bytes: Chunk): Unit
  def addChunks(id: ChunkID, chunks: Map[String, Chunk]): Unit
  def getChunks: Iterator[(String, ChunkID, Chunk)]
  def getColumns: collection.Set[String]
}

class GenericSegment[K : PrimaryKeyHelper](val keyRange: KeyRange[K],
                                           val index: SegmentRowIndex) extends Segment[K] {
  import Types._

  protected val helper = implicitly[PrimaryKeyHelper[K]]

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

  def getChunks: Iterator[(String, ChunkID, Chunk)] =
    for { column <- chunks.keysIterator
          chunkId <- chunks(column).keysIterator }
    yield { (column, chunkId, chunks(column)(chunkId)) }

  def getColumns: collection.Set[String] = chunks.keySet
}

// TODO: add a Segment class that helps take in a bunch of rows and serializes them into chunks,
// also correctly updating the rowIndex and chunkID.
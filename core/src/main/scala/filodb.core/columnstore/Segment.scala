package filodb.core.columnstore

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import org.velvia.filo.{VectorInfo, RowToVectorBuilder, RowReader, FiloRowReader, FastFiloRowReader}
import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ArrayBuffer, HashMap}

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.Column

/**
 * A Segment represents columnar chunks for a given dataset, partition, range of keys, and columns.
 * It also contains an index to help read data out in sorted order.
 * For more details see `doc/sorted_chunk_merge.md`.
 */
trait Segment[K] {
  val keyRange: KeyRange[K]
  val index: ChunkRowMap

  def segmentId: SegmentId = keyRange.binaryStart
  def dataset: TableName    = keyRange.dataset
  def partition: PartitionKey = keyRange.partition

  override def toString: String = s"Segment($dataset : $partition / ${keyRange.start}) columns($getColumns)"

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit
  def getChunks: Iterator[(ColumnId, ChunkID, Chunk)]
  def getColumns: collection.Set[ColumnId]

  def addChunks(id: ChunkID, chunks: Map[ColumnId, Chunk]): Unit = {
    for { (col, chunk) <- chunks } { addChunk(id, col, chunk) }
  }

  def isEmpty: Boolean = getColumns.isEmpty || index.isEmpty
}

/**
 * A generic segment implementation
 */
class GenericSegment[K](val keyRange: KeyRange[K],
                        val index: ChunkRowMap) extends Segment[K] {
  val chunkIds = ArrayBuffer[ChunkID]()
  val chunks = new HashMap[ColumnId, HashMap[ChunkID, Chunk]]

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit = {
    if (!(chunkIds contains id)) chunkIds += id
    val columnChunks = chunks.getOrElseUpdate(column, new HashMap[ChunkID, Chunk])
    columnChunks(id) = bytes
  }

  def getChunks: Iterator[(ColumnId, ChunkID, Chunk)] =
    for { column <- chunks.keysIterator
          chunkId <- chunks(column).keysIterator }
    yield { (column, chunkId, chunks(column)(chunkId)) }

  def getColumns: collection.Set[ColumnId] = chunks.keySet
}

/**
 * A Segment class for easily adding bunches of rows as new columnar chunks, based upon
 * a fixed schema and RowIngestSupport typeclass.  Automatically increments the chunkId
 * properly.
 */
class RowWriterSegment[K: SortKeyHelper](override val keyRange: KeyRange[K],
                                         schema: Seq[Column])
//scalastyle:off
//sorry for this ugly hack but only way to get scalac to pass in the SortKeyHelper
// properly it seems :(
extends GenericSegment(keyRange, null) {
  //scalastyle:on
  override val index = new UpdatableChunkRowMap[K]
  val filoSchema = schema.map {
    case Column(name, _, _, colType, serializer, false, false) =>
      require(serializer == Column.Serializer.FiloSerializer)
      VectorInfo(name, colType.clazz)
  }

  val updatingIndex = index.asInstanceOf[UpdatableChunkRowMap[K]]

  /**
   * Adds a bunch of rows as a new set of chunks with the same chunkId.  The nextChunkId from
   * a ChunkRowMap will be used.
   * @param getSortKey a function to extract the sort key K out of the RowReader.  Note that
   *                   the sort key must not be optional.
   */
  def addRowsAsChunk(rows: Iterator[RowReader], getSortKey: RowReader => K): Unit = {
    val newChunkId = index.nextChunkId
    val builder = new RowToVectorBuilder(filoSchema)
    // NOTE: some RowReaders, such as the one from RowReaderSegment, must be iterators
    // since rowNo in FastFiloRowReader is mutated.
    rows.zipWithIndex.foreach { case (r, i) =>
      updatingIndex.update(getSortKey(r), newChunkId, i)
      builder.addRow(r)
    }
    val chunkMap = builder.convertToBytes()
    chunkMap.foreach { case (col, bytes) => addChunk(newChunkId, col, bytes) }
  }

  def addRowsAsChunk(rows: Iterator[(PartitionKey, K, RowReader)]): Unit = {
    val newChunkId = index.nextChunkId
    val builder = new RowToVectorBuilder(filoSchema)
    rows.zipWithIndex.foreach { case ((_, k, r), i) =>
      updatingIndex.update(k, newChunkId, i)
      builder.addRow(r)
    }
    val chunkMap = builder.convertToBytes()
    chunkMap.foreach { case (col, bytes) => addChunk(newChunkId, col, bytes) }
  }
}

/**
 * A segment optimized for reading and iterating rows of data out of the chunks.
 * You cannot modify the chunkRowMap.... addChunks is only intended for filling in chunk data
 * as it is read from the ColumnStore.  Then, you call rowIterator.
 * It assumes that the ChunkID is a counter with low cardinality, and stores everything
 * as arrays for extremely fast access.
 */
class RowReaderSegment[K](val keyRange: KeyRange[K],
                          val index: BinaryChunkRowMap,
                          columns: Seq[Column]) extends Segment[K] with StrictLogging {
  import RowReaderSegment._

  // chunks(chunkId)(columnNum)
  val chunks = Array.fill(index.nextChunkId)(new Array[ByteBuffer](columns.length))
  val clazzes = columns.map(_.columnType.clazz).toArray

  val colIdToNumber = columns.zipWithIndex.map { case (col, idx) => (col.name, idx) }.toMap

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit =
    if (id < chunks.size) {
      //scalastyle:off
      if (bytes == null) logger.warn(s"null chunk detected! id=$id column=$column in $keyRange")
      //scalastyle:on
      chunks(id)(colIdToNumber(column)) = bytes
    } else {
      // Probably the result of corruption, such as OOM while writing segments
      logger.debug(s"Ignoring chunk id=$id column=$column in $keyRange, chunks.size=${chunks.size}")
    }

  def getChunks: Iterator[(ColumnId, ChunkID, Chunk)] = {
    for { chunkId <- (0 until index.nextChunkId).toIterator
          columnNo <- (0 until columns.length).toIterator }
    yield { (columns(columnNo).name, chunkId, chunks(chunkId)(columnNo)) }
  }

  def getColumns: collection.Set[ColumnId] = columns.map(_.name).toSet

  private def getReaders(readerFactory: RowReaderFactory): Array[FiloRowReader] =
    (0 until index.nextChunkId).map { chunkId =>
      val reader = readerFactory(chunks(chunkId), clazzes)
      // Cheap check for empty chunks
      if (reader.parsers(0).length == 0) {
        logger.warn(s"empty chunk detected!  chunkId=$chunkId in $keyRange")
      }
      reader
    }.toArray

  /**
   * Iterates over rows in this segment in the sort order defined by the ChunkRowMap.
   * Creates new RowReaders every time, so that multiple calls could be made and original
   * state in the Segment is not mutated.
   */
  def rowIterator(readerFactory: RowReaderFactory = DefaultReaderFactory): Iterator[RowReader] = {
    val readers = getReaders(readerFactory)
    if (readers.isEmpty) { Iterator.empty }
    else {
      new Iterator[RowReader] {
        var curChunk = 0
        var curReader = readers(curChunk)
        val len = index.chunkIds.length
        var i = 0

        // NOTE: manually iterate over the chunkIds / rowNums FiloVectors, instead of using the
        // iterator methods, which are extremely slow and boxes everything
        def hasNext: Boolean = i < len
        def next: RowReader = {
          val nextChunk = index.chunkIds(i)
          if (nextChunk != curChunk) {
            curChunk = nextChunk
            curReader = readers(nextChunk)
          }
          curReader.rowNo = index.rowNums(i)
          i += 1
          curReader
        }
      }
    }
  }

  /**
   * Returns an Iterator over (reader, chunkId, rowNum).  Intended for efficient ChunkRowMap
   * merging operations.
   */
  def rowChunkIterator(readerFactory: RowReaderFactory = DefaultReaderFactory):
      Iterator[(RowReader, ChunkID, Int)] = {
    val readers = getReaders(readerFactory)
    new Iterator[(RowReader, ChunkID, Int)] {
      val chunkIdIter = index.chunkIdIterator
      val rowNumIter = index.rowNumIterator

      def hasNext: Boolean = chunkIdIter.hasNext
      def next: (RowReader, ChunkID, Int) = {
        val nextChunk = chunkIdIter.next
        val nextRowNo = rowNumIter.next
        readers(nextChunk).rowNo = nextRowNo
        (readers(nextChunk), nextChunk, nextRowNo)
      }
    }
  }
}

object RowReaderSegment {
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]]) => FiloRowReader

  private val DefaultReaderFactory: RowReaderFactory =
    (bytes, clazzes) => new FastFiloRowReader(bytes, clazzes)

  def apply[K](genSeg: GenericSegment[K], schema: Seq[Column]): RowReaderSegment[K] = {
    val (chunkIdBuf, rowNumBuf) = genSeg.index.serialize()
    val binChunkMap = new BinaryChunkRowMap(chunkIdBuf, rowNumBuf, genSeg.index.nextChunkId)
    val readSeg = new RowReaderSegment(genSeg.keyRange, binChunkMap, schema)
    genSeg.getChunks.foreach { case (col, id, bytes) => readSeg.addChunk(id, col, bytes) }
    readSeg
  }
}

package filodb.core.store

import java.nio.ByteBuffer

import com.typesafe.scalalogging.slf4j.StrictLogging
import filodb.core.Types._
import filodb.core.metadata.{Column, RichProjection}
import org.velvia.filo.{FastFiloRowReader, FiloRowReader, RowReader, RowToVectorBuilder}

import scala.collection.mutable.{ArrayBuffer, HashMap}

//scalastyle:off
case class SegmentInfo[+PK, +SK](partition: PK, segment: SK) {
//scalastyle:on
  /**
   * Recast this SegmentInfo in the PK and SK types of another projection object.
   * Be careful using this.  This is needed because dependent-path types in Scala are not that smart.
   * If a class takes a RichProjection as a parameter, then the types of that SegmentInfo becomes
   * tied to that class's projection parameter, and Scala does not know that the original projection
   * object has the same types.
   */
  def basedOn(projection: RichProjection): SegmentInfo[projection.PK, projection.SK] =
    this.asInstanceOf[SegmentInfo[projection.PK, projection.SK]]
}

/**
 * A Segment represents all the rows that belong to a single segment key in a given projection.
 * When new rows belonging to that segment key are added, they are added to the same segment.
 */
trait Segment {
  val projection: RichProjection
  def segInfo: SegmentInfo[projection.PK, projection.SK]
  def index: ChunkRowMap

  def binaryPartition: BinaryPartition = projection.partitionType.toBytes(segInfo.partition)
  def segmentId: SegmentId = projection.segmentType.toBytes(segInfo.segment)

  override def toString: String = s"$segInfo columns($getColumns)"

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
 *
 * NOTE: making the constructor private is a hack around Scala not supporting path-dependent types
 * in class constructor param lists. See
 * http://stackoverflow.com/questions/32763822/use-of-path-dependent-type-as-a-class-parameter
 */
class GenericSegment private(val projection: RichProjection,
                             _segInfo: SegmentInfo[RichProjection#PK, RichProjection#SK],
                             val index: ChunkRowMap) extends Segment {
  def this(projection: RichProjection, index: ChunkRowMap)
          (segInfo: SegmentInfo[projection.PK, projection.SK]) =
    this(projection, segInfo, index)

  def segInfo: SegmentInfo[projection.PK, projection.SK] = _segInfo.basedOn(projection)

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
class RowWriterSegment private(override val projection: RichProjection,
                               segInfo: SegmentInfo[RichProjection#PK, RichProjection#SK],
                               schema: Seq[Column])
extends GenericSegment(projection,
                       new UpdatableChunkRowMap[projection.RK]()(projection.rowKeyType))(
                       segInfo.basedOn(projection)) {
  def this(projection: RichProjection, schema: Seq[Column])
          (segInfo: SegmentInfo[projection.PK, projection.SK]) = this(projection, segInfo, schema)

  val filoSchema = Column.toFiloSchema(schema)

  val updatingIndex = index.asInstanceOf[UpdatableChunkRowMap[projection.RK]]

  /**
   * Adds a bunch of rows as a new set of chunks with the same chunkId.  The nextChunkId from
   * a ChunkRowMap will be used.
   */
  def addRowsAsChunk(rows: Iterator[RowReader]): Unit = {
    val newChunkId = index.nextChunkId
    val builder = new RowToVectorBuilder(filoSchema)
    val rowKeyFunc = projection.rowKeyFunc
    // NOTE: some RowReaders, such as the one from RowReaderSegment, must be iterators
    // since rowNo in FastFiloRowReader is mutated.
    rows.zipWithIndex.foreach { case (r, i) =>
      updatingIndex.update(rowKeyFunc(r).asInstanceOf[projection.RK], newChunkId, i)
      builder.addRow(r)
    }
    val chunkMap = builder.convertToBytes()
    chunkMap.foreach { case (col, bytes) => addChunk(newChunkId, col, bytes) }
  }

  def addRichRowsAsChunk(rows: Iterator[(RichProjection#RK, RowReader)]): Unit = {
    val newChunkId = index.nextChunkId
    val builder = new RowToVectorBuilder(filoSchema)
    rows.zipWithIndex.foreach { case ((k, r), i) =>
      updatingIndex.update(k.asInstanceOf[projection.RK], newChunkId, i)
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
class RowReaderSegment(val projection: RichProjection,
                       _segInfo: SegmentInfo[_, _],
                       val index: BinaryChunkRowMap,
                       columns: Seq[Column]) extends Segment with StrictLogging {
  import RowReaderSegment._

  def segInfo: SegmentInfo[projection.PK, projection.SK] = _segInfo.basedOn(projection)

  // chunks(chunkId)(columnNum)
  val chunks = Array.fill(index.nextChunkId)(new Array[ByteBuffer](columns.length))
  val clazzes = columns.map(_.columnType.clazz).toArray

  val colIdToNumber = columns.zipWithIndex.map { case (col, idx) => (col.name, idx) }.toMap

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit =
    if (id < chunks.size) {
      //scalastyle:off
      if (bytes == null) logger.warn(s"null chunk detected! id=$id column=$column in $segInfo")
      //scalastyle:on
      chunks(id)(colIdToNumber(column)) = bytes
    } else {
      // Probably the result of corruption, such as OOM while writing segments
      logger.debug(s"Ignoring chunk id=$id column=$column in $segInfo, chunks.size=${chunks.size}")
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
      if (clazzes.nonEmpty && reader.parsers(0).length == 0) {
        logger.warn(s"empty chunk detected!  chunkId=$chunkId in $segInfo")
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
        private final val len = index.chunkIds.length
        private var i = 0

        // NOTE: manually iterate over the chunkIds / rowNums FiloVectors, instead of using the
        // iterator methods, which are extremely slow and boxes everything
        final def hasNext: Boolean = i < len
        final def next: RowReader = {
          val nextChunk = index.chunkIds(i)
          if (nextChunk != curChunk) {
            curChunk = nextChunk
            curReader = readers(nextChunk)
          }
          curReader.setRowNo(index.rowNums(i))
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
        readers(nextChunk).setRowNo(nextRowNo)
        (readers(nextChunk), nextChunk, nextRowNo)
      }
    }
  }
}

object RowReaderSegment {
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]]) => FiloRowReader

  val DefaultReaderFactory: RowReaderFactory =
    (bytes, clazzes) => new FastFiloRowReader(bytes, clazzes)

  def apply(genSeg: GenericSegment, schema: Seq[Column]): RowReaderSegment = {
    val (chunkIdBuf, rowNumBuf) = genSeg.index.serialize()
    val binChunkMap = new BinaryChunkRowMap(chunkIdBuf, rowNumBuf, genSeg.index.nextChunkId)
    val readSeg = new RowReaderSegment(genSeg.projection, genSeg.segInfo, binChunkMap, schema)
    genSeg.getChunks.foreach { case (col, id, bytes) => readSeg.addChunk(id, col, bytes) }
    readSeg
  }
}

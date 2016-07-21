package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.TreeMap
import org.velvia.filo.{RowToVectorBuilder, RowReader, FiloRowReader, FastFiloRowReader}
import scala.collection.mutable.{ArrayBuffer, HashMap}

import filodb.core.KeyType
import filodb.core.Types._
import filodb.core.metadata.{Column, RichProjection}

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

  def binaryPartition: BinaryPartition = projection.partitionType.toBytes(segInfo.partition)
  def segmentId: SegmentId = projection.segmentType.toBytes(segInfo.segment)

  override def toString: String = s"$segInfo"
}

/**
 * A Segment class tracking ingestion state.  Automatically increments the chunkId
 * properly.  Also holds state of all ChunkSetInfos and row replacements so we can calculate
 * them as we add rows.  Basically, this class has all the state for adding to a segment.
 * It is meant to be updated as ChunkSets are added and state modified.
 * Its state can be recovered from the index written to ColumnStore.
 */
class SegmentState private(projection: RichProjection,
                           segInfo: SegmentInfo[RichProjection#PK, RichProjection#SK],
                           infos: Seq[ChunkSetInfo],
                           schema: Seq[Column]) {
  def this(projection: RichProjection, schema: Seq[Column], infos: Seq[ChunkSetInfo])
          (segInfo: SegmentInfo[projection.PK, projection.SK]) = this(projection, segInfo, infos, schema)

  val filoSchema = Column.toFiloSchema(schema)
  val infoMap = new TreeMap[ChunkID, ChunkSetInfo]
  infos.foreach { info => infoMap.put(info.id, info) }

  // TODO(velvia): Use some TimeUUID for chunkID instead
  var nextChunkId = (infos.foldLeft(-1) { case (chunkId, info) => Math.max(info.id, chunkId) }) + 1

  /**
   * Creates a new ChunkSet, properly creating a ChunkID and populating the list of
   * overriding rows.
   * @param rows rows to be chunkified sorted in order of rowkey
   * NOTE: This is not a pure function, it updates the state of this segmentState.
   */
  def newChunkSet(rows: Seq[RowReader]): ChunkSet = {
    // NOTE: some RowReaders, such as the one from RowReaderSegment, must be iterators
    // since rowNo in FastFiloRowReader is mutated.
    val builder = new RowToVectorBuilder(filoSchema)
    val rowKeyFunc = projection.rowKeyFunc
    val info = ChunkSetInfo(nextChunkId, rows.length,
                            projection.rowKeyType.toBytes(rowKeyFunc(rows.head)),
                            projection.rowKeyType.toBytes(rowKeyFunc(rows.last)))
    nextChunkId = nextChunkId + 1
    // TODO: how to check for overrides
    rows.foreach(builder.addRow)
    infoMap.put(info.id, info)
    ChunkSet(info, Nil, builder.convertToBytes())
  }
}

/**
 * A Segment holding ChunkSets to be written out to the ColumnStore
 */
class ChunkSetSegment(val projection: RichProjection,
                      _segInfo: SegmentInfo[_, _]) extends Segment {
  val chunkSets = new ArrayBuffer[ChunkSet]

  def segInfo: SegmentInfo[projection.PK, projection.SK] = _segInfo.basedOn(projection)

  def addChunkSet(state: SegmentState, rows: Seq[RowReader]): Unit =
    chunkSets.append(state.newChunkSet(rows))

  def infosAndSkips: ChunkSetInfo.ChunkInfosAndSkips =
    chunkSets.map { chunkSet => (chunkSet.info, chunkSet.skips) }
}

/**
 * A segment optimized for reading and iterating rows of data out of the chunks.
 * addChunks is only intended for filling in chunk data
 * as it is read from the ColumnStore.  Then, you call rowIterator.
 * It assumes that the ChunkID is a counter with low cardinality, and stores everything
 * as arrays for extremely fast access.
 */
class RowReaderSegment(val projection: RichProjection,
                       _segInfo: SegmentInfo[_, _],
                       chunkInfos: Seq[(ChunkSetInfo, Array[Int])],
                       columns: Seq[Column]) extends Segment with StrictLogging {
  import RowReaderSegment._

  def segInfo: SegmentInfo[projection.PK, projection.SK] = _segInfo.basedOn(projection)

  val chunks = new HashMap[ChunkID, Array[ByteBuffer]]
  val clazzes = columns.map(_.columnType.clazz).toArray

  val colIdToNumber = columns.zipWithIndex.map { case (col, idx) => (col.name, idx) }.toMap

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit =
    if (id < chunks.size) {
      //scalastyle:off
      if (bytes == null) logger.warn(s"null chunk detected! id=$id column=$column in $segInfo")
      //scalastyle:on
      val chunkArray = chunks.getOrElseUpdate(id, new Array[ByteBuffer](columns.length))
      chunkArray(colIdToNumber(column)) = bytes
    } else {
      // Probably the result of corruption, such as OOM while writing segments
      logger.debug(s"Ignoring chunk id=$id column=$column in $segInfo, chunks.size=${chunks.size}")
    }

  def getColumns: collection.Set[ColumnId] = columns.map(_.name).toSet

  private def getReaders(readerFactory: RowReaderFactory): Array[(FiloRowReader, Int, Array[Int])] =
    chunkInfos.map { case (ChunkSetInfo(id, numRows, _, _), skipArray) =>
      val reader = readerFactory(chunks(id), clazzes, numRows)
      // Cheap check for empty chunks
      if (clazzes.nonEmpty && reader.parsers(0).length == 0) {
        logger.warn(s"empty chunk detected!  chunkId=$id in $segInfo")
      }
      (reader, numRows, skipArray)
    }.toArray

  /**
   * Iterates over rows in this segment, chunkset by chunkset, skipping over any rows
   * defined in skiplist.
   * Creates new RowReaders every time, so that multiple calls could be made and original
   * state in the Segment is not mutated.
   */
  def rowIterator(readerFactory: RowReaderFactory = DefaultReaderFactory): Iterator[RowReader] = {
    val readers = getReaders(readerFactory)
    if (readers.isEmpty) { Iterator.empty }
    else {
      // yes I know you can do Iterator.flatMap, but this is a very tight inner loop and we must write
      // high performance ugly Java code
      new Iterator[RowReader] {
        var curChunk = 0
        var (curReader, curChunkLen, curSkiplist) = readers(curChunk)
        private var i = 0
        private var skipIndex = 0

        // NOTE: manually iterate over the chunkIds / rowNums FiloVectors, instead of using the
        // iterator methods, which are extremely slow and boxes everything
        final def hasNext: Boolean = {
          // Skip past any rows that need skipping, then determine if we have rows left
          while (skipIndex < curSkiplist.size && i == curSkiplist(skipIndex)) {
            i += 1
            skipIndex += 1
          }
          if (i < curChunkLen) return true
          // So at this point we've reached end of current chunk.  See if more chunks
          if (curChunk < readers.size - 1) {
            // At end of current chunk, but more chunks to go
            curChunk += 1
            curReader = readers(curChunk)._1
            curSkiplist = readers(curChunk)._3
            curChunkLen = readers(curChunk)._2
            i = 0
            skipIndex = 0
            true
          } else { false }
        }

        final def next: RowReader = {
          curReader.setRowNo(i)
          i += 1
          curReader
        }
      }
    }
  }
}

object RowReaderSegment {
  type RowReaderFactory = (Array[ByteBuffer], Array[Class[_]], Int) => FiloRowReader

  val DefaultReaderFactory: RowReaderFactory =
    (bytes, clazzes, len) => new FastFiloRowReader(bytes, clazzes, len)
}

package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.TreeMap
import org.velvia.filo._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.FiniteDuration

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
                           schema: Seq[Column],
                           rowKeysForChunk: ChunkID => Array[ByteBuffer]) {
  import collection.JavaConverters._

  def this(projection: RichProjection,
           schema: Seq[Column],
           infos: Seq[ChunkSetInfo],
           rowKeysForChunk: ChunkID => Array[ByteBuffer])
          (segInfo: SegmentInfo[projection.PK, projection.SK]) =
    this(projection, segInfo, infos, schema, rowKeysForChunk)

  // Only use this constructor if you are really sure there is no overlap, like for tests
  def this(projection: RichProjection,
           schema: Seq[Column],
           infos: Seq[ChunkSetInfo])
          (segInfo: SegmentInfo[projection.PK, projection.SK]) =
    this(projection, segInfo, infos, schema, (x: ChunkID) => Array.empty)

  def this(proj: RichProjection,
           schema: Seq[Column],
           infos: Seq[ChunkSetInfo],
           scanner: ColumnStoreScanner,
           version: Int,
           timeout: FiniteDuration)
          (segInfo: SegmentInfo[proj.PK, proj.SK])
          (implicit ec: ExecutionContext) =
    this(proj, segInfo, infos, schema,
         (chunk: ChunkID) => Await.result(scanner.readRowKeyChunks(proj, version, chunk)
                                          (segInfo.basedOn(proj)), timeout))

  val filoSchema = Column.toFiloSchema(schema)
  val infoMap = new TreeMap[ChunkID, ChunkSetInfo]
  infos.foreach { info => infoMap.put(info.id, info) }
  val rowKeyColNos = projection.rowKeyColIndices.toArray

  // TODO(velvia): Use some TimeUUID for chunkID instead
  var nextChunkId = (infos.foldLeft(-1) { case (chunkId, info) => Math.max(info.id, chunkId) }) + 1

  /**
   * Creates a new ChunkSet, properly creating a ChunkID and populating the list of
   * overriding rows.
   * @param rows rows to be chunkified sorted in order of rowkey
   * NOTE: This is not a pure function, it updates the state of this segmentState.
   */
  def newChunkSet(rows: Iterator[RowReader]): ChunkSet = {
    // NOTE: some RowReaders, such as the one from RowReaderSegment, must be iterators
    // since rowNo in FastFiloRowReader is mutated.
    val builder = new RowToVectorBuilder(filoSchema)
    val infoChunkId = nextChunkId
    nextChunkId = nextChunkId + 1
    var numRows = 0
    //scalastyle:off
    var firstKey: RowReader = null
    var lastKey: RowReader = null
    //scalastyle:on
    rows.foreach { row =>
      builder.addRow(row)
      val rowKey = RoutingRowReader(row, rowKeyColNos)
      if (numRows == 0) firstKey = rowKey
      lastKey = rowKey
      numRows += 1
    }

    val chunkMap = builder.convertToBytes()
    val info = ChunkSetInfo(infoChunkId, numRows, firstKey, lastKey)
    val skips = ChunkSetInfo.detectSkips(projection, info,
                                         projection.rowKeyColumns.map(c => chunkMap(c.name)).toArray,
                                         infoMap.values.asScala.toSeq,
                                         rowKeysForChunk)
    infoMap.put(info.id, info)
    ChunkSet(info, skips, chunkMap)
  }
}

/**
 * A Segment holding ChunkSets to be written out to the ColumnStore
 */
class ChunkSetSegment(val projection: RichProjection,
                      _segInfo: SegmentInfo[_, _]) extends Segment {
  val chunkSets = new ArrayBuffer[ChunkSet]

  def segInfo: SegmentInfo[projection.PK, projection.SK] = _segInfo.basedOn(projection)

  def addChunkSet(state: SegmentState, rows: Iterator[RowReader]): Unit =
    chunkSets.append(state.newChunkSet(rows))

  def addChunkSet(state: SegmentState, rows: Seq[RowReader]): Unit = addChunkSet(state, rows.toIterator)

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

  def addChunk(id: ChunkID, column: ColumnId, bytes: Chunk): Unit = {
    //scalastyle:off
    if (bytes == null) logger.warn(s"null chunk detected! id=$id column=$column in $segInfo")
    //scalastyle:on
    val chunkArray = chunks.getOrElseUpdate(id, new Array[ByteBuffer](columns.length))
    chunkArray(colIdToNumber(column)) = bytes
  }

  def getColumns: collection.Set[ColumnId] = columns.map(_.name).toSet

  // TODO: Detect when no chunks corresponding to chunkInfos.  Either missing column or
  // inconsistency in data.  When inconsistency, should try reading again.
  private def getReaders(readerFactory: RowReaderFactory): Array[(FiloRowReader, Int, Array[Int])] =
  if (columns.nonEmpty) {
    chunkInfos
      .filter { case (info, _) => chunks contains info.id }
      .map { case (ChunkSetInfo(id, numRows, _, _), skipArray) =>
      val reader = readerFactory(chunks(id), clazzes, numRows)
      // Cheap check for empty chunks
      if (clazzes.nonEmpty && reader.parsers(0).length == 0) {
        logger.warn(s"empty chunk detected!  chunkId=$id in $segInfo")
      }
      (reader, numRows, skipArray)
    }.toArray
  } else {
    // No columns, probably a select count(*) query
    // Return an empty vector of the exact length, to allow proper counting of records
    chunkInfos.map { case (ChunkSetInfo(id, numRows, _, _), skipArray) =>
      //scalastyle:off
      (readerFactory(Array[ByteBuffer](null), Array(classOf[Int]), numRows), numRows, skipArray)
      //scalastyle:on
    }.toArray
  }

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

  def apply(cs: ChunkSetSegment, schema: Seq[Column]): RowReaderSegment = {
    val infosSkips = cs.chunkSets.map { cs => (cs.info, cs.skips) }
    val seg = new RowReaderSegment(cs.projection, cs.segInfo, ChunkSetInfo.collectSkips(infosSkips), schema)
    for { chunkSet <- cs.chunkSets
          (colId, bytes) <- chunkSet.chunks } {
      seg.addChunk(chunkSet.info.id, colId, bytes)
    }
    seg
  }
}

package filodb.core.store

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.TreeMap
import org.velvia.filo._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

import filodb.core.{KeyType, KeyRange}
import filodb.core.Types._
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.binaryrecord.BinaryRecord

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

import SegmentStateSettings._

object SegmentState {
  type IDAndFilter = (ChunkID, BloomFilter[Long])
  type InfoAndFilter = (ChunkSetInfo, BloomFilter[Long])

  def emptyFilter(settings: SegmentStateSettings): BloomFilter[Long] =
    BloomFilter[Long](settings.filterElems, settings.filterFalsePositiveRate)
}

/** Companion object */
object SegmentStateSettings {
  // It's really important that Bloom Filters are not overstuffed
  val DefaultFilterElements = 5000
  val DefaultFilterFalsePositiveRate = 1E-6
  val DefaultTimeout = 30.seconds
}

final case class SegmentStateSettings(filterElems: Int = DefaultFilterElements,
                                      filterFalsePositiveRate: Double = DefaultFilterFalsePositiveRate,
                                      timeout: FiniteDuration = DefaultTimeout)

/**
 * A Segment class tracking ingestion state.  Automatically increments the chunkId
 * properly.  Also holds state of all ChunkSetInfos and bloom filters so we can calculate
 * them as we add rows.  Basically, this class has all the state for adding to a segment.
 * It is meant to be updated as ChunkSets are added and state modified.
 *
 * Specific implementations control how rowkeys and bloom filters are retrieved and kept in memory.
 * Only responsible for keeping and updating state; ChunkSet creation is elsewhere.
 *
 * NOTE:  Tried to make this class immutable, but had to revert it as in the future the state will
 * probably get more complex with interval trees etc., and too many changes elsewhere in code base. TODO.
 *
 * @param projection the RichProjection for the dataset to ingest
 * @param schema     the columns to ingest
 * @param infosAndFilters existing ChunkSetInfos and BloomFilters for a segment
 * @param masterFilter the master BloomFilter covering all records in the segment
 * @param settings   the SegmentStateSettings
 */
abstract class SegmentState(val projection: RichProjection,
                            schema: Seq[Column],
                            settings: SegmentStateSettings) extends StrictLogging {
  import collection.JavaConverters._
  import SegmentState._

  val filoSchema = Column.toFiloSchema(schema)
  private val rowKeyIndices = projection.rowKeyColIndices.toArray

  // TODO(velvia): Use some TimeUUID for chunkID instead
  private var _nextChunkId = 0
  private val _infosAndFilters = new ArrayBuffer[SegmentState.InfoAndFilter]
  private var _masterFilter = emptyFilter(settings)


  // == Abstract methods ==
  def getRowKeyChunks(chunkID: ChunkID): Array[ByteBuffer]

  def infosAndFilters: Seq[SegmentState.InfoAndFilter] = _infosAndFilters
  def masterFilter: BloomFilter[Long] = _masterFilter
  def nextChunkId: ChunkID = _nextChunkId

  /**
   * Returns the number of chunks kept by this SegmentState instance.
   */
  def numChunks: Int = _infosAndFilters.length

  /**
   * Methods to append new ChunkSets or infos/filters to the current state
   */
  def add(chunkSet: ChunkSet): Unit = {
    _infosAndFilters += (chunkSet.info -> chunkSet.bloomFilter)
    _masterFilter = _masterFilter.union(chunkSet.bloomFilter)
    _nextChunkId = chunkSet.info.id + 1
  }

  def add(newInfosFilters: Seq[InfoAndFilter]): Unit = {
    _infosAndFilters ++= newInfosFilters
    _masterFilter = newInfosFilters.foldLeft(_masterFilter) { case (aggBf, (_, bf)) => aggBf.union(bf) }
    infosAndFilters.lastOption.foreach { case (info, _) => _nextChunkId = info.id + 1 }
  }

  def filterRowKeys(rowKeys: Array[BinaryRecord]): Array[BinaryRecord] =
    rowKeys.filter { k => masterFilter.mightContain(k.cachedHash64) }

  // It's good to create a separate RowKey that does not depend on existing RowReaders or chunks, because
  // the row key metadata gets saved for a long time, and we don't want dangling memory references to
  // chunks that have been flushed already
  def makeRowKey(row: RowReader): BinaryRecord =
    BinaryRecord(projection.rowKeyBinSchema, RoutingRowReader(row, rowKeyIndices))

  def makeBloomFilter(rowKeys: Array[BinaryRecord]): BloomFilter[Long] = {
    val bf = SegmentState.emptyFilter(settings)
    rowKeys.foreach { k => bf.add(k.cachedHash64) }
    bf
  }
}

class ColumnStoreSegmentState private(proj: RichProjection,
                                      segInfo: SegmentInfo[RichProjection#PK, RichProjection#SK],
                                      schema: Seq[Column],
                                      version: Int,
                                      scanner: ColumnStoreScanner,
                                      settings: SegmentStateSettings)
                                     (implicit ec: ExecutionContext)
extends SegmentState(proj, schema, settings) {
  def getRowKeyChunks(chunkID: ChunkID): Array[ByteBuffer] =
    Await.result(scanner.readRowKeyChunks(proj, version, chunkID)(segInfo.basedOn(proj)),
                 settings.timeout)
}

object ColumnStoreSegmentState extends StrictLogging {
  /**
   * Creates a new ColumnStoreSegmentState by retrieving existing indices and Bloom Filters
   */
  def apply(proj: RichProjection,
            schema: Seq[Column],
            version: Int,
            scanner: ColumnStoreScanner,
            settings: SegmentStateSettings = SegmentStateSettings())
           (segInfo: SegmentInfo[proj.PK, proj.SK])
           (implicit ec: ExecutionContext): ColumnStoreSegmentState = {
    val range = KeyRange(segInfo.partition, segInfo.segment, segInfo.segment, endExclusive = false)
    logger.debug(s"Retrieving segment indexes and filters from column store: $range")
    val indexRead = scanner.scanIndices(proj, version, SinglePartitionRangeScan(range))
    val indexIt = Await.result(indexRead, settings.timeout)
    val infos = indexIt.toSeq.headOption.map(_.infosAndSkips).getOrElse(Nil).map(_._1)
    val filterMap = if (infos.nonEmpty) {
     Await.result(scanner.readFilters(proj, version, (infos.head.id, infos.last.id))
                  (segInfo)(ec), settings.timeout).toMap
    } else { Map.empty[ChunkID, BloomFilter[Long]] }
    val emptyFilter = SegmentState.emptyFilter(settings)
    val state = new ColumnStoreSegmentState(proj, segInfo, schema, version, scanner, settings)(ec)
    state.add(infos.map { info => (info, filterMap.getOrElse(info.id, emptyFilter)) })
    state
  }
}

/**
 * A Segment holding ChunkSets to be written out to the ColumnStore
 */
class ChunkSetSegment(val projection: RichProjection,
                      _segInfo: SegmentInfo[_, _]) extends Segment {
  val chunkSets = new ArrayBuffer[ChunkSet]

  def segInfo: SegmentInfo[projection.PK, projection.SK] = _segInfo.basedOn(projection)

  /**
   * Creates a chunk set from rows and current segment state, and returns the new state
   */
  def addChunkSet(state: SegmentState,
                  rows: Iterator[RowReader],
                  detectSkips: Boolean = true): Unit = {
    val newChunkSet = if (detectSkips) { ChunkSet.withSkips(state, rows) }
                      else             { ChunkSet(state, rows) }
    chunkSets += newChunkSet
    state.add(newChunkSet)
  }

  def addChunkSet(state: SegmentState,
                  rows: Seq[RowReader]): Unit = addChunkSet(state, rows.toIterator)

  def infosSkipsFilters: ChunkSetInfo.IndexAndFilterSeq =
    chunkSets.map { chunkSet => (chunkSet.info, chunkSet.skips, chunkSet.bloomFilter) }
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

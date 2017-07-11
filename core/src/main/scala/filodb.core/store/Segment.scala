package filodb.core.store

import bloomfilter.mutable.BloomFilter
import com.typesafe.scalalogging.StrictLogging
import java.nio.ByteBuffer
import java.util.TreeMap
import monix.execution.Scheduler
import org.velvia.filo._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.{Column, RichProjection}
import filodb.core.query.{PartitionChunkIndex, RowkeyPartitionChunkIndex}
import filodb.core.Types._
import filodb.core.KeyType

//scalastyle:off
case class SegmentInfo[+PK, +SK](partition: PartitionKey, segment: SK) {
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

  def partition: PartitionKey = segInfo.partition

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
                            chunkIndex: PartitionChunkIndex,
                            schema: Seq[Column],
                            settings: SegmentStateSettings) extends StrictLogging {
  import collection.JavaConverters._
  import SegmentState._

  val filoSchema = Column.toFiloSchema(schema)
  private val rowKeyIndices = projection.rowKeyColIndices.toArray
  private val _filterMap = new HashMap[ChunkID, BloomFilter[Long]]
  private var _masterFilter = emptyFilter(settings)

  // == Abstract methods ==
  def getRowKeyVectors(key: BinaryRecord, chunkID: ChunkID): Array[FiloVector[_]]

  def infos: Iterator[ChunkSetInfo] = chunkIndex.allChunks.map(_._1)
  def filter(id: ChunkID): Option[BloomFilter[Long]] = _filterMap.get(id)
  def masterFilter: BloomFilter[Long] = _masterFilter
  def nextChunkId: ChunkID = timeUUID64

  /**
   * Returns the number of chunks kept by this SegmentState instance.
   */
  def numChunks: Int = chunkIndex.numChunks

  /**
   * Methods to append new ChunkSets or infos/filters to the current state
   */
  def add(chunkSet: ChunkSet): Unit = {
    chunkIndex.add(chunkSet.info, chunkSet.skips)
    _filterMap(chunkSet.info.id) = chunkSet.bloomFilter
    _masterFilter = _masterFilter.union(chunkSet.bloomFilter)
  }

  def addFilters(filters: Map[ChunkID, BloomFilter[Long]]): Unit = {
    _filterMap ++= filters
    _masterFilter = filters.foldLeft(_masterFilter) { case (aggBf, (id, bf)) => aggBf.union(bf) }
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
                                      chunkIndex: PartitionChunkIndex,
                                      schema: Seq[Column],
                                      version: Int,
                                      scanner: ColumnStore with ColumnStoreScanner,
                                      settings: SegmentStateSettings)
extends SegmentState(proj, chunkIndex, schema, settings) {
  def getRowKeyVectors(key: BinaryRecord, chunkID: ChunkID): Array[FiloVector[_]] =
    scanner.readRowKeyVectors(proj, version, segInfo.partition, key, chunkID)
}

object ColumnStoreSegmentState extends StrictLogging {
  /**
   * Creates a new ColumnStoreSegmentState by retrieving existing indices and Bloom Filters
   */
  def apply(proj: RichProjection,
            schema: Seq[Column],
            version: Int,
            scanner: ColumnStore with ColumnStoreScanner,
            settings: SegmentStateSettings = SegmentStateSettings())
           (segInfo: SegmentInfo[proj.PK, proj.SK])
           (implicit ec: Scheduler): ColumnStoreSegmentState = {
    logger.debug(s"Retrieving partition indexes and filters from column store: $segInfo")
    val indexObs = scanner.scanPartitions(proj, version, SinglePartitionScan(segInfo.partition))
                          .firstOrElseL(new RowkeyPartitionChunkIndex(segInfo.partition, proj))
    val index = Await.result(indexObs.runAsync, settings.timeout)
    val filterMap = if (index.numChunks > 0) {
      Await.result(scanner.readFilters(proj, version, (Long.MinValue, Long.MaxValue))
                   (segInfo)(ec), settings.timeout).toMap
    } else { Map.empty[ChunkID, BloomFilter[Long]] }
    val state = new ColumnStoreSegmentState(proj, segInfo, index, schema, version, scanner, settings)
    state.addFilters(filterMap)
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


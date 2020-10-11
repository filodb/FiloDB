package filodb.core.memstore

import com.typesafe.scalalogging.StrictLogging
import spire.syntax.cfor._

import filodb.core.DatasetRef
import filodb.core.Types._
import filodb.core.metadata.{Column, PartitionSchema, Schema}
import filodb.core.store._
import filodb.memory.{BinaryRegion, BinaryRegionLarge, BlockMemFactory, MemFactory}
import filodb.memory.data.{ChunkMap, Shutdown}
import filodb.memory.format._
import filodb.memory.format.MemoryReader._

object TimeSeriesPartition extends StrictLogging {
  type AppenderArray = Array[BinaryAppendableVector[_]]

  val nullChunks    = UnsafeUtils.ZeroPointer.asInstanceOf[AppenderArray]
  val nullInfo      = ChunkSetInfo(UnsafeUtils.ZeroPointer.asInstanceOf[BinaryRegion.NativePointer])

  // Use global logger so we can save a few fields for each of millions of TSPartitions  :)
  val _log = logger

  def partKeyString(schema: PartitionSchema, partKeyBase: Any, partKeyOffset: Long): String = {
    schema.binSchema.stringify(partKeyBase, partKeyOffset)
  }

  val publishIntervalTag = "_step_"
  // If you are changing the tag value, consider adding to the predefined list of keys in filo-defaults
  // for space optimization
  val publishIntervalFinder = TagPublishIntervalFinder(publishIntervalTag)
}

// Temporary holder of chunk metadata pointer and appenders array before optimize/finalize step
// In all cases should exist only until the next flush cycle
final case class InfoAppenders(info: ChunkSetInfo, appenders: TimeSeriesPartition.AppenderArray)

/**
 * A MemStore Partition holding chunks of data for different columns (a schema) for time series use cases.
 *
 * This implies:
 * - New data is assumed to mostly be increasing in time
 * - Thus newer chunks generally contain newer stretches of time
 * - Completed chunks are flushed to disk
 * - Oldest chunks are flushed first
 * - There are no skips or replacing rows.  Everything is append only.  This greatly simplifies the ingestion
 *   engine.
 *
 * Design is for high ingestion rates.
 * Concurrency/Ingestion flow:
 *   The idea is to alternate between ingest() and switchBuffers() in the ingestion thread.
 *     This allows for safe and cheap write buffer churn without losing any data.
 *   switchBuffers() is called before flush() is called in another thread, possibly.
 *
 * The main data structure used is inherited from ChunkMap, an efficient, offheap sorted map.
 * Note that other than the variables used in this class, there is NO JVM-managed memory used
 * for managing chunks.  Thus the amount of JVM-managed memory used for a partition is constant
 * regardless of the number of chunks in a TSPartition. The partition key and infoMap are both
 * in offheap write buffers, and chunks and chunk metadata are kept in offheap block memory.
 *
 * Note: Inheritance is chosen over composition to avoid an extra object allocation, which
 * speeds up GC and reduces memory overhead a bit.
 */
class TimeSeriesPartition(val partID: Int,
                          val schema: Schema,
                          partitionKey: BinaryRegion.NativePointer,
                          val shard: Int,
                          bufferPool: WriteBufferPool,
                          val shardStats: TimeSeriesShardStats,
                          memFactory: MemFactory,
                          initMapSize: Int)
extends ChunkMap(memFactory, initMapSize) with ReadablePartition {
  import TimeSeriesPartition._

  // Really important that buffer pool schema matches
  require(bufferPool.schema == schema.data,
    s"BufferPool schema was ${bufferPool.schema} but partition schema was ${schema.data}")

  def partKeyBase: Array[Byte] = UnsafeUtils.ZeroPointer.asInstanceOf[Array[Byte]]
  def partKeyOffset: Long = partitionKey

  def publishInterval: Option[Long] = {
    publishIntervalFinder.findPublishIntervalMs(schema.partition.hash, UnsafeUtils.ZeroArray, partitionKey)
  }
  /**
    * Incoming, unencoded data gets appended to these BinaryAppendableVectors.
    * There is one element for each column of the schema. All of them have the same chunkId.
    * Var mutates when buffers are switched for optimization back to NULL, until new data arrives.
    * in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]],
    * and new chunk is added to the partition.
    * Note that if this is not NULL, then it is always the most recent element of infoMap.
    */
  protected var currentChunks = nullChunks
  protected var currentInfo = nullInfo

  /**
    * True if partition is actively ingesting.
    * This flag is maintained in addition to the activelyIngesting bitmap maintained in
    * TimeSeriesShard because this flag is checked for each sample and bitmap.get is not fast.
    */
  @volatile var ingesting: Boolean = false

  /**
   * The newest ChunkID that has been flushed or encoded.  You can think of the progression of chunks like this,
   * from newest to oldest (thus represents a traversal of infoMap):
   * current -> notEncoded -> encodedNotFlushed -> Flushed -> Flushed -> (eventually) reclaimed
   *
   * During flush we ensure any unencoded chunks that are not current (writing) are encoded and updates these IDs.
   */
  private var newestFlushedID = Long.MinValue

  /**
   * A list of appenders yet to be encoded.  Normally empty, until switchBuffers is called and before optimization
   * happens.  Note that this is separate from newestFlushedID because it is possible for encoding to happen
   * correctly but not flushes, so that we would need to try flushing already-encoded blocks.
   */
  private var appenders: List[InfoAppenders] = Nil

  /**
   * Ingests a new row, adding it to currentChunks.
   * If ingesting a new row causes WriteBuffers to overflow, then the current chunks are encoded, a new set
   * of appending chunks are obtained, and we re-ingest into the new chunks.
   *
   * @param ingestionTime time (as milliseconds from 1970) at the data source
   * @param blockHolder the BlockMemFactory to use for encoding chunks in case of WriteBuffer overflow
   */
  def ingest(ingestionTime: Long, row: RowReader, blockHolder: BlockMemFactory,
             maxChunkTime: Long = Long.MaxValue): Unit = {
    // NOTE: lastTime is not persisted for recovery.  Thus the first sample after recovery might still be out of order.
    val ts = schema.timestamp(row)
    if (ts < timestampOfLatestSample) {
      shardStats.outOfOrderDropped.increment()
      return
    }

    val newChunk = currentChunks == nullChunks
    if (newChunk) initNewChunk(ts, ingestionTime)

    if (ts - currentInfo.startTime > maxChunkTime) {
      // we have reached maximum userTime in chunk. switch buffers, start a new chunk and ingest
      switchBuffersAndIngest(ingestionTime, ts, row, blockHolder, maxChunkTime)
    } else {
      cforRange { 0 until schema.numDataColumns } { col =>
        currentChunks(col).addFromReaderNoNA(row, col) match {
          case r: VectorTooSmall =>
            switchBuffersAndIngest(ingestionTime, ts, row, blockHolder, maxChunkTime)
            return
          // Different histogram bucket schema: need a new vector here
          case BucketSchemaMismatch =>
            switchBuffersAndIngest(ingestionTime, ts, row, blockHolder, maxChunkTime)
            return
          case other: AddResponse =>
        }
      }
      ChunkSetInfo.incrNumRows(currentInfo.infoAddr)

      // Update the end time as well.  For now assume everything arrives in increasing order
      ChunkSetInfo.setEndTime(currentInfo.infoAddr, ts)

      if (newChunk) {
        // Publish it now that it has something.
        infoPut(currentInfo)
      }
    }
  }

  private def switchBuffersAndIngest(ingestionTime: Long,
                                     userTime: Long,
                                     row: RowReader,
                                     blockHolder: BlockMemFactory,
                                     maxChunkTime: Long): Unit = {
    // NOTE: a very bad infinite loop is possible if switching buffers fails (if the # rows is 0) but one of the
    // vectors fills up.  This is possible if one vector fills up but the other one does not for some reason.
    // So we do not call ingest again unless switcing buffers succeeds.
    // re-ingest every element, allocating new WriteBuffers
    if (switchBuffers(blockHolder, encode = true)) { ingest(ingestionTime, row, blockHolder, maxChunkTime) }
    else { _log.warn("EMPTY WRITEBUFFERS when switchBuffers called!  Likely a severe bug!!! " +
      s"Part=$stringPartition userTime=$userTime numRows=${currentInfo.numRows}") }
  }

  protected def initNewChunk(startTime: Long, ingestionTime: Long): Unit = {
    // First row of a chunk, set the start time to it
    val (infoAddr, newAppenders) = bufferPool.obtain()
    val currentChunkID = chunkID(startTime, ingestionTime / 1000) // convert to seconds
    ChunkSetInfo.setChunkID(infoAddr, currentChunkID)
    ChunkSetInfo.setIngestionTime(infoAddr, ingestionTime)
    ChunkSetInfo.resetNumRows(infoAddr)    // Must reset # rows otherwise it keeps increasing!
    currentInfo = ChunkSetInfo(infoAddr)
    currentChunks = newAppenders
    // Don't publish the new chunk just yet. Wait until it has one row.
  }

  private def nonEmptyWriteBuffers: Boolean = currentInfo != nullInfo && currentInfo.numRows > 0

  /**
   * Atomically switches the writeBuffers to a null one.  If and when we get more data, then
   * we will initialize the writeBuffers to new ones.  This way dead partitions not getting more data will not
   * waste empty appenders.
   * Also populates a complete ChunkSetInfo so that these chunks may be queried reliably.
   * To guarantee no more writes happen when switchBuffers is called, have ingest() and switchBuffers() be
   * called from a single thread / single synchronous stream.
   */
  def switchBuffers(blockHolder: BlockMemFactory, encode: Boolean = false): Boolean =
    nonEmptyWriteBuffers && {
      val oldInfo = currentInfo
      val oldAppenders = currentChunks

      // Right after this all ingest() calls will check and potentially append to new chunks
      // We can reset currentInfo because it is already stored in infoMap map
      currentChunks = nullChunks
      currentInfo = nullInfo

      if (encode) { encodeOneChunkset(oldInfo, oldAppenders, blockHolder) }
      else        { appenders = InfoAppenders(oldInfo, oldAppenders) :: appenders }
      true
    }

  /**
   * Optimizes a set of chunks into the smallest BinaryVectors and updates index structure.  May be called concurrently.
   * Optimized chunks as well as chunk metadata are written into offheap block memory so they no longer consume
   */
  private def encodeOneChunkset(info: ChunkSetInfo, appenders: AppenderArray, blockHolder: BlockMemFactory) = {
    blockHolder.startMetaSpan()
    val frozenVectors = try {
      // optimize and compact chunks
      appenders.zipWithIndex.map { case (appender, i) =>
        // This assumption cannot break. We should ensure one vector can be written
        // to one block always atleast as per the current design.
        // If this gets triggered, decrease the max writebuffer size so smaller chunks are encoded
        require(blockHolder.blockAllocationSize() > appender.frozenSize)
        val optimized = appender.optimize(blockHolder)
        shardStats.encodedBytes.increment(BinaryVector.totalBytes(nativePtrReader, optimized))
        if (schema.data.columns(i).columnType == Column.ColumnType.HistogramColumn)
          shardStats.encodedHistBytes.increment(BinaryVector.totalBytes(nativePtrReader, optimized))
        optimized
      }
    } catch { case e: Exception =>
      // Shutdown process right away! Reaching this state means that we could not reclaim
      // a whole bunch of blocks possibly because they were not marked as reclaimable,
      // because of some bug. Cleanup or rollback at this point is not viable.
      Shutdown.haltAndCatchFire(new RuntimeException("Error occurred when encoding vectors", e))
      throw e
    }
    shardStats.numSamplesEncoded.increment(info.numRows)
    // Now, write metadata into offheap block metadata space and update infosChunks
    val metaAddr = blockHolder.endMetaSpan(TimeSeriesShard.writeMeta(_, partID, info, frozenVectors),
                                           schema.data.blockMetaSize.toShort)

    val newInfo = ChunkSetInfo(metaAddr + 4)
    _log.trace(s"Adding new chunk ${newInfo.debugString} to part $stringPartition")
    infoPut(newInfo)

    // release older write buffers back to pool.  Nothing at this point should reference the older appenders.
    bufferPool.release(info.infoAddr, appenders)
    frozenVectors
  }

  /**
   * Encodes (as necessary) and produces a series of ChunkSets for chunks not flushed yet.
   * Only one thread should call makeFlushChunks() at a time.
   * This may be called concurrently w.r.t. makeFlushChunks(), but switchBuffers() must be called first.
   *
   * For now, assume switchBuffers() is called synchronously with ingest() so that no changes occur to the
   * flushingChunks when this method is called.  If this is not true, then a retry loop is needed to guarantee
   * that nothing changes from underneath optimize().
   *
   * TODO: for partitions getting very little data, in the future, instead of creating a new set of chunks,
   * we might wish to flush current chunks as they are for persistence but then keep adding to the partially
   * filled currentChunks.  That involves much more state, so do much later.
   */
  def makeFlushChunks(blockHolder: BlockMemFactory): Iterator[ChunkSet] = {
    // Now return all the un-flushed chunksets
    encodeAndReleaseBuffers(blockHolder)
    infosToBeFlushed
      .map { info =>
        _log.trace(s"Preparing to flush chunk ${info.debugString} of part $stringPartition")
        ChunkSet(info, partitionKey, Nil,
                 (0 until schema.numDataColumns).map { i => BinaryVector.asBuffer(info.vectorPtr(i)) },
                 // Updates the newestFlushedID when the flush succeeds.
                 info => updateFlushedID(info))
      }
  }

  // Encodes remaining non-current appenders and releases any remaining WriteBuffers back to the pool
  def encodeAndReleaseBuffers(blockHolder: BlockMemFactory): Unit =
    appenders.foreach { case ia @ InfoAppenders(info, chunks) =>
      encodeOneChunkset(info, chunks, blockHolder)
      // Remove the list one at a time in case of errors during encoding
      appenders = appenders.filterNot(_ == ia)
    }

  def numChunks: Int = chunkmapSize // inherited from ChunkMap
  def appendingChunkLen: Int = if (currentInfo != nullInfo) currentInfo.numRows else 0

  /**
   * Number of unflushed chunksets lying around.  Goes up every time a new writebuffer is allocated and goes down
   * when flushes happen.  Computed dynamically from current infosChunks state.
   * NOTE: since sliceToEnd is inclusive, we need to start just past the newestFlushedID
   */
  def unflushedChunksets: Int = chunkmapSliceToEnd(newestFlushedID + 1).count

  private def allInfos: ChunkInfoIterator = new ElementChunkInfoIterator(chunkmapIterate)

  // NOT including currently flushing writeBuffer chunks if there are any
  private[memstore] def infosToBeFlushed: ChunkInfoIterator =
    new ElementChunkInfoIterator(chunkmapSliceToEnd(newestFlushedID + 1))
               .filter(_ != currentInfo)  // filter out the appending chunk

  def infos(method: ChunkScanMethod): ChunkInfoIterator = method match {
    case AllChunkScan        => allInfos
    case InMemoryChunkScan   => allInfos
    case r: TimeRangeChunkScan => allInfos.filter { ic =>
                                    ic.intersection(r.startTime, r.endTime).isDefined
                                  }
    case WriteBufferChunkScan => if (currentInfo == nullInfo) ChunkInfoIterator.empty
                                else {
                                  // Return a single element iterator which holds a shared lock.
                                  try {
                                    new OneChunkInfo(currentInfo)
                                  } catch {
                                    case e: Throwable => chunkmapReleaseShared(); throw e;
                                  }
                                }
  }

  def infos(startTime: Long, endTime: Long): ChunkInfoIterator =
    allInfos.filter(_.intersection(startTime, endTime).isDefined)

  def hasChunks(method: ChunkScanMethod): Boolean = {
    val chunkIter = infos(method)
    try {
      chunkIter.hasNext
    } finally {
      chunkIter.close()
    }
  }

  private class OneChunkInfo(info: => ChunkSetInfo) extends ChunkInfoIterator {
    var closed = false
    var valueSeen = false

    def close(): Unit = {
      if (!closed) doClose()
    }

    private def doClose(): Unit = {
      closed = true
      chunkmapReleaseShared()
    }

    def hasNext: Boolean = {
      if (valueSeen) doClose()
      !closed
    }

    def nextInfo: ChunkSetInfo = {
      if (closed) throw new NoSuchElementException()
      if (!valueSeen) {
        chunkmapAcquireShared()
        valueSeen = true
      }
      return info
    }

    final def lock(): Unit = chunkmapAcquireShared()
    final def unlock(): Unit = chunkmapReleaseShared()
  }

  def partKeyHash: Int = schema.partKeySchema.partitionHash(partKeyBase, partKeyOffset)

  /**
    * startTime of earliest chunk in memory.
    * Long.MaxValue if no chunk is present
    */
  final def earliestTime: Long = {
    if (numChunks == 0) {
      Long.MaxValue
    } else {
      // Acquire shared lock to safely access the native pointer.
      chunkmapWithShared(ChunkSetInfo(chunkmapDoGetFirst).startTime)
    }
  }

  /**
    * Timestamp of most recent sample in memory. If none, returns -1
    *
    * Remember that -1 can be returned even when there may be data in Cassandra that has
    * not been paged into memory
    */
  final def timestampOfLatestSample: Long = {
    if (currentInfo != nullInfo) {   // fastest: get the endtime from current chunk
      currentInfo.endTime
    } else if (numChunks > 0) {
      // Acquire shared lock to safely access the native pointer.
      chunkmapWithShared(infoLast.endTime)
    } else {
      -1
    }
  }

  // Disabled for now. Requires a shared lock on the inherited map.
  //def dataChunkPointer(id: ChunkID, columnID: Int): BinaryVector.BinaryVectorPtr = infoGet(id).vectorPtr(columnID)

  final def removeChunksAt(id: ChunkID): Unit = {
    // Remove all chunks at and lower than the given chunk. Doing so prevents a hole from
    // emerging in the middle which ODP can't easily cope with.
    val amt = chunkmapWithExclusive(chunkmapDoRemoveFloor(id))
    shardStats.chunkIdsEvicted.increment(amt)
  }

  final def hasChunksAt(id: ChunkID): Boolean = chunkmapContains(id)

  // Used for adding chunksets that are paged in, ie that are already persisted
  // Atomic and multi-thread safe; only mutates state if chunkID not present
  final def addChunkInfoIfAbsent(id: ChunkID, infoAddr: BinaryRegion.NativePointer): Boolean = {
    chunkmapWithExclusive({
      val inserted = chunkmapDoPutIfAbsent(infoAddr, newestFlushedID)
      // Make sure to update newestFlushedID so that flushes work correctly and don't try to flush these chunksets
      if (inserted) updateFlushedID(infoGet(id))
      inserted
    })
  }

  final def updateFlushedID(info: ChunkSetInfo): Unit = {
    newestFlushedID = Math.max(newestFlushedID, info.id)
  }

  // Caller must hold lock on the inherited map.
  private def infoGet(id: ChunkID): ChunkSetInfo = ChunkSetInfo(chunkmapDoGet(id))

  // Caller must hold lock on the inherited map.
  private[core] def infoLast(): ChunkSetInfo = ChunkSetInfo(chunkmapDoGetLast)

  private def infoPut(info: ChunkSetInfo): Unit = {
    chunkmapWithExclusive(chunkmapDoPut(info.infoAddr, newestFlushedID))
  }

  // Free memory (esp offheap) attached to this TSPartition and return buffers to common pool
  def shutdown(): Unit = {
    chunkmapFree()
  }

  override protected def finalize(): Unit = {
    memFactory.freeMemory(partKeyOffset)
    if (currentInfo != nullInfo) bufferPool.release(currentInfo.infoAddr, currentChunks)
  }
}

/**
 * A variant of the above which logs every sample ingested and buffer switching/encoding event,
 * for debugging purposes.  See the trace-filters StoreConfig / ingestion config setting.
 *
 * NOTE(velvia): The reason why I used inheritance was not so much memory but just ease of implementation.
 * With composition we'd need to add in tons of methods and clutter things up quite a bit. If it simply
 * implemented ReadablePartition that might break things in a bunch of places.
 * So best way to keep changes small and balance out different needs
 */
class TracingTimeSeriesPartition(partID: Int,
                                 ref: DatasetRef,
                                 schema: Schema,
                                 partitionKey: BinaryRegion.NativePointer,
                                 shard: Int,
                                 bufferPool: WriteBufferPool,
                                 shardStats: TimeSeriesShardStats,
                                 memFactory: MemFactory,
                                 initMapSize: Int) extends
TimeSeriesPartition(partID, schema, partitionKey, shard, bufferPool, shardStats, memFactory, initMapSize) {
  import TimeSeriesPartition._

  _log.info(s"Creating TracingTimeSeriesPartition dataset=$ref schema=${schema.name} partId=$partID $stringPartition")

  override def ingest(ingestionTime: Long, row: RowReader, blockHolder: BlockMemFactory, maxChunkTime: Long): Unit = {
    val ts = row.getLong(0)
    _log.info(s"Ingesting dataset=$ref schema=${schema.name} shard=$shard partId=$partID $stringPartition " +
               s"ingestionTime=$ingestionTime ts=$ts " +
               (1 until schema.numDataColumns).map(row.getAny).mkString("[", ",", "]"))
    super.ingest(ingestionTime, row, blockHolder, maxChunkTime)
  }

  override def switchBuffers(blockHolder: BlockMemFactory, encode: Boolean = false): Boolean = {
    _log.info(s"SwitchBuffers dataset=$ref schema=${schema.name} shard=$shard partId=$partID $stringPartition - " +
               s"encode=$encode for currentChunk ${currentInfo.debugString}")
    super.switchBuffers(blockHolder, encode)
  }

  override protected def initNewChunk(startTime: Long, ingestionTime: Long): Unit = {
    _log.info(s"dataset=$ref schema=${schema.name} shard=$shard partId=$partID $stringPartition - " +
               s"initNewChunk($startTime, $ingestionTime)")
    super.initNewChunk(startTime, ingestionTime)
    _log.info(s"dataset=$ref schema=${schema.name} shard=$shard partId=$partID $stringPartition - " +
               s"newly created ChunkInfo ${currentInfo.debugString}")
  }
}


final case class PartKeyRowReader(records: Iterator[PartKeyWithTimes]) extends Iterator[RowReader] {
  var currVal: PartKeyWithTimes = _

  private val rowReader = new RowReader {
    def notNull(columnNo: Int): Boolean = true
    def getBoolean(columnNo: Int): Boolean = ???
    def getInt(columnNo: Int): Int = ???
    def getLong(columnNo: Int): Long = {
      columnNo match {
        case 1 => currVal.startTime
        case 2 => currVal.endTime
        case _ => throw new IllegalArgumentException(s"Bad column $columnNo")
      }
    }
    def getDouble(columnNo: Int): Double = ???
    def getFloat(columnNo: Int): Float = ???
    def getString(columnNo: Int): String = ???
    def getAny(columnNo: Int): Any = ???

    def getBlobBase(columnNo: Int): Any = if (columnNo == 0) currVal.base
                                          else throw new IllegalArgumentException(s"Bad column $columnNo")
    def getBlobOffset(columnNo: Int): Long = if (columnNo == 0) currVal.offset
                                             else throw new IllegalArgumentException(s"Bad column $columnNo")
    def getBlobNumBytes(columnNo: Int): Int =
      if (columnNo == 0) BinaryRegionLarge.numBytes(currVal.base, currVal.offset) + BinaryRegionLarge.lenBytes
      else throw new IllegalArgumentException(s"Bad column $columnNo")
  }

  override def hasNext: Boolean = records.hasNext

  override def next(): RowReader = {
    currVal = records.next()
    rowReader
  }
}

package filodb.core.memstore

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentSkipListMap

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.reactive.Observable
import scalaxy.loops._

import filodb.core.Iterators
import filodb.core.Types._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.query.ChunkSetReader
import filodb.core.store._
import filodb.memory.BlockMemFactory
import filodb.memory.format._

final case class InfoChunks(info: ChunkSetInfo, chunks: Array[BinaryVector[_]])

object TimeSeriesPartition {
  val nullChunks    = UnsafeUtils.ZeroPointer.asInstanceOf[Array[BinaryAppendableVector[_]]]
}

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
 * This partition accepts as parameter a chunk source that represents the persistent column store
 * to fetch older chunks for the partition to support read-through caching. Chunks are fetched from the persistent
 * store lazily when a query requests chunks from the partition.
 *
 * TODO: eliminate chunkIDs, that can be stored in the index
 */
class TimeSeriesPartition(val partID: Int,
                          val dataset: Dataset,
                          val binPartition: PartitionKey,
                          val shard: Int,
                          val chunkSource: ChunkSource,
                          bufferPool: WriteBufferPool,
                          skipDemandPaging: Boolean,
                          pagedChunkStore: DemandPagedChunkStore,
                          val shardStats: TimeSeriesShardStats)
                         (implicit val queryScheduler: Scheduler) extends FiloPartition with StrictLogging {
  import ChunkSetInfo._
  import collection.JavaConverters._
  import TimeSeriesPartition._

  /**
   * This is the ONE main data structure holding the chunks of a TSPartition.  It is appended to as chunks arrive
   * and are encoded.  The last or most recent item should be the write buffer chunks.
   *    Key(ChunkID) -> Value InfoChunks(ChunkSetInfo, Array[BinaryVector])
   * Thus it is sorted by increasing chunkID, yet addressable by chunkID, and concurrent.
   */
  private var infosChunks = new ConcurrentSkipListMap[ChunkID, InfoChunks]

  /**
    * Incoming, unencoded data gets appended to these BinaryAppendableVectors.
    * There is one element for each column of the dataset. All of them have the same chunkId.
    * Var mutates when buffers are switched for optimization back to NULL, until new data arrives.
    * in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]],
    * and new chunk is added to the partition.
    * Note that if this is not NULL, then it is always the most recent element of infosChunks.
    */
  private var currentChunks = nullChunks
  private var currentChunkID = Long.MinValue

  /**
   * The newest ChunkID that has been flushed or encoded.  You can think of the progression of chunks like this,
   * from newest to oldest (thus represents a traversal of infosChunks):
   * current -> notEncoded -> encodedNotFlushed -> Flushed -> Flushed -> (eventually) reclaimed
   *
   * During flush we ensure any unencoded chunks that are not current (writing) are encoded and updates these IDs.
   */
  private var newestFlushedID = Long.MinValue

  /**
   * The number of rows in the currentChunks
   */
  var appendingChunkLen = 0

  /**
    * Number of columns in the dataset
    */
  private final val numColumns = dataset.dataColumns.length

  private val jvmStartTime = ManagementFactory.getRuntimeMXBean.getStartTime
  private val partitionLoadedFromPersistentStore = new AtomicBoolean(skipDemandPaging)

  /**
   * Ingests a new row, adding it to currentChunks.
   * If ingesting a new row causes WriteBuffers to overflow, then the current chunks are encoded, a new set
   * of appending chunks are obtained, and we re-ingest into the new chunks.
   *
   * @param blockHolder the BlockMemFactory to use for encoding chunks in case of WriteBuffer overflow
   */
  final def ingest(row: RowReader, offset: Long, blockHolder: BlockMemFactory): Unit = {
    if (currentChunks == nullChunks) initNewChunk()
    for { col <- 0 until numColumns optimized } {
      currentChunks(col).addFromReaderNoNA(row, col) match {
        case r: VectorTooSmall =>
          switchBuffers(blockHolder, encode=true)
          ingest(row, offset, blockHolder)   // re-ingest every element, allocating new WriteBuffers
          return
        case other: AddResponse =>
      }
    }
    appendingChunkLen += 1
  }

  /**
   * Atomically switches the writeBuffers to a null one.  If and when we get more data, then
   * we will initialize the writeBuffers to new ones.  This way dead partitions not getting more data will not
   * waste empty appenders.
   * Also populates a complete ChunkSetInfo so that these chunks may be queried reliably.
   * To guarantee no more writes happen when switchBuffers is called, have ingest() and switchBuffers() be
   * called from a single thread / single synchronous stream.
   */
  final def switchBuffers(blockHolder: BlockMemFactory, encode: Boolean = false): Unit = if (appendingChunkLen > 0) {
    // Create ChunkSetInfo
    val reader = new FastFiloRowReader(currentChunks.asInstanceOf[Array[FiloVector[_]]])
    reader.setRowNo(0)
    val firstRowKey = dataset.rowKey(reader)
    reader.setRowNo(appendingChunkLen - 1)
    val lastRowKey = dataset.rowKey(reader)
    val chunkInfo = infosChunks.get(currentChunkID).info.copy(numRows = appendingChunkLen,
                                                              firstKey = firstRowKey,
                                                              lastKey = lastRowKey)
    infosChunks.put(chunkInfo.id, InfoChunks(chunkInfo, currentChunks.asInstanceOf[Array[BinaryVector[_]]]))

    // Right after this all ingest() calls will check and potentially append to new chunks
    currentChunks = nullChunks
    currentChunkID = Long.MinValue
    appendingChunkLen = 0

    if (encode) encodeOneChunkset(chunkInfo.id, blockHolder)
  }

  /**
   * Optimizes a set of chunks into the smallest BinaryVectors and updates index structure.  May be called concurrently.
   */
  private def encodeOneChunkset(id: ChunkID, blockHolder: BlockMemFactory) = {
    val infoChunks = infosChunks.get(id)
    //scalastyle:off
    require(infoChunks != null, s"Calling encodeOneChunkset on ChunkID $id but it doesn't exist!!")
    //scalastyle:on
    val appenders = infoChunks.chunks.asInstanceOf[Array[BinaryAppendableVector[_]]]
    blockHolder.startMetaSpan()
    // optimize and compact chunks
    val frozenVectors = appenders.zipWithIndex.map { case (appender, i) =>
      // This assumption cannot break. We should ensure one vector can be written
      // to one block always atleast as per the current design.
      // If this gets triggered, decrease the max writebuffer size so smaller chunks are encoded
      require(blockHolder.blockAllocationSize() > appender.frozenSize)
      val optimized = appender.optimize(blockHolder)
      shardStats.encodedBytes.increment(optimized.numBytes)
      optimized
    }
    shardStats.numSamplesEncoded.increment(infoChunks.info.numRows)
    shardStats.numChunksEncoded.increment(frozenVectors.length)

    infosChunks.put(id, infoChunks.copy(chunks = frozenVectors))
    blockHolder.endMetaSpan(TimeSeriesShard.writeMeta(_, partID, id), TimeSeriesShard.BlockMetaAllocSize)

    // release older write buffers back to pool.  Nothing at this point should reference the older appenders.
    bufferPool.release(appenders)
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
    infosChunksToBeFlushed
      .map { case InfoChunks(info, chunks) =>
        ChunkSet(info, binPartition, Nil,
                 chunks.zipWithIndex.map { case (vect, pos) => (pos, vect.toFiloBuffer) },
                 // Updates the newestFlushedID when the flush succeeds.
                 // NOTE: by using a method instead of closure, we allocate less
                 updateFlushedID)
      }
  }

  // Encodes and releases any remaining WriteBuffers back to the pool
  def encodeAndReleaseBuffers(blockHolder: BlockMemFactory): Unit =
    infosChunksToBeFlushed
      .foreach { case InfoChunks(info, chunks) =>
        chunks(0) match {
          case v: BinaryAppendableVector[_] => encodeOneChunkset(info.id, blockHolder)
          case other: BinaryVector[_]       =>
        }
      }

  def numChunks: Int = infosChunks.size

  /**
   * Number of unflushed chunksets lying around.  Goes up every time a new writebuffer is allocated and goes down
   * when flushes happen.  Computed dynamically from current infosChunks state.
   */
  def unflushedChunksets: Int = infosChunks.tailMap(newestFlushedID, false).size

  private def allInfosChunks: Iterator[InfoChunks] = infosChunks.values.asScala.toIterator

  // NOT including currently flushing writeBuffer chunks if there are any
  private def infosChunksToBeFlushed: Iterator[InfoChunks] =
    infosChunks.tailMap(newestFlushedID, false).values
               .asScala.toIterator
               .filterNot(_.info.id == currentChunkID)  // filter out the appending chunk

  private def getVectors(columnIds: Array[Int],
                         vectors: Array[BinaryVector[_]]): Array[FiloVector[_]] = {
    val finalVectors = new Array[FiloVector[_]](columnIds.size)
    for { i <- 0 until columnIds.size optimized } {
      finalVectors(i) = if (Dataset.isPartitionID(columnIds(i))) { constPartitionVector(columnIds(i)) }
                        else                                     { vectors(columnIds(i)) }
    }
    finalVectors
  }

  private def readersFromMemory(method: ChunkScanMethod, columnIds: Array[Int]): Iterator[ChunkSetReader] = {
    val infosVects = method match {
      case AllChunkScan               =>
        allInfosChunks
      // To derive time range: r.startkey.getLong(0) -> r.endkey.getLong(0)
      case r: RowKeyChunkScan         =>
        allInfosChunks.filter { ic =>
          ic.info.id == currentChunkID ||      // most recent chunk gets a free pass, cannot compare range
          ic.info.intersection(r.startkey, r.endkey).isDefined
        }
      case LastSampleChunkScan        =>
        if (infosChunks.isEmpty) Iterator.empty else Iterator.single(infosChunks.get(infosChunks.lastKey))
    }

    infosVects.map { case InfoChunks(info, vectArray) =>
      //scalastyle:off
      require(vectArray != null, "INCONSISTENCY! vectArray is null!")
      //scalastyle:on
      val realInfo = info match {
        // First chunk, fill in real chunk length.  TODO: also fill in current first & last key
        case i @ ChunkSetInfo(_, -1, _, _) => i.copy(numRows = appendingChunkLen)
        case _: Any                        => info
      }
      val chunkset = getVectors(columnIds, vectArray)
      shardStats.numChunksQueried.increment(chunkset.length)
      new ChunkSetReader(realInfo, emptySkips, chunkset)
    }
  }

  def lastVectors: Array[FiloVector[_]] = currentChunks.asInstanceOf[Array[FiloVector[_]]]

  /**
    * Initializes vectors, chunkIDs for a new chunkset/chunkID.
    * This is called after switchBuffers() upon the first data that arrives.
    * Also compacts infosChunks tombstones.
    */
  private def initNewChunk(): Unit = {
    while (currentChunks == nullChunks) {
      try {
        currentChunks = bufferPool.obtain()
      } catch {
        case e: NoSuchElementException =>
          logger.warn(s"Out of write buffers.  Waiting in a loop until we have such buffers")
          Thread sleep 10000
      }
    }
    currentChunkID = timeUUID64
    val newInfo = ChunkSetInfo(currentChunkID, -1, BinaryRecord.empty, BinaryRecord.empty)
    infosChunks.put(currentChunkID, InfoChunks(newInfo, currentChunks.asInstanceOf[Array[BinaryVector[_]]]))
  }

  /**
    * Indicates that the partition's chunks have been loaded from persistent store into memory
    * and the reads do not need to go to the persistent column store
    *
    * @return true if chunks have been loaded, false otherwise
    */
  def cacheIsWarm(): Boolean = {
    if (partitionLoadedFromPersistentStore.get) {
      true
    } else {
      val timeSinceStart = System.currentTimeMillis - jvmStartTime
      if (timeSinceStart > pagedChunkStore.retentionMillis) partitionLoadedFromPersistentStore.set(true)
      timeSinceStart > pagedChunkStore.retentionMillis
    }
  }

  override def streamReaders(method: ChunkScanMethod, columnIds: Array[Int]): Observable[ChunkSetReader] = {
    if (cacheIsWarm) { // provide read-through functionality
      super.streamReaders(method, columnIds) // return data from memory
    } else {
      demandPageFromColStore(method, columnIds)
    }
  }

  override def readers(method: ChunkScanMethod, columnIds: Array[Int]): Iterator[ChunkSetReader] = {
    import Iterators._
    if (cacheIsWarm) { // provide read-through functionality
      readersFromMemory(method, columnIds) // return data from memory
    } else {
      demandPageFromColStore(method, columnIds).toIterator()
    }
  }

  final def removeChunksAt(id: ChunkID): Unit = {
    infosChunks.remove(id)
    shardStats.chunkIdsEvicted.increment()
  }

  // Used for adding chunksets that are paged in, ie that are already persisted
  final def addChunkSet(info: ChunkSetInfo, bytes: Array[BinaryVector[_]]): Unit = {
    infosChunks.put(info.id, InfoChunks(info, bytes))
    // Make sure to update newestFlushedID so that flushes work correctly and don't try to flush these chunksets
    updateFlushedID(info)
  }

  final def updateFlushedID(info: ChunkSetInfo): Unit = {
    newestFlushedID = Math.max(newestFlushedID, info.id)
  }

  /**
    * Implements the on-demand paging for the time series partition
    */
  private def demandPageFromColStore(method: ChunkScanMethod, columnIds: Array[Int]): Observable[ChunkSetReader] = {

    // results where query time range spans both data from cassandra and recently ingested data
    val resultsFromMemory = Observable.fromIterator(readersFromMemory(method, columnIds))

    val colStoreReaders = readFromColStore()

    // store the chunks into memory (happens asynchronously, not on the query path)
    colStoreReaders.flatMap { onHeapReaders =>
      pagedChunkStore.storeAsync(onHeapReaders, this)
    }.map { onHeapInfos =>
      partitionLoadedFromPersistentStore.set(true)
      shardStats.partitionsPagedFromColStore.increment()
      shardStats.chunkIdsPagedFromColStore.increment(onHeapInfos.size)
      logger.trace(s"Successfully paged ${onHeapInfos.size} chunksets from cassandra for partition key $binPartition")
    }.onFailure { case e =>
      logger.warn("Error occurred when paging data into Memstore", e)
    }

    // now filter the data read from colStore per user query
    val resultsFromColStore = Observable.fromFuture(colStoreReaders).flatMap(Observable.fromIterable(_))
      .filter { r =>
        // filter chunks based on query
        method match {
          case AllChunkScan =>                          true
          case RowKeyChunkScan(from, to) =>             r.info.intersection(from.binRec, to.binRec).isDefined
          case LastSampleChunkScan =>                   false
              // TODO return last chunk only if no data is ingested.
              // For now assume data is ingested continuously, and the chunk in memory will suffice
        }
      }.map { r =>
        // get right columns
        val chunkset = getVectors(columnIds, r.vectors.map(_.asInstanceOf[BinaryVector[_]]))
        shardStats.numChunksQueried.increment(chunkset.length)
        new ChunkSetReader(r.info, emptySkips, chunkset)
      }
    resultsFromMemory ++ resultsFromColStore
  }

  private def readFromColStore() = {
    val colStoreScan = dataset.timestampColumn match {
      case Some(tsCol) =>
        // scan colStore since retention (ex, 72) hours ago until (not including) first key in memstore
        val from = BinaryRecord(dataset, Seq(System.currentTimeMillis() - pagedChunkStore.retentionMillis))
        val toL = infosChunks.values.asScala.headOption.flatMap { v =>
          if (v.info.firstKey.isEmpty) None else Some(v.info.firstKey.getLong(tsCol.id) - 1)
        }
        val to = BinaryRecord(dataset, Seq(toL.getOrElse(Long.MaxValue)))
        RowKeyChunkScan(from, to)
      case None =>
        AllChunkScan
    }
    chunkSource.scanPartitions(dataset, SinglePartitionScan(binPartition, shard))
      .take(1)
      .flatMap(_.streamReaders(colStoreScan, dataset.dataColumns.map(_.id).toArray)) // all chunks, all columns
      .toListL
      .runAsync
  }
}
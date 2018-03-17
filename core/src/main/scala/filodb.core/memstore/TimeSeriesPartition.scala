package filodb.core.memstore

import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentSkipListMap

import com.typesafe.config.Config
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
                          config: Config,
                          skipDemandPaging: Boolean,
                          pagedChunkStore: DemandPagedChunkStore,
                          val shardStats: TimeSeriesShardStats)
                         (implicit val queryScheduler: Scheduler) extends FiloPartition with StrictLogging {
  import ChunkSetInfo._
  import collection.JavaConverters._

  private val nullAppenders = UnsafeUtils.ZeroPointer.asInstanceOf[Array[RowReaderAppender]]
  private val nullChunks    = UnsafeUtils.ZeroPointer.asInstanceOf[Array[BinaryAppendableVector[_]]]


  /**
   * This is the ONE main data structure holding the chunks of a TSPartition.  It is appended to as chunks arrive
   * and are encoded.  The last or most recent item should be the write buffer chunks.
   *    Key(ChunkID) -> Value InfoChunks(ChunkSetInfo, Array[BinaryVector])
   * Thus it is sorted by increasing chunkID, yet addressable by chunkID, and concurrent.
   */
  private var infosChunks = new ConcurrentSkipListMap[ChunkID, InfoChunks].asScala

  /**
    * Ingested data goes into this appender. There is one appender for each column in the dataset.
    * Var mutates when buffers are switched for optimization. During switching of buffers
    * in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]], current var
    * value is assigned to flushingAppenders, and null appenders is assigned until more data arrives.
    */
  private var appenders = nullAppenders

  /**
    * This is essentially the chunks (binaryVectors) associated with 'appenders' member of this class. This var will
    * hold the incoming un-encoded data for the partition.
    * There is one element for each column of the dataset. All of them have the same chunkId.
    * Var mutates when buffers are switched for optimization
    * in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]],
    * and new chunk is added to the partition.
    */
  private var currentChunks = nullChunks

  /**
    * This var holds the next appender to be optimized and persisted.
    * Mutates when buffers are switched for optimization in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]].
    * There is one element for each column of the dataset.
    * At the beginning nothing is ready to be flushed, so set to null
    */
  private var flushingAppenders = nullAppenders

   /**
    * Holds the id of the chunks in flushingAppender that should be flushed next.
    * Mutates when buffers are switched for optimization in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]].
    *
    * Always increases since it is a timeuuid.
    * Initially Long.MinValue since no chunk is ready for flush in the beginning.
    */
  private var flushingChunkID = Long.MinValue

  /**
    * Number of columns in the dataset
    */
  private final val numColumns = dataset.dataColumns.length

  private val jvmStartTime = ManagementFactory.getRuntimeMXBean.getStartTime

  // TODO data retention has not been implemented as a feature yet.
  private val chunkRetentionMillis = config.getDuration("memstore.demand-paged-chunk-retention-period",
    TimeUnit.MILLISECONDS)

  private val partitionLoadedFromPersistentStore = new AtomicBoolean(skipDemandPaging)

  /**
   * Ingests a new row, adding it to currentChunks.
   * Note that it is the responsibility of flush() to ensure the right currentChunks is allocated.
   */
  def ingest(row: RowReader, offset: Long): Unit = {
    if (appenders == nullAppenders) initNewChunk()
    for { col <- 0 until numColumns optimized } {
      appenders(col).append(row)
    }
  }

  /**
   * Atomically switches the writeBuffers/appenders to a null one.  If and when we get more data, then
   * we will initialize the appenders to new ones.  This way dead partitions not getting more data will not
   * waste empty appenders.
   * The old writeBuffers/chunks becomes flushingAppenders.
   * In theory this may be called from another thread from ingest(), but then ingest() may continue to write
   * to the old flushingAppenders buffers until the concurrent ingest() finishes.
   * To guarantee no more writes happen when switchBuffers is called, have ingest() and switchBuffers() be
   * called from a single thread / single synchronous stream.
   */
  def switchBuffers(): Unit = if (latestChunkLen > 0) {
    flushingAppenders = appenders
    flushingChunkID = infosChunks.keys.last
    // Right after this all ingest() calls will check and potentially append to new chunks
    appenders = nullAppenders
    currentChunks = nullChunks
  }

  def notFlushing: Boolean =
    flushingAppenders == UnsafeUtils.ZeroPointer || flushingAppenders(0).appender.length == 0

  /**
   * Optimizes flushingChunks into smallest BinaryVectors, store in memory and produce a ChunkSet for persistence.
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
  def makeFlushChunks(blockHolder: BlockMemFactory): Option[ChunkSet] = {
    if (notFlushing) {
      None
    } else {
      blockHolder.startMetaSpan()
      // optimize and compact old chunks
      val frozenVectors = flushingAppenders.zipWithIndex.map { case (appender, i) =>
        //This assumption cannot break. We should ensure one vector can be written
        //to one block always atleast as per the current design. We want a flush group
        //to typically end in atmost 2 blocks.
        //TODO: Break up chunks longer than blockAllocationSize into smaller ones
        assert(blockHolder.blockAllocationSize() > appender.appender.frozenSize)
        val optimized = appender.appender.optimize(blockHolder)
        shardStats.encodedBytes.increment(optimized.numBytes)
        optimized
      }
      val numSamples = frozenVectors(0).length
      shardStats.numSamplesEncoded.increment(numSamples)
      shardStats.numChunksEncoded.increment(frozenVectors.length)

      // Create ChunkSetInfo
      val reader = new FastFiloRowReader(frozenVectors.asInstanceOf[Array[FiloVector[_]]])
      reader.setRowNo(0)
      val firstRowKey = dataset.rowKey(reader)
      reader.setRowNo(numSamples - 1)
      val lastRowKey = dataset.rowKey(reader)
      val chunkInfo = infosChunks(flushingChunkID).info.copy(numRows = numSamples,
                                                             firstKey = firstRowKey,
                                                             lastKey = lastRowKey)
      // replace write buffers / vectors with compacted, immutable chunks
      infosChunks.put(flushingChunkID, InfoChunks(chunkInfo, frozenVectors))

      // release older appenders back to pool.  Nothing at this point should reference the older appenders.
      bufferPool.release(flushingAppenders)
      flushingAppenders = nullAppenders

      blockHolder.endMetaSpan(TimeSeriesShard.writeMeta(_, partID, chunkInfo.id), TimeSeriesShard.BlockMetaAllocSize)

      Some(ChunkSet(chunkInfo, binPartition, Nil,
                    frozenVectors.zipWithIndex.map { case (vect, pos) => (pos, vect.toFiloBuffer) }))
    }
  }

  def numChunks: Int = infosChunks.size
  def latestChunkLen: Int = if (currentChunks != nullChunks) currentChunks(0).length else 0

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
        infosChunks.values.toIterator
      // To derive time range: r.startkey.getLong(0) -> r.endkey.getLong(0)
      case r: RowKeyChunkScan         =>
        infosChunks.values.toIterator.filter { ic =>
          ic.info.firstKey.isEmpty ||      // empty key = most recent chunk, get a free pass
          ic.info.intersection(r.startkey, r.endkey).isDefined
        }
      case LastSampleChunkScan        =>
        if (infosChunks.isEmpty) Iterator.empty else Iterator.single(infosChunks.values.last)
    }

    infosVects.map { case InfoChunks(info, vectArray) =>
      //scalastyle:off
      require(vectArray != null, "INCONSISTENCY! vectArray is null!")
      //scalastyle:on
      val realInfo = info match {
        // First chunk, fill in real chunk length.  TODO: also fill in current first & last key
        case i @ ChunkSetInfo(_, -1, _, _) => i.copy(numRows = latestChunkLen)
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
    val (newAppenders, newCurChunks) = bufferPool.obtain()
    appenders = newAppenders
    currentChunks = newCurChunks
    val newChunkId = timeUUID64
    val newInfo = ChunkSetInfo(newChunkId, -1, BinaryRecord.empty, BinaryRecord.empty)
    infosChunks.put(newChunkId, InfoChunks(newInfo, currentChunks.asInstanceOf[Array[BinaryVector[_]]]))
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
      if (timeSinceStart > chunkRetentionMillis) partitionLoadedFromPersistentStore.set(true)
      timeSinceStart > chunkRetentionMillis
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

  def removeChunksAt(id: ChunkID): Unit = {
    infosChunks.remove(id)
    shardStats.chunkIdsEvicted.increment()
  }

  def addChunkSet(info: ChunkSetInfo, bytes: Array[BinaryVector[_]]): Unit = {
    infosChunks.put(info.id, InfoChunks(info, bytes))
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
        val from = BinaryRecord(dataset, Seq(System.currentTimeMillis() - chunkRetentionMillis))
        val toL = infosChunks.values.headOption.flatMap { v =>
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
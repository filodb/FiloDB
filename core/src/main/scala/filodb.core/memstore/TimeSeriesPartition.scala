package filodb.core.memstore

import java.lang.management.ManagementFactory
import java.util.concurrent.ConcurrentSkipListMap

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.Observable
import scalaxy.loops._

import filodb.core.Iterators
import filodb.core.Types._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.query.ChunkSetReader
import filodb.core.store._
import filodb.memory.{BlockHolder, ReclaimListener}
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
class TimeSeriesPartition(val dataset: Dataset,
                          val binPartition: PartitionKey,
                          val shard: Int,
                          val chunkSource: ChunkSource,
                          bufferPool: WriteBufferPool,
                          val shardStats: TimeSeriesShardStats) extends FiloPartition with StrictLogging {
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
  // There are pending design decisions, so punting on the config
  private val dataRetentionTime = 3.days.toMillis

  private var partitionLoadedFromPersistentStore = false

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
  def makeFlushChunks(blockHolder: BlockHolder): Option[ChunkSet] = {
    if (flushingAppenders == UnsafeUtils.ZeroPointer || flushingAppenders(0).appender.length == 0) {
      None
    } else {
      blockHolder.startPartition()
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

      blockHolder.endPartition(new ReclaimListener {
        //It is very likely that a flushChunk ends only in one block. At worst in may end up in a couple.
        //So a blockGroup contains atmost 2. When any one Block in the flushGroup is evicted the flushChunk is removed.
        //So if and when a second block gets reclaimed this is a no-op
        override def onReclaim(): Unit = {
          infosChunks.remove(chunkInfo.id)
          shardStats.chunkIdsEvicted.increment()
        }
      })

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
  private def cacheIsWarm = {
    if (partitionLoadedFromPersistentStore) {
      true
    } else {
      val timeSinceStart = System.currentTimeMillis - jvmStartTime
      timeSinceStart > dataRetentionTime
    }
  }

  override def streamReaders(method: ChunkScanMethod, columnIds: Array[Int]): Observable[ChunkSetReader] = {
    if (cacheIsWarm) { // provide read-through functionality
      super.streamReaders(method, columnIds) // return data from memory
    } else {
      // fetch data from colStore
      Observable.fromTask(ingestChunksFromColStore()) flatMap { _ =>
        // then read memstore to return response for query
        super.streamReaders(method, columnIds)
      }
    }
  }

  override def readers(method: ChunkScanMethod, columnIds: Array[Int]): Iterator[ChunkSetReader] = {
    import Iterators._
    if (cacheIsWarm) { // provide read-through functionality
      readersFromMemory(method, columnIds) // return data from memory
    } else {
      // fetch data from colStore
      Observable.fromTask(ingestChunksFromColStore()).map { _ =>
        // then read memstore to return response for query
        readersFromMemory(method, columnIds)
      }.toIterator().flatten
    }
    // Runtime errors are thrown back to caller
  }

  /**
    * Helper function to read chunks from colStore and write into memStore.
    *
    * IMPORTANT NOTE: It is possible that multiple queries are causing concurrent loading of data from Cassandra
    * at the same time. We do not prevent parallel loads from cassandra since the ingestOptimizedChunks call
    * is idempotent. Keeps the implementation simple. Revisit later if this is really an issue.
    *
    */
  private def ingestChunksFromColStore(): Task[Unit] = {
    val readers = chunkSource.scanPartitions(dataset, SinglePartitionScan(binPartition, shard))
      .take(1)
      .flatMap(_.streamReaders(AllChunkScan, dataset.dataColumns.map(_.id).toArray)) // all chunks, all columns
    readers.map { r =>
      // TODO r.vectors is on-heap. It should be copied to the offheap store before adding to index.
      infosChunks.put(r.info.id, InfoChunks(r.info, r.vectors.map(_.asInstanceOf[BinaryVector[_]])))
    }.countL.map { count =>
      shardStats.partitionsPagedFromColStore.increment()
      shardStats.chunkIdsPagedFromColStore.increment(count)
      logger.trace(s"Successfully ingested $count rows from cassandra into Memstore for partition key $binPartition")
      partitionLoadedFromPersistentStore = true
    }
  }

}
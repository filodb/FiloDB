package filodb.core.memstore

import scalaxy.loops._

import filodb.core.Types._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.query.{ChunkIDPartitionChunkIndex, ChunkSetReader}
import filodb.core.store._
import filodb.memory.BlockHolder
import filodb.memory.format._

import kamon.Kamon
import org.jctools.maps.NonBlockingHashMapLong

object TimeSeriesPartition {
  val numChunksEncoded = Kamon.metrics.counter("memstore-chunks-encoded")
  val numSamplesEncoded = Kamon.metrics.counter("memstore-samples-encoded")
  val encodedBytes     = Kamon.metrics.counter("memstore-encoded-bytes-allocated")
}

/**
 * A MemStore Partition holding chunks of data for different columns (a schema) for time series use cases.
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
 * TODO: eliminate chunkIDs, that can be stored in the index
 */
class TimeSeriesPartition(val dataset: Dataset,
                          val binPartition: PartitionKey,
                          val shard: Int,
                          bufferPool: WriteBufferPool)
                         (implicit blockHolder: BlockHolder) extends FiloPartition {
  import ChunkSetInfo._
  import TimeSeriesPartition._

  /**
    * This is a map from chunkId to the Array of chunks(BinaryVector) corresponding to that chunkId.
    *
    * NOTE: private final compiles down to a field in bytecode, faster than method invocation
    */
  private final val vectors = new NonBlockingHashMapLong[Array[BinaryVector[_]]](32, false)

  /**
    * As new chunks are initialized in this partition, the ids are appended to this queue
    */
  private final val chunkIDs = new collection.mutable.Queue[ChunkID]

  /**
    * This is the index enabling queries to be done on the ingested data. Allows for
    * query by chunkId, rowKey etc. See [[filodb.core.memstore.TimeSeriesPartition#readers]]
    * for how this is done.
    *
    * This only holds immutable, finished chunks.
    *
    */
  private final val index = new ChunkIDPartitionChunkIndex(binPartition, dataset)

  // Set initial size to a fraction of the max chunk size, so that partitions with sparse amount of data
  // will not cause too much memory bloat.  GrowableVector allows vectors to grow, so this should be OK
  private val (initAppenders, initCurChunks) = bufferPool.obtain()

  /**
    * Ingested data goes into this appender. There is one appender for each column in the dataset.
    * Var mutates when buffers are switched for optimization. During switching of buffers
    * in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]], current var
    * value is assigned to flushingAppenders, and new appender that is added to the partition is assigned to this var.
    */
  private var appenders = initAppenders

  /**
    * This is essentially the chunks (binaryVectors) associated with 'appenders' member of this class. This var will
    * hold the incoming un-encoded data for the partition.
    * There is one element for each column of the dataset. All of them have the same chunkId.
    * Var mutates when buffers are switched for optimization
    * in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]],
    * and new chunk is added to the partition.
    */
  private var currentChunks = initCurChunks

  /**
    * This var holds the next appender to be optimized and persisted.
    * Mutates when buffers are switched for optimization in [[filodb.core.memstore.TimeSeriesPartition#switchBuffers]].
    * There is one element for each column of the dataset.
    * Initialized to ZeroPointer since at the beginning nothing is ready to be flushed
    */
  private var flushingAppenders = UnsafeUtils.ZeroPointer.asInstanceOf[Array[RowReaderAppender]]

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
  private final val numColumns = appenders.size

  // Not used for now
  // private final val partitionVectors = new Array[FiloVector[_]](dataset.partitionColumns.length)

  // Add initial write buffers as the first chunkSet/chunkID
  initNewChunk()

  /**
   * Ingests a new row, adding it to currentChunks.
   * Note that it is the responsibility of flush() to ensure the right currentChunks is allocated.
   */
  def ingest(row: RowReader, offset: Long): Unit = {
    for { col <- 0 until numColumns optimized } {
      appenders(col).append(row)
    }
  }

  /**
   * Atomically switches the writeBuffers/appenders to a new empty one.
   * The old writeBuffers/chunks becomes flushingAppenders.
   * In theory this may be called from another thread from ingest(), but then ingest() may continue to write
   * to the old flushingAppenders buffers until the concurrent ingest() finishes.
   * To guarantee no more writes happen when switchBuffers is called, have ingest() and switchBuffers() be
   * called from a single thread / single synchronous stream.
   */
  def switchBuffers(): Unit = if (currentChunks(0).length > 0) {
    // Get new write buffers from pool
    flushingAppenders = appenders
    flushingChunkID = chunkIDs.last
    val newAppendersAndChunks = bufferPool.obtain()
    // Right after this all ingest() calls will append to new chunks
    appenders = newAppendersAndChunks._1
    currentChunks = newAppendersAndChunks._2
    initNewChunk()   // At this point the new buffers can be read from
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
  def makeFlushChunks(): Option[ChunkSet] = {
    if (flushingAppenders == UnsafeUtils.ZeroPointer || flushingAppenders(0).appender.length == 0) {
      None
    } else {
      // optimize and compact old chunks
      val frozenVectors = flushingAppenders.zipWithIndex.map { case (appender, i) =>
        val optimized = appender.appender.optimize(blockHolder)
        encodedBytes.increment(optimized.numBytes)
        optimized
      }
      val numSamples = frozenVectors(0).length
      numSamplesEncoded.increment(numSamples)
      numChunksEncoded.increment(frozenVectors.length)

      // replace appendableVectors reference in vectors hash with compacted, immutable chunks
      vectors.put(flushingChunkID, frozenVectors)

      // release older appenders back to pool.  Nothing at this point should reference the older appenders.
      bufferPool.release(flushingAppenders)
      flushingAppenders = UnsafeUtils.ZeroPointer.asInstanceOf[Array[RowReaderAppender]]

      // Create ChunkSetInfo
      val reader = new FastFiloRowReader(frozenVectors.asInstanceOf[Array[FiloVector[_]]])
      reader.setRowNo(0)
      val firstRowKey = dataset.rowKey(reader)
      reader.setRowNo(numSamples - 1)
      val lastRowKey = dataset.rowKey(reader)
      val chunkInfo = ChunkSetInfo(flushingChunkID, numSamples, firstRowKey, lastRowKey)
      index.add(chunkInfo, Nil)

      Some(ChunkSet(chunkInfo, binPartition, Nil,
                    frozenVectors.zipWithIndex.map { case (vect, pos) => (pos, vect.toFiloBuffer) }))
    }
  }

  def latestN(n: Int): InfosSkipsIt =
    if (latestChunkLen > 0) { latestChunkIt ++ index.latestN(n - 1) }
    else                    { index.latestN(n) }

  def numChunks: Int = chunkIDs.size
  def latestChunkLen: Int = currentChunks(0).length

  /**
   * Gets the most recent n ChunkSetInfos and skipMaps (which will be empty)
   */
  def newestChunkIds(n: Int): InfosSkipsIt = {
    val latest = latestChunkIt
    val numToTake = if (latest.isEmpty) n else (n - 1)
    index.latestN(numToTake) ++ latest
  }

  private def latestChunkInfo: ChunkSetInfo =
    ChunkSetInfo(chunkIDs.last, latestChunkLen, BinaryRecord.empty, BinaryRecord.empty)

  private def latestChunkIt: InfosSkipsIt = Iterator.single((latestChunkInfo, emptySkips))

  private def getVectors(columnIds: Array[Int],
                         vectors: Array[BinaryVector[_]],
                         vectLength: Int): Array[FiloVector[_]] = {
    val finalVectors = new Array[FiloVector[_]](columnIds.size)
    for { i <- 0 until columnIds.size optimized } {
      finalVectors(i) = if (Dataset.isPartitionID(columnIds(i))) { constPartitionVector(columnIds(i)) }
                        else                                     { vectors(columnIds(i)) }
    }
    finalVectors
  }

  def readers(method: ChunkScanMethod, columnIds: Array[Int]): Iterator[ChunkSetReader] = {
    val infosSkips = method match {
      case AllChunkScan               => index.allChunks ++ latestChunkIt
      // To derive time range: r.startkey.getLong(0) -> r.endkey.getLong(0)
      case r: RowKeyChunkScan         => index.rowKeyRange(r.startkey, r.endkey) ++ latestChunkIt
      case r @ SingleChunkScan(_, id) => index.singleChunk(r.startkey, id)
      case LastSampleChunkScan        => latestN(1)
    }

    infosSkips.map { case (info, skips) =>
      val vectArray = vectors.get(info.id)
      new ChunkSetReader(info, binPartition, skips, getVectors(columnIds, vectArray, info.numRows))
    }
  }

  def lastVectors: Array[FiloVector[_]] = currentChunks.asInstanceOf[Array[FiloVector[_]]]

  /**
    * Initializes vectors, chunkIDs for a new chunkset/chunkID.
    * This is called once every chunk-duration for each group, when the buffers are switched for that group
    */
  private def initNewChunk(): Unit = {
    val newChunkID = timeUUID64
    vectors.put(newChunkID, currentChunks.asInstanceOf[Array[BinaryVector[_]]])
    chunkIDs += newChunkID
  }
}
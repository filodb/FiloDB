package filodb.core.memstore

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
import filodb.memory.{BinaryRegion, BlockMemFactory}
import filodb.memory.format._

// Have one class (ideally a value class) that can represent both ChunkSetInfo as well as point at the chunks
// to save heap memory.  We have many millions of these objects.
trait InfoChunks extends ChunkSetInfo {
  def chunks: Array[BinaryVector[_]]
}

object TimeSeriesPartition {
  val nullChunks    = UnsafeUtils.ZeroPointer.asInstanceOf[Array[BinaryAppendableVector[_]]]
  val nullInfo      = UnsafeUtils.ZeroPointer.asInstanceOf[MutableInfoChunks]
}

// Only used for WriteBuffers and chunks before they are flushed
final case class MutableInfoChunks(id: ChunkID, var numRows: Int, var startTime: Long, var endTime: Long)
extends InfoChunks {
  var chunks = TimeSeriesPartition.nullChunks.asInstanceOf[Array[BinaryVector[_]]]
}

// Used to store chunkset meta after flushing (and writing to offheap block memory)
final case class OffheapInfoChunks(infoAddr: Long, chunks: Array[BinaryVector[_]]) extends InfoChunks {
  def id: ChunkID = UnsafeUtils.getLong(infoAddr + 4)
  def numRows: Int = UnsafeUtils.getInt(infoAddr + 12)
  def startTime: Long = UnsafeUtils.getLong(infoAddr + 16)
  def endTime: Long = UnsafeUtils.getLong(infoAddr + 24)
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
                          partitionKey: BinaryRegion.NativePointer,
                          val shard: Int,
                          val chunkSource: ChunkSource,
                          bufferPool: WriteBufferPool,
                          pagedChunkStore: DemandPagedChunkStore,
                          val shardStats: TimeSeriesShardStats)
                         (implicit val queryScheduler: Scheduler) extends FiloPartition with StrictLogging {
  import ChunkSetInfo._
  import collection.JavaConverters._
  import TimeSeriesPartition._

  def partKeyBase: Array[Byte] = UnsafeUtils.ZeroPointer.asInstanceOf[Array[Byte]]
  def partKeyOffset: Long = partitionKey

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
  private var currentInfo = nullInfo

  /**
   * The newest ChunkID that has been flushed or encoded.  You can think of the progression of chunks like this,
   * from newest to oldest (thus represents a traversal of infosChunks):
   * current -> notEncoded -> encodedNotFlushed -> Flushed -> Flushed -> (eventually) reclaimed
   *
   * During flush we ensure any unencoded chunks that are not current (writing) are encoded and updates these IDs.
   */
  private var newestFlushedID = Long.MinValue

  /**
    * Number of columns in the dataset
    */
  private final val numColumns = dataset.dataColumns.length

  /**
   * Ingests a new row, adding it to currentChunks.
   * If ingesting a new row causes WriteBuffers to overflow, then the current chunks are encoded, a new set
   * of appending chunks are obtained, and we re-ingest into the new chunks.
   *
   * @param blockHolder the BlockMemFactory to use for encoding chunks in case of WriteBuffer overflow
   */
  final def ingest(row: RowReader, blockHolder: BlockMemFactory): Unit = {
    if (currentChunks == nullChunks) {
      // First row of a chunk, set the start time to it
      initNewChunk().startTime = dataset.timestamp(row)
    }
    for { col <- 0 until numColumns optimized } {
      currentChunks(col).addFromReaderNoNA(row, col) match {
        case r: VectorTooSmall =>
          switchBuffers(blockHolder, encode=true)
          ingest(row, blockHolder)   // re-ingest every element, allocating new WriteBuffers
          return
        case other: AddResponse =>
      }
    }
    currentInfo.numRows += 1
  }

  private def nonEmptyWriteBuffers: Boolean = currentInfo != UnsafeUtils.ZeroPointer && currentInfo.numRows > 0

  /**
   * Atomically switches the writeBuffers to a null one.  If and when we get more data, then
   * we will initialize the writeBuffers to new ones.  This way dead partitions not getting more data will not
   * waste empty appenders.
   * Also populates a complete ChunkSetInfo so that these chunks may be queried reliably.
   * To guarantee no more writes happen when switchBuffers is called, have ingest() and switchBuffers() be
   * called from a single thread / single synchronous stream.
   */
  final def switchBuffers(blockHolder: BlockMemFactory, encode: Boolean = false): Unit =
    if (nonEmptyWriteBuffers) {
      // Update ChunkSetInfo of current buffers.
      // TODO: update this info dynamically, and get rid of the exceptions around "latest chunk"?
      val reader = new FastFiloRowReader(currentChunks.asInstanceOf[Array[FiloVector[_]]])
      reader.setRowNo(currentInfo.numRows - 1)
      currentInfo.endTime = dataset.timestamp(reader)
      val currentId = currentInfo.id

      // Right after this all ingest() calls will check and potentially append to new chunks
      // We can reset currentInfo because it is already stored in infosChunks map
      currentChunks = nullChunks
      currentInfo = nullInfo

      if (encode) encodeOneChunkset(currentId, blockHolder)
    }

  /**
   * Optimizes a set of chunks into the smallest BinaryVectors and updates index structure.  May be called concurrently.
   * Optimized chunks as well as chunk metadata are written into offheap block memory so they no longer consume
   */
  private def encodeOneChunkset(id: ChunkID, blockHolder: BlockMemFactory) = {
    val infoChunks = infosChunks.get(id).asInstanceOf[MutableInfoChunks]
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
    shardStats.numSamplesEncoded.increment(infoChunks.numRows)
    shardStats.numChunksEncoded.increment(frozenVectors.length)

    // Now, write metadata into offheap block metadata space and update infosChunks
    val metaAddr = blockHolder.endMetaSpan(TimeSeriesShard.writeMeta(_, partID, infoChunks),
                                           TimeSeriesShard.BlockMetaAllocSize)
    infosChunks.put(id, OffheapInfoChunks(metaAddr, frozenVectors))

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
      .map { case i: InfoChunks =>
        ChunkSet(i, partitionKey, Nil,
                 i.chunks.zipWithIndex.map { case (vect, pos) => (pos, vect.toFiloBuffer) },
                 // Updates the newestFlushedID when the flush succeeds.
                 // NOTE: by using a method instead of closure, we allocate less
                 updateFlushedID)
      }
  }

  // Encodes and releases any remaining WriteBuffers back to the pool
  def encodeAndReleaseBuffers(blockHolder: BlockMemFactory): Unit =
    infosChunksToBeFlushed
      .foreach { case i: InfoChunks =>
        i.chunks(0) match {
          case v: BinaryAppendableVector[_] => encodeOneChunkset(i.id, blockHolder)
          case other: BinaryVector[_]       =>
        }
      }

  def numChunks: Int = infosChunks.size
  def appendingChunkLen: Int = if (currentInfo != UnsafeUtils.ZeroPointer) currentInfo.numRows else 0

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
               .filterNot(_ == currentInfo)  // filter out the appending chunk

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
          ic == currentInfo ||      // most recent chunk gets a free pass, cannot compare range
          ic.intersection(r.startTime, r.endTime).isDefined
        }
      case LastSampleChunkScan        =>
        if (infosChunks.isEmpty) Iterator.empty else Iterator.single(infosChunks.get(infosChunks.lastKey))
    }

    infosVects.map { case i: InfoChunks =>
      //scalastyle:off
      require(i.chunks != null, "INCONSISTENCY! vectArray is null!")
      //scalastyle:on
      val chunkset = getVectors(columnIds, i.chunks)
      shardStats.numChunksQueried.increment(chunkset.length)
      new ChunkSetReader(i, emptySkips, chunkset)
    }
  }

  def lastVectors: Array[FiloVector[_]] = currentChunks.asInstanceOf[Array[FiloVector[_]]]

  /**
    * Initializes vectors, chunkIDs for a new chunkset/chunkID.
    * This is called after switchBuffers() upon the first data that arrives.
    * Also compacts infosChunks tombstones.
    */
  private def initNewChunk(): MutableInfoChunks = {
    currentChunks = bufferPool.obtain()
    val currentChunkID = timeUUID64
    currentInfo = MutableInfoChunks(currentChunkID, 0, Long.MinValue, Long.MaxValue)
    currentInfo.chunks = currentChunks.asInstanceOf[Array[BinaryVector[_]]]
    infosChunks.put(currentChunkID, currentInfo)
    currentInfo
  }

  /**
   * Returns the method to use to read from the ChunkSource
   * if the chunks in memory are not enough to satisfy the request.
   * NOTE: Just a temporary implementation.  To really answer this question, we need to know, from the index,
   * how much data there really is outside of memory, instead of guessing.
   * This is just an initial stab.
   * Also, this code should really live outside of TSPartition in its own loader.
   */
  private def chunkStoreScanMethod(method: ChunkScanMethod): Option[ChunkScanMethod] =
    if (pagedChunkStore.onDemandPagingEnabled) {
      method match {
        // For now, allChunkScan will always load from disk.  This is almost never used, and without an index we have
        // no way of knowing what there is anyways.
        case AllChunkScan               => Some(AllChunkScan)
        // Assume initial startKey of first chunk is the earliest - typically true unless we load in historical data
        // Compare desired time range with start key and see if in memory data covers desired range
        // Also assume we have in memory all data since first key.  Just return the missing range of keys.
        case r: RowKeyChunkScan         =>
          if (infosChunks.size > 0) {
            val memStartTime = infosChunks.firstEntry.getValue.startTime
            val firstInMemKey = BinaryRecord.timestamp(memStartTime)
            if (r.startTime < memStartTime) { Some(RowKeyChunkScan(r.startkey, firstInMemKey)) }
            else                            { None }
          } else {
            Some(r)    // if no chunks ingested yet, read everything from disk
          }
        // The last sample is almost always in memory.  But this is only used for old QueryEngine.
        case LastSampleChunkScan        => None
      }
    } else {
      None
    }

  override def streamReaders(method: ChunkScanMethod, columnIds: Array[Int]): Observable[ChunkSetReader] = {
    chunkStoreScanMethod(method).map { m =>
      demandPageFromColStore(m, columnIds)
    }.getOrElse {
      super.streamReaders(method, columnIds) // return data from memory
    }
  }

  override def readers(method: ChunkScanMethod, columnIds: Array[Int]): Iterator[ChunkSetReader] = {
    import Iterators._
    chunkStoreScanMethod(method).map { m =>
      demandPageFromColStore(m, columnIds).toIterator()
    }.getOrElse {
      readersFromMemory(method, columnIds) // return data from memory
    }
  }

  final def removeChunksAt(id: ChunkID): Unit = {
    infosChunks.remove(id)
    shardStats.chunkIdsEvicted.increment()
  }

  final def hasChunksAt(id: ChunkID): Boolean = infosChunks.containsKey(id)

  // Used for adding chunksets that are paged in, ie that are already persisted
  final def addChunkSet(metaAddr: Long, chunks: Array[BinaryVector[_]]): Unit = {
    val newInfo = OffheapInfoChunks(metaAddr, chunks)
    infosChunks.put(newInfo.id, newInfo)
    // Make sure to update newestFlushedID so that flushes work correctly and don't try to flush these chunksets
    updateFlushedID(newInfo)
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

    val colStoreReaders = readFromColStore(method)

    // store the chunks into memory (happens asynchronously, not on the query path)
    val readChunkIds = new collection.mutable.HashSet[ChunkID]
    colStoreReaders.flatMap { onHeapReaders =>
      pagedChunkStore.storeAsync(onHeapReaders, this)
    }.map { onHeapInfos =>
      shardStats.partitionsPagedFromColStore.increment()
      shardStats.chunkIdsPagedFromColStore.increment(onHeapInfos.size)
      logger.trace(s"Successfully paged ${onHeapInfos.size} chunksets from cassandra for partition $stringPartition")
    }.onFailure { case e =>
      logger.warn("Error occurred when paging data into Memstore", e)
    }

    val resultsFromColStore = Observable.fromFuture(colStoreReaders).flatMap(Observable.fromIterable(_))
      .map { r =>
        // get right columns
        val chunkset = getVectors(columnIds, r.vectors.map(_.asInstanceOf[BinaryVector[_]]))
        shardStats.numChunksQueried.increment(chunkset.length)
        readChunkIds += r.info.id
        new ChunkSetReader(r.info, emptySkips, chunkset)
      }

    // In theory, we are trying to read non-overlapping ranges from colStore and from memory.  IN reality
    // we need to filter because AllChunksScan of both will return overlapping results.  :(
    resultsFromColStore ++ resultsFromMemory.filter { r => !readChunkIds.contains(r.info.id) }
  }

  private def readFromColStore(method: ChunkScanMethod) =
    chunkSource.readChunks(dataset, dataset.dataColumns.map(_.id),
                           SinglePartitionScan(partKeyBytes, shard),
                           method)
               .toListL
               .runAsync
}
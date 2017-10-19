package filodb.core.memstore

import scalaxy.loops._

import filodb.core.Types._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.Dataset
import filodb.core.query.{ChunkIDPartitionChunkIndex, ChunkSetReader}
import filodb.core.store._
import filodb.memory.BlockHolder
import filodb.memory.format.{FiloVector, RowReader}

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
 * Concurrency: single writer for ingest, multiple readers OK for reads
 *
 * TODO: eliminate chunkIDs, that can be stored in the index
 * TODO: document tradeoffs between chunksToKeep, maxChunkSize
 * TODO: update flushedWatermark when flush is confirmed
 */
class TimeSeriesPartition(val dataset: Dataset,
                          val binPartition: PartitionKey,
                          val shard: Int,
                          chunksToKeep: Int,
                          maxChunkSize: Int)
                         (implicit blockHolder: BlockHolder) extends FiloPartition {
  import ChunkSetInfo._
  import TimeSeriesPartition._

  // NOTE: private final compiles down to a field in bytecode, faster than method invocation
  private final val vectors = new NonBlockingHashMapLong[Array[FiloVector[_]]](32, false)
  private final val chunkIDs = new collection.mutable.Queue[ChunkID]
  // This only holds immutable, finished chunks
  private final val index = new ChunkIDPartitionChunkIndex(binPartition, dataset)
  private var currentChunkLen = 0
  private var firstRowKey = BinaryRecord.empty

  // Set initial size to a fraction of the max chunk size, so that partitions with sparse amount of data
  // will not cause too much memory bloat.  GrowableVector allows vectors to grow, so this should be OK
  private final val appenders = MemStore.getAppendables(dataset, maxChunkSize / 8)
  private final val numColumns = appenders.size

  private final val partitionVectors = new Array[FiloVector[_]](dataset.partitionColumns.length)
  private final val currentChunks = appenders.map(_.appender)

  // The highest offset of a row that has been committed to disk
  var flushedWatermark = Long.MinValue
  // The highest offset ingested, but probably not committed
  var ingestedWatermark = Long.MinValue

  /**
   * Ingests a new row, adding it to currentChunks IF the offset is greater than the flushedWatermark.
   * The condition ensures data already persisted will not be encoded again.
   */
  def ingest(row: RowReader, offset: Long): Unit = {
    if (offset > flushedWatermark) {
      // Don't create new chunkID until new data appears
      if (currentChunkLen == 0) {
        initNewChunk()
        firstRowKey = dataset.rowKey(row)
      }
      for { col <- 0 until numColumns optimized } {
        appenders(col).append(row)
      }
      currentChunkLen += 1
      if (ingestedWatermark < offset) ingestedWatermark = offset
      if (currentChunkLen >= maxChunkSize) flush(dataset.rowKey(row))
    }
  }

  /**
   * Compacts currentChunks and flushes to disk.  When the flush is complete, update the watermark.
   * Gets ready for ingesting a new set of chunks. Keeps chunks in memory until they are aged out.
   * TODO: for partitions getting very little data, in the future, instead of creating a new set of chunks,
   * we might wish to flush current chunks as they are for persistence but then keep adding to the partially
   * filled currentChunks.  That involves much more state, so do much later.
   */
  protected def flush(lastRowKey: BinaryRecord): Unit = {
    // optimize and compact current chunks
    val frozenVectors = currentChunks.zipWithIndex.map { case (curChunk,i) =>
      val optimized = curChunk.optimize()
      val block = blockHolder.requestBlock(optimized.numBytes)
      block.own()
      val (pos, size) = block.write(optimized)
      val vector = dataset.vectorMakers(i)(block.read(pos, size), size)
      optimized.dispose()

      encodedBytes.increment(optimized.numBytes)
      curChunk.reset()
      vector
    }
    numSamplesEncoded.increment(currentChunkLen)
    numChunksEncoded.increment(frozenVectors.length)

    val curChunkID = chunkIDs.last
    val chunkInfo = ChunkSetInfo(curChunkID, currentChunkLen, firstRowKey, lastRowKey)
    index.add(chunkInfo, Nil)
    // TODO: push new chunks to ColumnStore

    // replace appendableVectors reference in vectors hash with compacted, immutable chunks
    vectors.put(curChunkID, frozenVectors)

    // Finally, mark current chunk len as 0 so we know to create a new chunkID on next row
    currentChunkLen = 0
  }

  def latestN(n: Int): InfosSkipsIt =
    if (n == 1) latestChunkIt else index.latestN(n)

  def numChunks: Int = chunkIDs.size
  def latestChunkLen: Int = currentChunkLen

  /**
   * Gets the most recent n ChunkSetInfos and skipMaps (which will be empty)
   */
  def newestChunkIds(n: Int): InfosSkipsIt = {
    val latest = latestChunkIt
    val numToTake = if (latest.isEmpty) n else (n - 1)
    index.latestN(numToTake) ++ latest
  }

  // Only returns valid results if there is a current chunk being appended to
  private def latestChunkInfo: ChunkSetInfo =
    ChunkSetInfo(chunkIDs.last, currentChunkLen, firstRowKey, BinaryRecord.empty)

  private def latestChunkIt: InfosSkipsIt =
    if (currentChunkLen > 0) { Iterator.single((latestChunkInfo, emptySkips)) }
    else                     { Iterator.empty }


  private def getVectors(columnIds: Array[Int],
                         vectors: Array[FiloVector[_]],
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

  def lastVectors: Array[FiloVector[_]] =
    (if (currentChunkLen == 0 && chunkIDs.nonEmpty) {
      vectors.get(chunkIDs.last)
    } else { currentChunks }).asInstanceOf[Array[FiloVector[_]]]

  // Initializes vectors, chunkIDs for a new chunkset/chunkID
  private def initNewChunk(): Unit = {
    val newChunkID = timeUUID64
    vectors.put(newChunkID, currentChunks.asInstanceOf[Array[FiloVector[_]]])
    chunkIDs += newChunkID

    // check number of chunks, flush out old chunk if needed
    if (chunkIDs.length > chunksToKeep) {
      val oldestID = chunkIDs.dequeue // how to remove head element?
      vectors.remove(oldestID)
      index.remove(oldestID)
    }
  }
}
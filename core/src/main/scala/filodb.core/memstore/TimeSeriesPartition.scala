package filodb.core.memstore

import kamon.Kamon
import org.jctools.maps.NonBlockingHashMapLong
import org.velvia.filo.{BinaryAppendableVector, BinaryVector, FiloVector, RowReader, RoutingRowReader}
import scalaxy.loops._

import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.RichProjection
import filodb.core.query.{PartitionChunkIndex, ChunkIDPartitionChunkIndex, ChunkSetReader}
import filodb.core.store._
import filodb.core.Types._

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
 * This also implements PartitionChunkIndex, and the various chunk indexing methods include returning the
 * very latest data.  However the ChunkSetInfo for the latest chunk still being appended will not have
 * a correct set of starting and ending keys (since they are still changing).
 *
 * TODO: eliminate chunkIDs, that can be stored in the index
 * TODO: document tradeoffs between chunksToKeep, maxChunkSize
 * TODO: update flushedWatermark when flush is confirmed
 */
class TimeSeriesPartition(val projection: RichProjection,
                          val binPartition: PartitionKey,
                          val shard: Int,
                          chunksToKeep: Int,
                          maxChunkSize: Int) extends FiloPartition {
  import ChunkSetInfo._
  import TimeSeriesPartition._

  // NOTE: private final compiles down to a field in bytecode, faster than method invocation
  private final val vectors = new NonBlockingHashMapLong[Array[BinaryVector[_]]](32, false)
  private final val chunkIDs = new collection.mutable.Queue[ChunkID]
  // This only holds immutable, finished chunks
  private final val index = new ChunkIDPartitionChunkIndex(binPartition, projection)
  private var currentChunkLen = 0
  private var firstRowKey = BinaryRecord.empty

  // Set initial size to a fraction of the max chunk size, so that partitions with sparse amount of data
  // will not cause too much memory bloat.  GrowableVector allows vectors to grow, so this should be OK
  private final val appenders = MemStore.getAppendables(projection, binPartition, maxChunkSize / 8)
  private final val numColumns = appenders.size
  private final val partitionVectors = new Array[FiloVector[_]](projection.partitionColumns.length)
  private final val currentChunks = appenders.map(_.appender)
  // TODO(velvia): These are not correct, needs to be adjusted for non-partition columns
  private final val rowKeyIndices = projection.rowKeyColIndices.toArray

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
        firstRowKey = makeRowKey(row)
      }
      for { col <- 0 until numColumns optimized } {
        appenders(col).append(row)
      }
      currentChunkLen += 1
      if (ingestedWatermark < offset) ingestedWatermark = offset
      if (currentChunkLen >= maxChunkSize) flush(makeRowKey(row))
    }
  }

  /**
   * Compacts currentChunks and flushes to disk.  When the flush is complete, update the watermark.
   * Gets ready for ingesting a new set of chunks. Keeps chunks in memory until they are aged out.
   * TODO: for partitions getting very little data, in the future, instead of creating a new set of chunks,
   * we might wish to flush current chunks as they are for persistence but then keep adding to the partially
   * filled currentChunks.  That involves much more state, so do much later.
   */
  def flush(lastRowKey: BinaryRecord): Unit = {
    // optimize and compact current chunks
    val frozenVectors = currentChunks.map { curChunk =>
      val optimized = curChunk.optimize()
      encodedBytes.increment(optimized.numBytes)
      curChunk.reset()
      optimized
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

  private def makeRowKey(row: RowReader): BinaryRecord =
    BinaryRecord(projection.rowKeyBinSchema, RoutingRowReader(row, rowKeyIndices))

  private def getVectors(positions: Array[Int],
                         vectors: Array[BinaryVector[_]],
                         vectLength: Int): Array[FiloVector[_]] = {
    val finalVectors = new Array[FiloVector[_]](positions.size)
    for { i <- 0 until positions.size optimized } {
      finalVectors(i) = if (positions(i) >= 0) { vectors(positions(i)) }
                        else {
                          // Create const vectors for partition key columns on demand, since they are
                          // rarely queried.  Cache them in partitionVectors.
                          // scalastyle:off null
                          val partVectPos = -positions(i) - 1
                          val partVect = partitionVectors(partVectPos)
                          if (partVect == null) {
                            val constVect = projection.partExtractors(partVectPos).
                                              constVector(binPartition, partVectPos, vectLength)
                            partitionVectors(partVectPos) = constVect
                            constVect
                          } else { partVect }
                          // scalastyle:on null
                        }
    }
    finalVectors
  }

  /**
   * Streams back ChunkSetReaders from this Partition as an iterator of readers by chunkID
   * @param method the ChunkScanMethod determining which chunks to read out
   * @param positions an array of the column positions according to projection.dataColumns, ie 0 for the first
   *                  column, up to projection.dataColumns.length - 1
   */
  def readers(method: ChunkScanMethod, positions: Array[Int]): Iterator[ChunkSetReader] = {
    val infosSkips = method match {
      case AllChunkScan               => index.allChunks ++ latestChunkIt
      // To derive time range: r.startkey.getLong(0) -> r.endkey.getLong(0)
      case r: RowKeyChunkScan         => index.rowKeyRange(r.startkey, r.endkey) ++ latestChunkIt
      case r @ SingleChunkScan(_, id) => index.singleChunk(r.startkey, id)
      case LastSampleChunkScan        => latestN(1)
    }

    infosSkips.map { case (info, skips) =>
      val vectArray = vectors.get(info.id)
      new ChunkSetReader(info, binPartition, skips, getVectors(positions, vectArray, info.numRows))
    }
  }

  def lastVectors: Array[FiloVector[_]] =
    (if (currentChunkLen == 0 && chunkIDs.nonEmpty) {
      vectors.get(chunkIDs.last)
    } else { currentChunks }).asInstanceOf[Array[FiloVector[_]]]

  // Initializes vectors, chunkIDs for a new chunkset/chunkID
  private def initNewChunk(): Unit = {
    val newChunkID = timeUUID64
    vectors.put(newChunkID, currentChunks.asInstanceOf[Array[BinaryVector[_]]])
    chunkIDs += newChunkID

    // check number of chunks, flush out old chunk if needed
    if (chunkIDs.length > chunksToKeep) {
      val oldestID = chunkIDs.dequeue // how to remove head element?
      vectors.remove(oldestID)
      index.remove(oldestID)
    }
  }
}
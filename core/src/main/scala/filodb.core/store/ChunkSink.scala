package filodb.core.store

import scala.concurrent.Future

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core._
import filodb.core.metadata.Dataset


/**
 * ChunkSink is the base trait for a sink, or writer to a persistent store, of chunks
 */
trait ChunkSink {
  def sinkStats: ChunkSinkStats

  /**
   * Writes the ChunkSets appearing in a stream/Observable to persistent storage, with backpressure
   * @param dataset the Dataset to write to
   * @param chunksets an Observable stream of chunksets to write
   * @return Success when the chunksets stream ends and is completely written.
   *         Future.failure(exception) if an exception occurs.
   */
  def write(dataset: Dataset, chunksets: Observable[ChunkSet]): Future[Response]

  /**
   * Initializes the ChunkSink for a given dataset.  Must be called once before writing.
   */
  def initialize(dataset: DatasetRef): Future[Response]

  /**
   * Truncates/clears all data from the ChunkSink for that given dataset.
   * NOTE: please make sure there are no writes going on before calling this
   */
  def truncate(dataset: DatasetRef): Future[Response]

  /**
   * Completely and permanently drops the dataset from the ChunkSink.
   * @param dataset the DatasetRef for the dataset to drop.
   */
  def dropDataset(dataset: DatasetRef): Future[Response]

  /** Resets state, whatever that means for the sink */
  def reset(): Unit
}

/**
 * Stats for a ChunkSink
 */
class ChunkSinkStats {
  private val numChunkWriteCalls = Kamon.metrics.counter("chunk-write-calls-num")
  private val chunksPerCallHist  = Kamon.metrics.histogram("chunks-per-call")
  private val chunkBytesHist     = Kamon.metrics.histogram("chunk-bytes-per-call")

  private val numIndexWriteCalls = Kamon.metrics.counter("index-write-calls-num")
  private val indexBytesHist     = Kamon.metrics.histogram("index-bytes-per-call")

  private val chunksetWrites     = Kamon.metrics.counter("chunkset-writes")
  var chunksetsWritten = 0

  def addChunkWriteStats(numChunks: Int, totalChunkBytes: Long): Unit = {
    numChunkWriteCalls.increment
    chunksPerCallHist.record(numChunks)
    chunkBytesHist.record(totalChunkBytes)
  }

  def addIndexWriteStats(totalIndexBytes: Long): Unit = {
    numIndexWriteCalls.increment
    indexBytesHist.record(totalIndexBytes)
  }

  def chunksetWrite(): Unit = {
    chunksetWrites.increment
    chunksetsWritten += 1
  }
}

/**
 * NullChunkSink keeps stats but other than that writes chunks nowhere.  It's convenient for testing though.
 */
class NullColumnStore(implicit sched: Scheduler) extends ColumnStore with StrictLogging {
  val sinkStats = new ChunkSinkStats
  val stats = new ChunkSourceStats

  def write(dataset: Dataset, chunksets: Observable[ChunkSet]): Future[Response] = {
    chunksets.foreach { chunkset =>
      val totalBytes = chunkset.chunks.map(_._2.limit).sum
      sinkStats.addChunkWriteStats(chunkset.chunks.length, totalBytes)
      sinkStats.chunksetWrite()
      logger.debug(s"NullChunkSink: [${chunkset.partition}] ${chunkset.info}  ${chunkset.chunks.length} " +
                   s"chunks with $totalBytes bytes")
    }
    Future.successful(Success)
  }

  def initialize(dataset: DatasetRef): Future[Response] = Future.successful(Success)

  def truncate(dataset: DatasetRef): Future[Response] = Future.successful(Success)

  def dropDataset(dataset: DatasetRef): Future[Response] = Future.successful(Success)

  def reset(): Unit = {}

  override def shutdown(): Unit = {}

  override def scanPartitions(dataset: Dataset,
                              partMethod: PartitionScanMethod): Observable[FiloPartition] = Observable.empty

  override def getScanSplits(dataset: DatasetRef, splitsPerNode: Int): Seq[ScanSplit] = Seq.empty
}

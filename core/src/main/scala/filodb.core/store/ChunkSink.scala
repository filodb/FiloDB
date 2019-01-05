package filodb.core.store

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
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
   * @param diskTimeToLive the time for chunksets to live on disk (Cassandra)
   * @return Success when the chunksets stream ends and is completely written.
   *         Future.failure(exception) if an exception occurs.
   */
  def write(dataset: Dataset, chunksets: Observable[ChunkSet], diskTimeToLive: Int = 259200): Future[Response]

  def writePartKeyTimeBucket(dataset: Dataset,
                             shardNum: Int,
                             timeBucket: Int,
                             partitionIndex: Seq[Array[Byte]],
                             diskTimeToLive: Int): Future[Response]

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
  private val chunksPerCallHist  = Kamon.histogram("chunks-per-call")
  private val chunkBytesHist     = Kamon.histogram("chunk-bytes-per-call")
  private val chunkLenHist       = Kamon.histogram("chunk-length")

  private val numIndexWriteCalls = Kamon.counter("index-write-calls-num")
  private val indexBytesHist     = Kamon.histogram("index-bytes-per-call")

  private val chunksetWrites     = Kamon.counter("chunkset-writes")
  var chunksetsWritten = 0
  var timeBucketsWritten = 0

  def addChunkWriteStats(numChunks: Int, totalChunkBytes: Long, chunkLen: Int): Unit = {
    chunksPerCallHist.record(numChunks)
    chunkBytesHist.record(totalChunkBytes)
    chunkLenHist.record(chunkLen)
  }

  def addIndexWriteStats(totalIndexBytes: Long): Unit = {
    numIndexWriteCalls.increment
    indexBytesHist.record(totalIndexBytes)
  }

  def chunksetWrite(): Unit = {
    chunksetWrites.increment
    chunksetsWritten += 1
  }

  def indexTimeBucketWritten(numBytes: Int): Unit = {
    timeBucketsWritten += 1
  }
}

/**
 * NullColumnStore keeps stats and partitions but other than that writes chunks nowhere.
 * It's convenient for testing though.
 */
class NullColumnStore(implicit sched: Scheduler) extends ColumnStore with StrictLogging {
  val sinkStats = new ChunkSinkStats
  val stats = new ChunkSourceStats

  // in-memory store of partition keys
  val partitionKeys = new ConcurrentHashMap[DatasetRef, scala.collection.mutable.Set[Types.PartitionKey]]().asScala

  def write(dataset: Dataset, chunksets: Observable[ChunkSet], diskTimeToLive: Int): Future[Response] = {
    chunksets.foreach { chunkset =>
      val totalBytes = chunkset.chunks.map(_.limit()).sum
      sinkStats.addChunkWriteStats(chunkset.chunks.length, totalBytes, chunkset.info.numRows)
      sinkStats.chunksetWrite()
      logger.debug(s"NullColumnStore: [${chunkset.partition}] ${chunkset.info}  ${chunkset.chunks.length} " +
                   s"chunks with $totalBytes bytes")
      chunkset.listener(chunkset.info)
    }
    Future.successful(Success)
  }

  def initialize(dataset: DatasetRef): Future[Response] = Future.successful(Success)

  def truncate(dataset: DatasetRef): Future[Response] = {
    partitionKeys -= dataset
    Future.successful(Success)
  }

  def dropDataset(dataset: DatasetRef): Future[Response] = Future.successful(Success)

  def reset(): Unit = {
    partitionKeys.clear()
  }

  override def shutdown(): Unit = {}

  def readRawPartitions(dataset: Dataset,
                        columnIDs: Seq[Types.ColumnId],
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] =  Observable.empty

  override def getScanSplits(dataset: DatasetRef, splitsPerNode: Int): Seq[ScanSplit] = Seq.empty

  private def createConcurrentSet[T]() = {
    java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap[T, java.lang.Boolean]).asScala
  }

  override def getPartKeyTimeBucket(dataset: Dataset, shardNum: Int,
                                    timeBucket: Int): Observable[PartKeyTimeBucketSegment] = Observable.empty

  override def writePartKeyTimeBucket(dataset: Dataset, shardNum: Int, timeBucket: Int,
                                      partitionIndex: Seq[Array[Byte]],
                                      diskTimeToLive: Int): Future[Response] = {
    sinkStats.indexTimeBucketWritten(partitionIndex.map(_.length).sum)
    Future.successful(Success)
  }
}

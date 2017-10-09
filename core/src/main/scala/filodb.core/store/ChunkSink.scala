package filodb.core.store

import scala.concurrent.Future

import kamon.Kamon
import monix.reactive.Observable

import filodb.core._
import filodb.core.metadata.{ Projection, RichProjection}



/**
 * ChunkSink is the base trait for a sink, or writer to a persistent store, of chunks
 */
trait ChunkSink {
  def sinkStats: ChunkSinkStats

  /**
   * Writes the ChunkSets appearing in a stream/Observable to persistent storage, with backpressure
   * @param projection the Projection to write to
   * @param version the version # to write the chunks to
   * @param chunksets an Observable stream of chunksets to write
   * @return Success when the chunksets stream ends and is completely written.
   *         Future.failure(exception) if an exception occurs.
   */
  def write(projection: RichProjection,
            version: Int,
            chunksets: Observable[ChunkSet]): Future[Response]

  /**
   * Initializes the ChunkSink for a given dataset projection.  Must be called once before writing.
   */
  def initializeProjection(projection: Projection): Future[Response]

  /**
   * Clears all data from the ChunkSink for that given projection, for all versions.
   * More like a truncation, not a drop.
   * NOTE: please make sure there are no reprojections or writes going on before calling this
   */
  def clearProjectionData(projection: Projection): Future[Response]

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

  def addChunkWriteStats(numChunks: Int, totalChunkBytes: Long): Unit = {
    numChunkWriteCalls.increment
    chunksPerCallHist.record(numChunks)
    chunkBytesHist.record(totalChunkBytes)
  }

  def addIndexWriteStats(totalIndexBytes: Long): Unit = {
    numIndexWriteCalls.increment
    indexBytesHist.record(totalIndexBytes)
  }

  def chunksetWrite(): Unit = { chunksetWrites.increment }
}

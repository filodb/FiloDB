package filodb.core.store

import com.typesafe.config.Config
import kamon.Kamon

/**
 * Something to enable ColumnStores to collect basic stats for perf measurement / monitoring
 */
trait ColumnStoreStats {
  /**
   * Call this for each invocation of the writeChunks method
   */
  def addChunkWriteStats(numChunks: Int, totalChunkBytes: Long): Unit

  /**
   * Call this for each invocation of index read method
   */
  def addIndexWriteStats(totalIndexBytes: Long): Unit

  def segmentAppend(): Unit

  def segmentEmpty(): Unit

  def segmentIndexMissingRead(): Unit

  def incrReadSegments(numSegments: Int): Unit
  def readSegments: Int
}

object ColumnStoreStats {
  def apply(): ColumnStoreStats = new KamonColumnStoreStats
}

private[store] class KamonColumnStoreStats extends ColumnStoreStats {
  private val numChunkWriteCalls = Kamon.metrics.counter("chunk-write-calls-num")
  private val chunksPerCallHist  = Kamon.metrics.histogram("chunks-per-call")
  private val chunkBytesHist     = Kamon.metrics.histogram("chunk-bytes-per-call")

  private val numIndexWriteCalls = Kamon.metrics.counter("index-write-calls-num")
  private val indexBytesHist     = Kamon.metrics.histogram("index-bytes-per-call")

  private val segmentAppends     = Kamon.metrics.counter("segment-appends")
  private val segmentEmpties     = Kamon.metrics.counter("segment-empties")
  private val segmentCacheMisses = Kamon.metrics.counter("segment-cache-misses")

  private val readSegmentsCtr    = Kamon.metrics.counter("read-segments")
  var readSegments: Int = 0

  def addChunkWriteStats(numChunks: Int, totalChunkBytes: Long): Unit = {
    numChunkWriteCalls.increment
    chunksPerCallHist.record(numChunks)
    chunkBytesHist.record(totalChunkBytes)
  }

  /**
   * Call this for each invocation of index read method
   */
  def addIndexWriteStats(totalIndexBytes: Long): Unit = {
    numIndexWriteCalls.increment
    indexBytesHist.record(totalIndexBytes)
  }

  def segmentAppend(): Unit = { segmentAppends.increment }

  def segmentEmpty(): Unit = { segmentEmpties.increment }

  def segmentIndexMissingRead(): Unit = { segmentCacheMisses.increment }

  def incrReadSegments(numSegments: Int): Unit = {
    readSegmentsCtr.increment(numSegments)
    readSegments += numSegments
  }
}
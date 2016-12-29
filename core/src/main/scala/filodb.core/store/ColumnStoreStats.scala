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
   * Call this for each invocation of index write method
   */
  def addIndexWriteStats(totalIndexBytes: Long): Unit

  def addFilterWriteStats(totalFilterBytes: Long): Unit

  def segmentAppend(): Unit

  def segmentEmpty(): Unit

  def segmentIndexMissingRead(): Unit

  def incrReadPartitions(numSegments: Int): Unit
  def readPartitions: Int
  def readChunkSets: Int

  def incrReadChunksets(): Unit
  def incrChunkWithNoInfo(): Unit
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
  private val filterBytesHist    = Kamon.metrics.histogram("filter-bytes-per-call")

  private val segmentAppends     = Kamon.metrics.counter("segment-appends")
  private val segmentEmpties     = Kamon.metrics.counter("segment-empties")
  private val segmentCacheMisses = Kamon.metrics.counter("segment-cache-misses")

  private val readPartitionsCtr  = Kamon.metrics.counter("read-partitions")
  private val readChunksetsCtr   = Kamon.metrics.counter("read-chunksets")
  private val chunkNoInfoCtr     = Kamon.metrics.counter("read-chunks-with-no-info")
  var readChunkSets: Int = 0
  var readPartitions: Int = 0

  def addChunkWriteStats(numChunks: Int, totalChunkBytes: Long): Unit = {
    numChunkWriteCalls.increment
    chunksPerCallHist.record(numChunks)
    chunkBytesHist.record(totalChunkBytes)
  }

  def addIndexWriteStats(totalIndexBytes: Long): Unit = {
    numIndexWriteCalls.increment
    indexBytesHist.record(totalIndexBytes)
  }

  def addFilterWriteStats(totalFilterBytes: Long): Unit = {
    filterBytesHist.record(totalFilterBytes)
  }

  def segmentAppend(): Unit = { segmentAppends.increment }

  def segmentEmpty(): Unit = { segmentEmpties.increment }

  def segmentIndexMissingRead(): Unit = { segmentCacheMisses.increment }

  def incrReadPartitions(numPartitions: Int): Unit = {
    readPartitionsCtr.increment(numPartitions)
    readPartitions += numPartitions
  }

  def incrReadChunksets(): Unit = {
    readChunksetsCtr.increment
    readChunkSets += 1
  }

  def incrChunkWithNoInfo(): Unit = { chunkNoInfoCtr.increment }
}
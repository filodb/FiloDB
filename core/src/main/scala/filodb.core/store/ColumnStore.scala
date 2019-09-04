package filodb.core.store

import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.query._
import filodb.memory.BinaryRegionLarge

sealed trait PartitionScanMethod {
  def shard: Int
}

final case class SinglePartitionScan(partition: Array[Byte], shard: Int = 0) extends PartitionScanMethod

object SinglePartitionScan {
  def apply(partKeyAddr: Long, shard: Int): SinglePartitionScan =
    SinglePartitionScan(BinaryRegionLarge.asNewByteArray(partKeyAddr), shard)
}

final case class MultiPartitionScan(partitions: Seq[Array[Byte]],
                                    shard: Int = 0) extends PartitionScanMethod
// NOTE: One ColumnFilter per column please.
final case class FilteredPartitionScan(split: ScanSplit,
                                       filters: Seq[ColumnFilter] = Nil) extends PartitionScanMethod {
  def shard: Int = split match {
    case ShardSplit(shard) => shard
    case other: ScanSplit  => ???
  }
}

sealed trait ChunkScanMethod {
  def startTime: Long
  def endTime: Long
}

trait AllTimeScanMethod {
  def startTime: Long = Long.MinValue
  def endTime: Long = Long.MaxValue
}

case object AllChunkScan extends AllTimeScanMethod with ChunkScanMethod
final case class TimeRangeChunkScan(startTime: Long, endTime: Long) extends ChunkScanMethod
case object WriteBufferChunkScan extends AllTimeScanMethod with ChunkScanMethod
// Only read chunks which are in memory
case object InMemoryChunkScan extends AllTimeScanMethod with ChunkScanMethod

trait ScanSplit {
  // Should return a set of hostnames or IP addresses describing the preferred hosts for that scan split
  def hostnames: Set[String]
}

final case class ShardSplit(shard: Int) extends ScanSplit {
  def hostnames: Set[String] = Set.empty
}

/**
 * ColumnStore defines all of the read/query methods for a ColumnStore.
 * TODO: only here to keep up appearances with old stuff, refactor further.
 */
trait ColumnStore extends ChunkSink with RawChunkSource with StrictLogging {
  /**
   * Shuts down the ColumnStore, including any threads that might be hanging around
   */
  def shutdown(): Unit

  def getPartKeyTimeBucket(ref: DatasetRef, shardNum: Int, timeBucket: Int): Observable[PartKeyTimeBucketSegment]
}

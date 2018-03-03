package filodb.core.store

import com.typesafe.scalalogging.StrictLogging

import filodb.core.binaryrecord.{BinaryRecord, BinaryRecordWrapper}
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.Types.PartitionKey

sealed trait PartitionScanMethod {
  def shard: Int
}

final case class SinglePartitionScan(partition: PartitionKey, shard: Int = 0) extends PartitionScanMethod
final case class MultiPartitionScan(partitions: Seq[PartitionKey],
                                    shard: Int = 0) extends PartitionScanMethod
// NOTE: One ColumnFilter per column please.
final case class FilteredPartitionScan(split: ScanSplit,
                                       filters: Seq[ColumnFilter] = Nil) extends PartitionScanMethod {
  def shard: Int = split match {
    case ShardSplit(shard) => shard
    case other: ScanSplit  => ???
  }
}

sealed trait ChunkScanMethod
case object AllChunkScan extends ChunkScanMethod
// NOTE: BinaryRecordWrapper must be used as this case class might be Java Serialized
final case class RowKeyChunkScan(firstBinKey: BinaryRecordWrapper,
                                 lastBinKey: BinaryRecordWrapper) extends ChunkScanMethod {
  def startkey: BinaryRecord = firstBinKey.binRec
  def endkey: BinaryRecord = lastBinKey.binRec
}
case object LastSampleChunkScan extends ChunkScanMethod

object RowKeyChunkScan {
  def apply(startKey: BinaryRecord, endKey: BinaryRecord): RowKeyChunkScan =
    RowKeyChunkScan(BinaryRecordWrapper(startKey), BinaryRecordWrapper(endKey))

  def apply(dataset: Dataset, startKey: Seq[Any], endKey: Seq[Any]): RowKeyChunkScan =
    RowKeyChunkScan(BinaryRecord(dataset, startKey), BinaryRecord(dataset, endKey))
}


final case class QuerySpec(column: String,
                           aggregateFunc: AggregationFunction,
                           aggregateArgs: Seq[String] = Nil,
                           combinerFunc: CombinerFunction = CombinerFunction.Simple,
                           combinerArgs: Seq[String] = Nil)

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
trait ColumnStore extends ChunkSink with ChunkSource with StrictLogging {
  /**
   * Shuts down the ColumnStore, including any threads that might be hanging around
   */
  def shutdown(): Unit
}

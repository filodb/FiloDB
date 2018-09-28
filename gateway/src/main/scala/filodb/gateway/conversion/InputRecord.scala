package filodb.gateway.conversion

import remote.RemoteStorage.TimeSeries

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset

/**
 * An InputRecord represents one "record" of timeseries data for input to FiloDB system.
 * It knows how to extract the information necessary for shard calculation as well as
 * how to add the right fields to the RecordBuilder.
 */
trait InputRecord {
  /**
   * The shardKeyHash and partitionKeyHash for ShardMappers.ingestionShard method
   */
  def shardKeyHash: Int
  def partitionKeyHash: Int

  /**
   * The values for each of the tag keys found in DatasetOptions.nonMetricShardColumns
   * @return the nonMetricShardColumns tag values, in the same order as nonMetricShardColumns
   *         If any tag/key is not found, then the Seq will be truncated at the last found value.
   */
  def nonMetricShardValues: Seq[String]

  // This is the metric value needed for shard spread calculation
  def getMetric: String

  /**
   * Adds the contents of this record to RecordBuilder as one or more Filo records
   */
  def addToBuilder(builder: RecordBuilder): Unit
}

/**
 * A Prometheus-format time series input record.
 * Logic in here does the following conversions:
 * - Shard/partition hashes are calculated scuh that histogram time series go to the same shard.
 *   IE _bucket _sum _count are stripped off of all metric names.
 *   Similarly "le" is stripped off of Prom tags for shard calculation purposes
 * shardKeys in tags are used to compute the shardKeyHash, all other tags are used to compute
 * the partition key hash.
 */
case class PrometheusInputRecord(tags: Map[String, String],
                                 dataset: Dataset,
                                 timestamp: Long,
                                 value: Double) extends InputRecord {
  import collection.JavaConverters._

  val shardKeys = dataset.options.shardKeyColumns.toSet
  val scalaKVs = tags.toSeq
  val originalKVs = new java.util.ArrayList(scalaKVs.asJava)
  val forShardKVs = scalaKVs.map { case (k, v) =>
                      val trimmedVal = RecordBuilder.trimShardColumn(dataset, k, v)
                      (k, trimmedVal)
                    }
  val kvsForShardCalc = new java.util.ArrayList(forShardKVs.asJava)

  // Get hashes and sort tags of the keys/values for shard calculation
  val hashes = RecordBuilder.sortAndComputeHashes(kvsForShardCalc)

  final def shardKeyHash: Int = RecordBuilder.combineHashIncluding(kvsForShardCalc, hashes, shardKeys)
                                             .getOrElse(PrometheusInputRecord.DefaultShardHash)
  final def partitionKeyHash: Int = RecordBuilder.combineHashExcluding(kvsForShardCalc, hashes, shardKeys)

  final def nonMetricShardValues: Seq[String] = dataset.options.nonMetricShardColumns.map(tags)
  final def getMetric: String = tags(dataset.options.metricColumn)

  final def addToBuilder(builder: RecordBuilder): Unit = {
    originalKVs.sort(RecordBuilder.stringPairComparator)
    builder.startNewRecord()
    builder.addLong(timestamp)
    builder.addDouble(value)
    builder.addSortedPairsAsMap(originalKVs, hashes)
    builder.endRecord()
  }
}

object PrometheusInputRecord {
  val DefaultShardHash = -1

  // Create PrometheusInputRecord from a TimeSeries protobuf object
  def apply(tsProto: TimeSeries, dataset: Dataset): PrometheusInputRecord = {
    val tags = (0 until tsProto.getLabelsCount).map { i =>
      val labelPair = tsProto.getLabels(i)
      (labelPair.getName, labelPair.getValue)
    }
    val sample = tsProto.getSamples(0)
    PrometheusInputRecord(tags.toMap, dataset, sample.getTimestampMs, sample.getValue)
  }
}
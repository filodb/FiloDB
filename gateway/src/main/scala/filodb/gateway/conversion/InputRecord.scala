package filodb.gateway.conversion

import scala.language.postfixOps

import remote.RemoteStorage.TimeSeries
import scalaxy.loops._

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
 * The tags should NOT include the metric name.
 */
case class PrometheusInputRecord(tags: Map[String, String],
                                 metric: String,
                                 dataset: Dataset,
                                 timestamp: Long,
                                 value: Double) extends InputRecord {
  import collection.JavaConverters._

  val trimmedMetric = RecordBuilder.trimShardColumn(dataset, dataset.options.metricColumn, metric)
  val javaTags = new java.util.ArrayList(tags.toSeq.asJava)

  // Get hashes and sort tags of the keys/values for shard calculation
  val hashes = RecordBuilder.sortAndComputeHashes(javaTags)

  final def shardKeyHash: Int = RecordBuilder.shardKeyHash(nonMetricShardValues, trimmedMetric)
  final def partitionKeyHash: Int = RecordBuilder.combineHashExcluding(javaTags, hashes,
                                                    dataset.options.ignorePartKeyHashTags)

  val nonMetricShardValues: Seq[String] = dataset.options.nonMetricShardColumns.flatMap(tags.get)
  final def getMetric: String = metric

  final def addToBuilder(builder: RecordBuilder): Unit = {
    builder.startNewRecord()
    builder.addLong(timestamp)
    builder.addDouble(value)
    builder.startMap()
    val metricBytes = metric.getBytes
    builder.addMapKeyValueHash(dataset.options.metricBytes, dataset.options.metricHash,
                               metricBytes, 0, metricBytes.size)
    for { i <- 0 until javaTags.size optimized } {
      val (k, v) = javaTags.get(i)
      builder.addMapKeyValue(k.getBytes, v.getBytes)
      builder.updatePartitionHash(hashes(i))
    }
    builder.endMap(bulkHash = false)

    builder.endRecord()
  }
}

object PrometheusInputRecord {
  val DefaultShardHash = -1

  // Create PrometheusInputRecords from a TimeSeries protobuf object
  def apply(tsProto: TimeSeries, dataset: Dataset): Seq[PrometheusInputRecord] = {
    val tags = (0 until tsProto.getLabelsCount).map { i =>
      val labelPair = tsProto.getLabels(i)
      (labelPair.getName, labelPair.getValue)
    }
    val metricTags = tags.filter(_._1 == dataset.options.metricColumn)
    if (metricTags.isEmpty) {
      Nil
    } else {
      val metric = metricTags.head._2
      val transformedTags = transformTags(tags.filterNot(_._1 == dataset.options.metricColumn), dataset).toMap
      (0 until tsProto.getSamplesCount).map { i =>
        val sample = tsProto.getSamples(i)
        PrometheusInputRecord(transformedTags, metric, dataset, sample.getTimestampMs, sample.getValue)
      }
    }
  }

  /**
   * Uses DatasetOptions.copyTags to copy missing tags.
   * If a tag in copyTags is found and the destination tag is missing, then the destination tag is created
   * with the value from the source tag.
   */
  def transformTags(tags: Seq[(String, String)], dataset: Dataset): Seq[(String, String)] = {
    val keys = tags.map(_._1).toSet
    val extraTags = new collection.mutable.ArrayBuffer[(String, String)]()
    tags.foreach { case (k, v) =>
      if (dataset.options.copyTags contains k) {
        val renamedKey = dataset.options.copyTags(k)
        if (!(keys contains renamedKey))
          extraTags += renamedKey -> v
      }
    }
    tags ++ extraTags
  }
}
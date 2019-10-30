package filodb.gateway.conversion

import remote.RemoteStorage.TimeSeries

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{DatasetOptions, Schema}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String => ZCUTF8}
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}

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

// Utilities for adding data for standard schemas
object InputRecord {
  import filodb.core.metadata.Schemas._
  import ZCUTF8._

  /**
   * Writes a BinaryRecord for the built-in gauge schema.  Note that tags are NOT manipulated at all.
   */
  def writeGaugeRecord(builder: RecordBuilder,
                       metric: String,
                       tags: Map[String, String],
                       timestamp: Long,
                       value: Double): Unit =
    writeKVRecord(builder, metric, tags, timestamp, value, gauge)

  private def writeKVRecord(builder: RecordBuilder,
                            metric: String,
                            tags: Map[String, String],
                            timestamp: Long,
                            value: Double,
                            schema: Schema): Unit = {
    builder.startNewRecord(schema)
    builder.addLong(timestamp)
    builder.addDouble(value)

    builder.addString(metric)
    builder.addMap(tags.map { case (k, v) => (k.utf8, v.utf8) })
    builder.endRecord()
  }

  def writePromCounterRecord(builder: RecordBuilder,
                             metric: String,
                             tags: Map[String, String],
                             timestamp: Long,
                             count: Double): Unit =
    writeKVRecord(builder, metric, tags, timestamp, count, promCounter)

  def writeUntypedRecord(builder: RecordBuilder,
                         metric: String,
                         tags: Map[String, String],
                         timestamp: Long,
                         value: Double): Unit =
    writeKVRecord(builder, metric, tags, timestamp, value, untyped)


  /**
   * Writes a Prometheus-style increasing histogram record, along with the sum and count,
   * using the efficient prom-histogram schema, storing the entire histogram together for efficiency.
   * The list of key-values should have "sum", "count", and bucket tops as keys, in any order.
   * This code will sort and encode histograms correctly from those.
   */
  def writePromHistRecord(builder: RecordBuilder,
                          metric: String,
                          tags: Map[String, String],
                          timestamp: Long,
                          kvs: Seq[(String, Double)]): Unit = {
    var sum = Double.NaN
    var count = Double.NaN

    // Filter out sum and count, then convert and sort buckets
    val sortedBuckets = kvs.filter {
      case ("sum", v) => sum = v
                         false
      case ("count", v) => count = v
                           false
      case other      => true
    }.map {
      case ("+Inf", v) => (Double.PositiveInfinity, v.toLong)
      case (k, v) =>      (k.toDouble, v.toLong)
    }.sorted

    // Built up custom histogram objects and scheme, then encode
    val buckets = CustomBuckets(sortedBuckets.map(_._1).toArray)
    val hist = LongHistogram(buckets, sortedBuckets.map(_._2).toArray)

    // Now, write out histogram
    builder.startNewRecord(promHistogram)
    builder.addLong(timestamp)
    builder.addDouble(sum)
    builder.addDouble(count)
    builder.addBlob(hist.serialize())

    builder.addString(metric)
    builder.addMap(tags.map { case (k, v) => (k.utf8, v.utf8) })
    builder.endRecord()
  }
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
                                 timestamp: Long,
                                 value: Double) extends InputRecord {
  import filodb.core.metadata.Schemas.promCounter
  import PrometheusInputRecord._
  import collection.JavaConverters._

  val trimmedMetric = RecordBuilder.trimShardColumn(promCounter.options, metricCol, metric)
  val javaTags = new java.util.ArrayList(tags.toSeq.asJava)

  // Get hashes and sort tags of the keys/values for shard calculation
  val hashes = RecordBuilder.sortAndComputeHashes(javaTags)

  final def shardKeyHash: Int = RecordBuilder.shardKeyHash(nonMetricShardValues, trimmedMetric)
  final def partitionKeyHash: Int = RecordBuilder.combineHashExcluding(javaTags, hashes, ignorePartKeyTags)

  val nonMetricShardValues: Seq[String] = nonMetricShardCols.flatMap(tags.get)
  final def getMetric: String = metric

  final def addToBuilder(builder: RecordBuilder): Unit =
    InputRecord.writeUntypedRecord(builder, metric, tags, timestamp, value)
}

object PrometheusInputRecord {
  val DefaultShardHash = -1

  import filodb.core.metadata.Schemas.promCounter
  val metricCol = promCounter.options.metricColumn
  val nonMetricShardCols = promCounter.options.nonMetricShardColumns
  val ignorePartKeyTags = promCounter.options.ignorePartKeyHashTags

  // Create PrometheusInputRecords from a TimeSeries protobuf object
  def apply(tsProto: TimeSeries): Seq[PrometheusInputRecord] = {
    val tags = (0 until tsProto.getLabelsCount).map { i =>
      val labelPair = tsProto.getLabels(i)
      (labelPair.getName, labelPair.getValue)
    }
    val metricTags = tags.filter { case (k, v) => k == "__name__" || k == metricCol }
    if (metricTags.isEmpty) {
      Nil
    } else {
      val metric = metricTags.head._2
      val metricKey = metricTags.head._1
      val transformedTags = transformTags(tags.filterNot(_._1 == metricKey), promCounter.options)
      (0 until tsProto.getSamplesCount).map { i =>
        val sample = tsProto.getSamples(i)
        PrometheusInputRecord(transformedTags, metric, sample.getTimestampMs, sample.getValue)
      }
    }
  }

  /**
   * Uses DatasetOptions.copyTags to copy missing tags.
   * If a tag in copyTags is found and the destination tag is missing, then the destination tag is created
   * with the value from the source tag.
   */
  def transformTags(tags: Seq[(String, String)],
                    options: DatasetOptions): Map[String, String] = {
    val extraTags = new collection.mutable.HashMap[String, String]()
    val tagsMap = tags.toMap
    for ((k, v) <- options.copyTags) {
      if (!extraTags.contains(v)
        && !tagsMap.contains(v)
        && tagsMap.contains(k)) {
        extraTags += v -> tagsMap(k)
      }
    }
    tagsMap ++ extraTags
  }
}

/**
 * A generic InputRecord that can serve different data schemas, so long as the partition key consists of:
 *  - a single metric StringColumn
 *  - a MapColumn of tags
 *
 * Can be used to adapt custom dataset/schemas for input into FiloDB using the gateway.
 * Not going to be the fastest InputRecord but extremely flexible.
 *
 * @param values the data column values, first one is probably timestamp
 */
class MetricTagInputRecord(values: Seq[Any],
                           metric: String,
                           tags: Map[ZCUTF8, ZCUTF8],
                           schema: Schema) extends InputRecord {
  final def shardKeyHash: Int = RecordBuilder.shardKeyHash(nonMetricShardValues, metric)
  // NOTE: this is probably not very performant right now.
  final def partitionKeyHash: Int = tags.hashCode

  val nonMetricShardValues: Seq[String] =
    schema.options.nonMetricShardKeyUTF8.flatMap(tags.get).map(_.toString).toSeq
  final def getMetric: String = metric

  def addToBuilder(builder: RecordBuilder): Unit = {
    val reader = SeqRowReader(values :+ metric :+ tags)
    builder.addFromReader(reader, schema)
  }
}

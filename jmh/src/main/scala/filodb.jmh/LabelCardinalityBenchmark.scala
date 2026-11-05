package filodb.jmh

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration._

import ch.qos.logback.classic.{Level => LogLevel, Logger}
import org.apache.datasketches.cpc.CpcSketch
import org.openjdk.jmh.annotations._

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.{MapItemConsumer, RecordBuilder, RecordSchema}
import filodb.core.memstore.{PartKeyIndexRaw, PartKeyLuceneIndex}
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter}
import filodb.memory.{BinaryRegionConsumer, MemFactory, UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.ZeroCopyUTF8String
import filodb.memory.format.ZeroCopyUTF8String._

/**
 * Compares the ways of computing label cardinality (per label CPC sketches, as LabelCardinalityExec does)
 * for one metric on a faceted Lucene index of a 1M series shard:
 *
 *  - faceted: label names from the label list facet, then one faceted label values lookup per label
 *    (the default path)
 *  - partKeyScan: labels of each matching part key read from the part key bytes and added to the sketches in the
 *    index search callback, as with query.label-cardinality-partkey-scan-enabled
 *    (TimeSeriesShard.foreachPartKeyLabelWithFilters, then LabelCardinalityExec)
 *
 * The shard holds metrics of 1k, 10k and 100k series plus filler metrics up to 1M series, with labels unique
 * across the shard (instance, pod) and low cardinality labels.  matchingSeries selects the metric queried.
 * With refreshBeforeQuery, one series is added and the index readers refreshed before every query, measuring
 * the first query after a refresh, when the faceted path rebuilds its cached facet state for every label.
 * Readers refresh every 30-55s in production, so the two settings bound the cost of the faceted path.
 *
 * Run with: jmh:run -prof gc filodb.jmh.LabelCardinalityBenchmark
 */
// scalastyle:off
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
class LabelCardinalityBenchmark {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(LogLevel.ERROR)

  @Param(Array("1000", "10000", "100000"))
  var matchingSeries: Int = _

  @Param(Array("false", "true"))
  var refreshBeforeQuery: Boolean = _

  final val totalSeries = 1000000
  final val queriedMetricSizes = Seq(1000, 10000, 100000)
  final val fillerMetricSize = 10000
  final val maxSeries = 1000000  // default timeSeriesScanned and execPlanLeafSamples limits
  final val logK = 10

  val schema = Schemas.promCounter
  val binSchema = schema.partition.binSchema
  val now = System.currentTimeMillis()
  var index: PartKeyLuceneIndex = _
  var filters: Seq[ColumnFilter] = _
  private var nextPartId = 0
  private var lookupTime = now + 1000
  private val builder = new RecordBuilder(MemFactory.onHeapFactory, RecordBuilder.DefaultContainerSize, false)

  private def tags(metricIdx: Int, i: Int): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = Map(
    "_ws_" -> "demo",
    "_ns_" -> "App-0",
    "instance" -> s"10.$metricIdx.${i / 256}.${i % 256}:9100", // unique across the shard
    "pod" -> s"svc-$metricIdx-${i / 2}-7d9f8b6c4",              // unique across the shard, 2 series per pod
    "container" -> s"container-${i % 10}",
    "job" -> s"job-${i % 5}",
    "dc" -> s"dc-${i % 3}",
    "host" -> s"host-${i % 1000}",
    "version" -> s"v1.${i % 20}"
  ).map { case (k, v) => k.utf8 -> v.utf8 }

  private def addSeries(metric: String, metricIdx: Int, from: Int, until: Int): Unit = {
    for (i <- from until until) builder.partKeyFromObjects(schema, metric, tags(metricIdx, i))
    builder.allContainers.foreach(_.consumeRecords(new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit = {
        index.addPartKey(binSchema.asByteArray(base, offset), nextPartId, now)()
        nextPartId += 1
      }
    }))
    builder.removeAndFreeContainers(builder.allContainers.size)
  }

  // Changes the end of the time range for every query, so that the Lucene query cache is not hit
  private def nextLookupTime(): Long = {
    lookupTime += 1
    lookupTime
  }

  @Setup(Level.Trial)
  def setup(): Unit = {
    index = new PartKeyLuceneIndex(DatasetRef("prometheus"), schema.partition, true, true, 0, 1.hour.toMillis)
    val start = System.nanoTime()
    queriedMetricSizes.zipWithIndex.foreach { case (size, idx) => addSeries(s"metric_$size", idx, 0, size) }
    var metricIdx = queriedMetricSizes.size
    while (nextPartId < totalSeries) {
      addSeries(s"filler_$metricIdx", metricIdx, 0, math.min(fillerMetricSize, totalSeries - nextPartId))
      metricIdx += 1
    }
    index.refreshReadersBlocking()
    println(s"Indexed $nextPartId series in ${(System.nanoTime() - start) / 1000000} ms")

    filters = Seq(ColumnFilter("_ws_", Filter.Equals("demo")), ColumnFilter("_ns_", Filter.Equals("App-0")),
      ColumnFilter("_metric_", Filter.Equals(s"metric_$matchingSeries")))

    // sanity checks: the queried metric has matchingSeries series and all approaches agree
    val matched = index.partKeyRecordsFromFilters(filters, now, Long.MaxValue, Int.MaxValue).size
    require(matched == matchingSeries, s"expected $matchingSeries matching series, got $matched")
    val facetedEstimates = faceted().map { case (l, s) => l -> s.getEstimate }
    val scanEstimates = partKeyScan().map { case (l, s) => l.toString -> s.getEstimate }
    require(facetedEstimates.keySet == scanEstimates.keySet, s"labels differ: $facetedEstimates $scanEstimates")
    facetedEstimates.foreach { case (label, estimate) =>
      require(math.abs(scanEstimates(label) - estimate) <= 0.05 * estimate + 1, s"$label: $estimate vs $scanEstimates")
    }
    println(s"Label cardinality estimates for metric_$matchingSeries: $scanEstimates")
  }

  @Setup(Level.Invocation)
  def refresh(): Unit = if (refreshBeforeQuery) {
    addSeries("refresh_metric", 999, nextPartId, nextPartId + 1)
    index.refreshReadersBlocking()
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = index.closeIndex()

  @Benchmark
  def faceted(): mutable.Map[String, CpcSketch] = {
    val endTime = nextLookupTime()
    val sketches = mutable.Map[String, CpcSketch]()
    index.labelNamesEfficient(filters, now, endTime).foreach { label =>
      // values are converted to and from ZeroCopyUTF8String as in TimeSeriesShard and LabelCardinalityExec
      index.labelValuesEfficient(filters, now, endTime, label, maxSeries).iterator.map(_.utf8).foreach { value =>
        sketches.getOrElseUpdate(label, new CpcSketch(logK)).update(value.toString)
      }
    }
    sketches
  }

  private val columns = binSchema.columnTypes.zip(binSchema.colNames.map(_.utf8)).zipWithIndex
  private val typeLabel = Schemas.TypeLabel.utf8

  @Benchmark
  def partKeyScan(): mutable.Map[ZeroCopyUTF8String, CpcSketch] = {
    val sketchMap = mutable.Map[ZeroCopyUTF8String, CpcSketch]()
    // as in LabelCardinalityExec
    def visit(label: ZeroCopyUTF8String, value: ZeroCopyUTF8String): Unit = if (value.numBytes > 0) {
      var sketch = sketchMap.getOrElse(label, null)
      if (sketch == null && sketchMap.size < 200) {
        sketch = new CpcSketch(logK)
        sketchMap.put(ZeroCopyUTF8String(label.asNewByteArray), sketch)
      }
      if (sketch != null) sketch.update(value.asNewByteArray)
    }
    // as in TimeSeriesShard.foreachPartKeyLabelWithFilters
    val mapConsumer = new MapItemConsumer {
      def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
        val label = new ZeroCopyUTF8String(keyBase, keyOffset + 1, UTF8StringShort.numBytes(keyBase, keyOffset))
        if (label != typeLabel)
          visit(label, new ZeroCopyUTF8String(valueBase, valueOffset + 2, UTF8StringMedium.numBytes(valueBase, valueOffset)))
      }
    }
    var schemaId = -1
    var schemaName = ZeroCopyUTF8String.empty
    index.foreachPartKeyMatchingFilter(filters, now, nextLookupTime(), { partKey =>
      val offset = PartKeyIndexRaw.bytesRefToUnsafeOffset(partKey.offset)
      columns.foreach {
        case ((StringColumn, name), i) => visit(name, binSchema.asZCUTF8Str(partKey.bytes, offset, i))
        case ((MapColumn, _), i)       => binSchema.consumeMapItems(partKey.bytes, offset, i, mapConsumer)
        case _                         =>
      }
      val id = RecordSchema.schemaID(partKey.bytes, offset)
      if (id != schemaId) {
        schemaId = id
        schemaName = Schemas.global.schemaName(id).utf8
      }
      visit(typeLabel, schemaName)
    })
    sketchMap
  }
}

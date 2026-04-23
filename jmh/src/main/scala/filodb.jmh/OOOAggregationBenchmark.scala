package filodb.jmh

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import ch.qos.logback.classic.{Level, Logger}
import org.openjdk.jmh.annotations.{Level => JMHLevel, _}
import org.openjdk.jmh.infra.Blackhole

import filodb.core.TestData
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore._
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query._
import filodb.core.store._
import filodb.memory._
import filodb.memory.format.TupleRowReader

//scalastyle:off regex
/**
 * JMH benchmark comparing the performance of AggregatingTimeSeriesPartition (out-of-order aggregation)
 * vs standard TimeSeriesPartition ingestion and query paths.
 *
 * Benchmarks:
 * - ingestRegular:      Ingestion throughput for standard TimeSeriesPartition
 * - ingestAggregating:  Ingestion throughput for AggregatingTimeSeriesPartition (with aggregation)
 * - queryRegular:       Query (iterate rows) from standard partition
 * - queryAggregating:   Query (iterate rows) from AggregatingRangeVector with active buckets
 * - finalizeBuckets:    Throughput of finalizing and flushing aggregation buckets
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class OOOAggregationBenchmark {

  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  // --- Memory infrastructure ---
  val memFactory = new NativeMemoryManager(100 * 1024 * 1024)
  val evictionLock = new EvictionLock
  //scalastyle:off null
  val blockStore = new PageAlignedBlockManager(
    200 * 1024 * 1024,
    new MemoryStats(Map("test" -> "test")),
    null, // no reclaimer needed for benchmarks
    1,
    evictionLock
  )
  //scalastyle:on null

  // --- Dataset definitions ---
  val scalarDatasetOptions = DatasetOptions.DefaultOptions.copy(
    metricColumn = "series",
    shardKeyColumns = Seq("series")
  )

  // Regular dataset (no aggregation)
  val regularDataset = Dataset("regular_metrics", Seq("series:string"),
    Seq("timestamp:ts", "value:double"), scalarDatasetOptions)

  // Aggregating dataset (with bucket aggregation for OOO support)
  val aggregatingDataset = Dataset("agg_metrics", Seq("series:string"),
    Seq("timestamp:ts", "value:double"), scalarDatasetOptions,
    aggregatorNames = Seq("dSum(1)"),
    aggregationIntervalMs = 60000L,
    aggregationOooToleranceMs = 30000L)

  // --- Partition infrastructure ---
  val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
  val defaultPartKey = partKeyBuilder.partKeyFromObjects(regularDataset.schema, "series0")

  val regularBufferPool = new WriteBufferPool(memFactory, regularDataset.schema.data, TestData.storeConf)
  val aggBufferPool = new WriteBufferPool(memFactory, aggregatingDataset.schema.data, TestData.storeConf)

  // --- Partitions ---
  var regularPart: TimeSeriesPartition = _
  var aggPart: AggregatingTimeSeriesPartition = _
  var ingestBlockHolder: BlockMemFactory = _

  // --- Pre-generated sample data ---
  // We generate samples in-order and out-of-order for realistic benchmarking
  val numSamples = 1000
  val baseTs = 1000000L
  val sampleStep = 10000L // 10s between samples

  // In-order samples
  val inOrderSamples: Array[TupleRowReader] = (0 until numSamples).map { i =>
    TupleRowReader((Some(baseTs + i * sampleStep), Some(i.toDouble)))
  }.toArray

  // Out-of-order samples: groups of 10 shuffled within each group (realistic OOO pattern)
  val oooSamples: Array[TupleRowReader] = {
    val grouped = (0 until numSamples).grouped(10).toArray
    val rng = new java.util.Random(42) // deterministic seed
    grouped.flatMap { group =>
      val shuffled = group.toArray
      // Fisher-Yates shuffle
      var i = shuffled.length - 1
      while (i > 0) {
        val j = rng.nextInt(i + 1)
        val tmp = shuffled(i)
        shuffled(i) = shuffled(j)
        shuffled(j) = tmp
        i -= 1
      }
      shuffled.map(idx => TupleRowReader((Some(baseTs + idx * sampleStep), Some(idx.toDouble))))
    }
  }

  // For query benchmarks: pre-populated partitions with data already ingested
  var queryRegularPart: TimeSeriesPartition = _
  var queryAggPart: AggregatingTimeSeriesPartition = _
  var queryBlockHolder: BlockMemFactory = _
  var queryAggRangeVector: AggregatingRangeVector = _

  // For finalize benchmark
  var finalizePart: AggregatingTimeSeriesPartition = _
  var finalizeBlockHolder: BlockMemFactory = _

  // Current sample index for ingestion benchmarks
  var regularIdx = 0
  var aggIdx = 0

  private def makePartition(dataset: Dataset, bufPool: WriteBufferPool): TimeSeriesPartition = {
    val bufPools = debox.Map(dataset.schema.schemaHash -> bufPool)
    val shardInfo = TimeSeriesShardInfo(0,
      new TimeSeriesShardStats(dataset.ref, 0), bufPools, memFactory)
    new TimeSeriesPartition(0, dataset.schema, defaultPartKey, shardInfo, 40)
  }

  private def makeAggPartition(dataset: Dataset, bufPool: WriteBufferPool): AggregatingTimeSeriesPartition = {
    val bufPools = debox.Map(dataset.schema.schemaHash -> bufPool)
    val shardInfo = TimeSeriesShardInfo(0,
      new TimeSeriesShardStats(dataset.ref, 0), bufPools, memFactory)
    new AggregatingTimeSeriesPartition(0, dataset.schema, defaultPartKey, shardInfo, 40)
  }

  @Setup(JMHLevel.Trial)
  def setupQueryPartitions(): Unit = {
    queryBlockHolder = new BlockMemFactory(blockStore,
      regularDataset.schema.data.blockMetaSize, Map("query" -> "test"), true)

    queryRegularPart = makePartition(regularDataset,
      new WriteBufferPool(memFactory, regularDataset.schema.data, TestData.storeConf))
    queryAggPart = makeAggPartition(aggregatingDataset,
      new WriteBufferPool(memFactory, aggregatingDataset.schema.data, TestData.storeConf))

    ingestQueryData()
    buildQueryRangeVector()
  }

  private def ingestQueryData(): Unit = {
    val fim = Option(TestData.storeConf.flushInterval.toMillis)
    val ta = TestData.storeConf.timeAlignedChunksEnabled
    val queryNumSamples = 500
    for (i <- 0 until queryNumSamples) {
      val ts = baseTs + i * sampleStep
      val row = TupleRowReader((Some(ts), Some(i.toDouble)))
      queryRegularPart.ingest(ts, row, queryBlockHolder, ta, fim, false)
      queryAggPart.ingest(ts, row, queryBlockHolder, ta, fim, false)
    }
  }

  private def buildQueryRangeVector(): Unit = {
    val endTs = baseTs + 500 * sampleStep
    val columnIDs = Array(0, 1)
    queryAggRangeVector = AggregatingRangeVector(
      PartitionRangeVectorKey(
        Left(queryAggPart),
        aggregatingDataset.schema.partKeySchema,
        aggregatingDataset.schema.infosFromIDs(
          aggregatingDataset.schema.partition.columns.map(_.id)),
        0, 0, queryAggPart.partID, aggregatingDataset.schema.name
      ),
      queryAggPart,
      TimeRangeChunkScan(baseTs, endTs),
      columnIDs,
      new AtomicLong(0),
      new AtomicLong(0),
      Long.MaxValue,
      "benchmark-query"
    )
  }

  @Setup(JMHLevel.Iteration)
  def setupIteration(): Unit = {
    ingestBlockHolder = new BlockMemFactory(blockStore,
      regularDataset.schema.data.blockMetaSize, Map("test" -> "test"), true)

    regularPart = makePartition(regularDataset, regularBufferPool)
    aggPart = makeAggPartition(aggregatingDataset, aggBufferPool)

    val finBufPool = new WriteBufferPool(memFactory, aggregatingDataset.schema.data, TestData.storeConf)
    finalizePart = makeAggPartition(aggregatingDataset, finBufPool)
    finalizeBlockHolder = new BlockMemFactory(blockStore,
      aggregatingDataset.schema.data.blockMetaSize, Map("finalize" -> "test"), true)

    regularIdx = 0
    aggIdx = 0
  }

  private val flushIntervalMillis = Option(TestData.storeConf.flushInterval.toMillis)
  private val timeAligned = TestData.storeConf.timeAlignedChunksEnabled

  // ==========================================================================
  // Ingestion Benchmarks
  // ==========================================================================

  /**
   * Ingestion throughput for standard TimeSeriesPartition (no aggregation).
   * Measures the baseline cost of ingesting one sample.
   */
  @Benchmark
  def ingestRegular(bh: Blackhole): Unit = {
    val row = inOrderSamples(regularIdx % numSamples)
    val ts = baseTs + regularIdx * sampleStep
    regularPart.ingest(ts, row, ingestBlockHolder, timeAligned, flushIntervalMillis, false)
    regularIdx += 1
    bh.consume(regularPart)
  }

  /**
   * Ingestion throughput for AggregatingTimeSeriesPartition with in-order samples.
   * Measures the overhead of the aggregation path compared to standard ingestion.
   */
  @Benchmark
  def ingestAggregatingInOrder(bh: Blackhole): Unit = {
    val row = inOrderSamples(aggIdx % numSamples)
    val ts = baseTs + aggIdx * sampleStep
    aggPart.ingest(ts, row, ingestBlockHolder, timeAligned, flushIntervalMillis, false)
    aggIdx += 1
    bh.consume(aggPart)
  }

  /**
   * Ingestion throughput for AggregatingTimeSeriesPartition with out-of-order samples.
   * Measures the cost when samples arrive shuffled within 10-sample windows.
   */
  @Benchmark
  def ingestAggregatingOOO(bh: Blackhole): Unit = {
    val row = oooSamples(aggIdx % numSamples)
    aggPart.ingest(row.getLong(0), row, ingestBlockHolder, timeAligned, flushIntervalMillis, false)
    aggIdx += 1
    bh.consume(aggPart)
  }

  // ==========================================================================
  // Query Benchmarks
  // ==========================================================================

  /**
   * Query throughput for standard TimeSeriesPartition.
   * Iterates all rows from chunks using WriteBufferChunkScan.
   */
  @Benchmark
  def queryRegular(bh: Blackhole): Unit = {
    val iter = queryRegularPart.timeRangeRows(WriteBufferChunkScan, Array(0, 1))
    var count = 0
    while (iter.hasNext) {
      bh.consume(iter.next())
      count += 1
    }
    bh.consume(count)
  }

  /**
   * Query throughput for AggregatingRangeVector (merges finalized + active bucket data).
   * This is the realistic query path for out-of-order partitions.
   */
  @Benchmark
  def queryAggregating(bh: Blackhole): Unit = {
    val cursor = queryAggRangeVector.rows()
    try {
      var count = 0
      while (cursor.hasNext) {
        bh.consume(cursor.next())
        count += 1
      }
      bh.consume(count)
    } finally {
      cursor.close()
    }
  }

  // ==========================================================================
  // Bucket Finalization Benchmark
  // ==========================================================================

  /**
   * Measures throughput of ingesting samples that trigger bucket finalization.
   * Each invocation ingests a batch of samples spanning multiple buckets,
   * then flushes all active buckets.
   */
  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def finalizeBuckets(bh: Blackhole): Unit = {
    // Ingest 100 samples across multiple buckets (60s interval, 10s step = ~6 samples/bucket)
    val batchSize = 100
    val batchBase = baseTs + (regularIdx * batchSize) * sampleStep
    var i = 0
    while (i < batchSize) {
      val ts = batchBase + i * sampleStep
      val row = TupleRowReader((Some(ts), Some(i.toDouble)))
      finalizePart.ingest(ts, row, finalizeBlockHolder, timeAligned, flushIntervalMillis, false)
      i += 1
    }

    // Force flush all active buckets
    val flushTs = batchBase + batchSize * sampleStep + 100000L
    finalizePart.flushAllBuckets(flushTs, finalizeBlockHolder, timeAligned, flushIntervalMillis, true)

    regularIdx += 1
    bh.consume(finalizePart)
  }

  @TearDown(JMHLevel.Trial)
  def tearDown(): Unit = {
    memFactory.shutdown()
  }
}

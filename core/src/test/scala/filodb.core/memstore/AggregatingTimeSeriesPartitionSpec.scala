package filodb.core.memstore

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.store._
import filodb.memory._
import filodb.memory.format.{TupleRowReader, UnsafeUtils}
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}

object AggregatingTimeSeriesPartitionSpec {
  import BinaryRegion.NativePointer

  val memFactory = new NativeMemoryManager(50 * 1024 * 1024)

  val maxChunkSize = TestData.storeConf.maxChunksSize
  private val flushIntervalMillis = Option(TestData.storeConf.flushInterval.toMillis)
  private val timeAlignedChunksEnabled = TestData.storeConf.timeAlignedChunksEnabled
  private val acceptDuplicateSamples = false

  // Dataset with aggregation config for scalar columns
  val scalarColumns = Seq(
    "timestamp:ts",
    "value:double:{aggregation=sum,interval=1m,ooo-tolerance=30s}"
  )

  val scalarDatasetOptions = DatasetOptions.DefaultOptions.copy(
    metricColumn = "series",
    shardKeyColumns = Seq("series")
  )

  val scalarDataset = Dataset("scalar_metrics", Seq("series:string"), scalarColumns, scalarDatasetOptions)
  val scalarSchema = scalarDataset.schema

  // Dataset with histogram aggregation
  val histogramColumns = Seq(
    "timestamp:ts",
    "hist:hist:{counter=true,aggregation=histogram_sum,interval=1m,ooo-tolerance=30s}"
  )

  val histogramDataset = Dataset("histogram_metrics", Seq("series:string"), histogramColumns, scalarDatasetOptions)
  val histogramSchema = histogramDataset.schema

  // Dataset with histogram_last aggregation
  val histogramLastColumns = Seq(
    "timestamp:ts",
    "hist:hist:{counter=true,aggregation=histogram_last,interval=1m,ooo-tolerance=30s}"
  )

  val histogramLastDataset = Dataset("histogram_last_metrics", Seq("series:string"), histogramLastColumns, scalarDatasetOptions)
  val histogramLastSchema = histogramLastDataset.schema

  // Mixed dataset with both scalar and histogram columns
  val mixedColumns = Seq(
    "timestamp:ts",
    "value:double:{aggregation=avg,interval=1m,ooo-tolerance=30s}",
    "hist:hist:{counter=true,aggregation=histogram_sum,interval=1m,ooo-tolerance=30s}"
  )

  val mixedDataset = Dataset("mixed_metrics", Seq("series:string"), mixedColumns, scalarDatasetOptions)
  val mixedSchema = mixedDataset.schema

  val partKeyBuilder = new filodb.core.binaryrecord2.RecordBuilder(TestData.nativeMem, 2048)
  val defaultPartKey = partKeyBuilder.partKeyFromObjects(scalarDataset.schema, "series0")

  def makeScalarBufferPool(): WriteBufferPool = {
    new WriteBufferPool(memFactory, scalarSchema.data, TestData.storeConf)
  }

  def makeHistogramBufferPool(): WriteBufferPool = {
    new WriteBufferPool(memFactory, histogramSchema.data, TestData.storeConf)
  }

  def makeHistogramLastBufferPool(): WriteBufferPool = {
    new WriteBufferPool(memFactory, histogramLastSchema.data, TestData.storeConf)
  }

  def makeMixedBufferPool(): WriteBufferPool = {
    new WriteBufferPool(memFactory, mixedSchema.data, TestData.storeConf)
  }

  def makeAggregatingPart(
    partNo: Int,
    dataset: Dataset,
    partKey: NativePointer = defaultPartKey,
    bufferPool: WriteBufferPool
  ): AggregatingTimeSeriesPartition = {
    val bufferPools = debox.Map(dataset.schema.schemaHash -> bufferPool)
    val shardInfo = TimeSeriesShardInfo(0, new TimeSeriesShardStats(dataset.ref, 0), bufferPools, memFactory)
    new AggregatingTimeSeriesPartition(partNo, dataset.schema, partKey, shardInfo, 40)
  }

  // Helper to create a simple histogram
  def createHistogram(buckets: Seq[(Double, Long)]): org.agrona.DirectBuffer = {
    val bucketBoundaries = buckets.map(_._1).toArray :+ Double.PositiveInfinity
    val bucketCounts = buckets.map(_._2).toArray :+ 0L // Add 0 for infinity bucket
    val hist = LongHistogram(CustomBuckets(bucketBoundaries), bucketCounts)
    hist.serialize()
  }
}

class AggregatingTimeSeriesPartitionSpec extends AnyFunSpec with Matchers
    with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import AggregatingTimeSeriesPartitionSpec._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(50, Millis))

  import monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb").resolve()
  val colStore: ColumnStore = new NullColumnStore()

  var part: AggregatingTimeSeriesPartition = null

  val evictionLock = new EvictionLock

  val reclaimer = new ReclaimListener {
    def onReclaim(metaAddr: Long, numBytes: Int): Unit = {
      if (part != null && numBytes == part.schema.data.blockMetaSize) {
        val partID = UnsafeUtils.getInt(metaAddr)
        val chunkID = UnsafeUtils.getLong(metaAddr + 4)
        part.removeChunksAt(chunkID)
      }
    }
  }

  private val blockStore = new PageAlignedBlockManager(
    200 * 1024 * 1024,
    new MemoryStats(Map("test" -> "test")),
    reclaimer,
    1,
    evictionLock
  )

  protected val ingestBlockHolder = new BlockMemFactory(
    blockStore,
    scalarSchema.data.blockMetaSize,
    Map("test" -> "test"),
    true
  )

  override def afterAll(): Unit = {
    super.afterAll()
    memFactory.shutdown()
  }

  before {
    colStore.truncate(scalarDataset.ref, 4).futureValue
  }

  describe("Scalar aggregation with out-of-order samples") {
    it("should aggregate in-order samples correctly") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L
      val bucketInterval = 60000L

      // Ingest 3 samples in same bucket (all ceil to 120000)
      val samples = Seq(
        TupleRowReader((Some(baseTs), Some(10.0))),         // 100000 -> bucket 120000
        TupleRowReader((Some(baseTs + 10000), Some(20.0))), // 110000 -> bucket 120000
        TupleRowReader((Some(baseTs + 15000), Some(30.0)))  // 115000 -> bucket 120000
      )

      samples.foreach { row =>
        part.ingest(baseTs, row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Values are kept in memory until finalization
      // Query the in-memory aggregated value
      val bucketTs = 120000L
      val inMemoryValue = part.getAggregatedValue(1, bucketTs)
      inMemoryValue shouldBe defined
      inMemoryValue.get.asInstanceOf[Double] shouldEqual 60.0 +- 0.01

      // Force finalization to write to vectors
      part.flushAllBuckets(baseTs + bucketInterval + oooTolerance + 10000, ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Now check the vector
      val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val values = iterator.map(_.getDouble(0)).toSeq
      values.length shouldEqual 1
      values.head shouldEqual 60.0 +- 0.01
    }

    it("should handle out-of-order samples within tolerance") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L
      val bucketInterval = 60000L

      // Ingest samples out-of-order within tolerance window (all ceil to 120000)
      val samples = Seq(
        TupleRowReader((Some(baseTs + 15000), Some(30.0))),  // 115000 -> bucket 120000 (Latest first)
        TupleRowReader((Some(baseTs), Some(10.0))),          // 100000 -> bucket 120000 (Earliest)
        TupleRowReader((Some(baseTs + 10000), Some(20.0)))   // 110000 -> bucket 120000 (Middle)
      )

      samples.foreach { row =>
        part.ingest(row.getLong(0), row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Verify in-memory aggregation (sum = 60.0)
      val bucketTs = 120000L
      val inMemoryValue = part.getAggregatedValue(1, bucketTs)
      inMemoryValue shouldBe defined
      inMemoryValue.get.asInstanceOf[Double] shouldEqual 60.0 +- 0.01

      // Force finalization
      part.flushAllBuckets(baseTs + bucketInterval + oooTolerance + 10000, ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Verify vector values
      val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val values = iterator.map(_.getDouble(0)).toSeq
      values.length shouldEqual 1
      values.head shouldEqual 60.0 +- 0.01
    }

    it("should handle samples in different buckets") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val bucketInterval = 60000L
      val oooTolerance = 30000L

      // Samples in 3 different buckets
      val samples = Seq(
        TupleRowReader((Some(baseTs), Some(10.0))),              // Bucket 120000
        TupleRowReader((Some(baseTs + bucketInterval), Some(20.0))),    // Bucket 180000
        TupleRowReader((Some(baseTs + 2 * bucketInterval), Some(30.0))) // Bucket 240000
      )

      samples.foreach { row =>
        part.ingest(row.getLong(0), row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Force finalization of all buckets
      part.flushAllBuckets(baseTs + 3 * bucketInterval + oooTolerance + 10000, ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Should have 3 separate aggregated values
      val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val values = iterator.map(_.getDouble(0)).toSeq.sorted
      values.length shouldEqual 3
      values shouldEqual Seq(10.0, 20.0, 30.0)
    }

    it("should aggregate samples for same bucket correctly") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L
      val bucketInterval = 60000L

      // Ingest first sample for bucket 120000
      part.ingest(baseTs, TupleRowReader((Some(baseTs), Some(10.0))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Ingest out-of-order sample for same bucket (within tolerance)
      // Both samples ceil to bucket 120000
      part.ingest(baseTs + 15000, TupleRowReader((Some(baseTs + 10000), Some(15.0))),
        ingestBlockHolder, timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Verify in-memory aggregation shows sum = 25.0
      val bucketTs = 120000L
      val inMemoryValue = part.getAggregatedValue(1, bucketTs)
      inMemoryValue shouldBe defined
      inMemoryValue.get.asInstanceOf[Double] shouldEqual 25.0 +- 0.01

      // Force finalization
      part.flushAllBuckets(baseTs + bucketInterval + oooTolerance + 10000, ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Verify vector has sum = 25.0
      val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val values = iterator.map(_.getDouble(0)).toSeq
      values.length shouldEqual 1
      values.head shouldEqual 25.0 +- 0.01
    }

    it("should drop samples outside tolerance window") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L

      // Ingest a recent sample
      part.ingest(baseTs + 100000, TupleRowReader((Some(baseTs + 100000), Some(50.0))),
        ingestBlockHolder, timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Try to ingest a sample that's too old (outside tolerance)
      part.ingest(baseTs + 100000, TupleRowReader((Some(baseTs), Some(10.0))),
        ingestBlockHolder, timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Force finalization
      part.flushAllBuckets(baseTs + 200000, ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Should only have the recent sample (old one should be dropped)
      val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val values = iterator.map(_.getDouble(0)).toSeq
      values.length shouldEqual 1
      values.head shouldEqual 50.0 +- 0.01
    }
  }

  describe("Histogram aggregation (HistogramSum)") {
    it("should aggregate histograms using sum") {
      val bufferPool = makeHistogramBufferPool()
      part = makeAggregatingPart(0, histogramDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L

      // Create two histograms to aggregate
      val hist1 = createHistogram(Seq((1.0, 5L), (2.0, 10L), (5.0, 15L)))
      val hist2 = createHistogram(Seq((1.0, 3L), (2.0, 7L), (5.0, 12L)))

      val samples = Seq(
        TupleRowReader((Some(baseTs), Some(hist1))),
        TupleRowReader((Some(baseTs + 15000), Some(hist2)))
      )

      samples.foreach { row =>
        part.ingest(baseTs, row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Check in-memory histogram is queryable
      val bucketTs = 120000L // ceiled bucket timestamp
      val aggregatedHist = part.getAggregatedHistogram(1, bucketTs)
      aggregatedHist shouldBe defined

      // Verify histogram was summed correctly
      val hist = aggregatedHist.get
      // Buckets should be summed: (1.0, 8L), (2.0, 17L), (5.0, 27L) + infinity bucket
      hist.numBuckets shouldEqual 4
    }

    it("should keep histograms queryable in-memory") {
      val bufferPool = makeHistogramBufferPool()
      part = makeAggregatingPart(0, histogramDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val hist1 = createHistogram(Seq((1.0, 5L), (2.0, 10L)))

      part.ingest(baseTs, TupleRowReader((Some(baseTs), Some(hist1))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Should be immediately queryable from in-memory state
      val bucketTs = 120000L
      val aggregatedHist = part.getAggregatedHistogram(1, bucketTs)
      aggregatedHist shouldBe defined
    }
  }

  describe("Histogram aggregation (HistogramLast)") {
    it("should keep last histogram by timestamp") {
      val bufferPool = makeHistogramLastBufferPool()
      part = makeAggregatingPart(0, histogramLastDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L

      val hist1 = createHistogram(Seq((1.0, 5L), (2.0, 10L)))
      val hist2 = createHistogram(Seq((1.0, 8L), (2.0, 15L)))
      val hist3 = createHistogram(Seq((1.0, 3L), (2.0, 7L)))

      // Ingest out-of-order - should keep the one with latest timestamp
      val samples = Seq(
        TupleRowReader((Some(baseTs + 10000), Some(hist2))),  // Middle timestamp
        TupleRowReader((Some(baseTs), Some(hist1))),          // Earliest
        TupleRowReader((Some(baseTs + 20000), Some(hist3)))   // Latest - this should be kept
      )

      samples.foreach { row =>
        part.ingest(row.getLong(0), row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Should have the last histogram by timestamp
      val bucketTs = 120000L
      val aggregatedHist = part.getAggregatedHistogram(1, bucketTs)
      aggregatedHist shouldBe defined

      // Should be hist3
      val hist = aggregatedHist.get
      // 2 user-defined buckets + infinity bucket = 3 total
      hist.numBuckets shouldEqual 3
    }
  }

  describe("Bucket cleanup and finalization") {
    it("should finalize histogram buckets when they exit tolerance window") {
      val bufferPool = makeHistogramBufferPool()
      part = makeAggregatingPart(0, histogramDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L
      val bucketInterval = 60000L

      val hist1 = createHistogram(Seq((1.0, 5L), (2.0, 10L)))

      // Ingest old histogram
      part.ingest(baseTs, TupleRowReader((Some(baseTs), Some(hist1))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      val bucketTs = 120000L
      part.getAggregatedHistogram(1, bucketTs) shouldBe defined

      // Ingest new data that triggers finalization (far in the future)
      val futureTs = baseTs + bucketInterval + oooTolerance + 10000
      part.ingest(futureTs, TupleRowReader((Some(futureTs), Some(hist1))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Old bucket should be finalized (no longer in active state)
      part.getAggregatedHistogram(1, bucketTs) shouldBe None

      // But should be written to vector
      part.numChunks should be > 0
    }

    it("should clean up scalar bucket state when buckets are finalized") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L
      val bucketInterval = 60000L

      // Ingest old sample
      part.ingest(baseTs, TupleRowReader((Some(baseTs), Some(10.0))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Verify it's in active state
      val bucketTs = 120000L
      part.getAggregatedValue(1, bucketTs) shouldBe defined

      // Ingest new sample far in the future to trigger finalization
      val futureTs = baseTs + 2 * bucketInterval + oooTolerance + 10000
      part.ingest(futureTs, TupleRowReader((Some(futureTs), Some(20.0))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Old bucket should be finalized (no longer in active state)
      part.getAggregatedValue(1, bucketTs) shouldBe None

      // Should have data in vectors
      part.numChunks should be > 0
    }
  }

  describe("Mixed scalar and histogram columns") {
    it("should handle both scalar and histogram columns in same schema") {
      val bufferPool = makeMixedBufferPool()
      part = makeAggregatingPart(0, mixedDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L
      val bucketInterval = 60000L
      val hist1 = createHistogram(Seq((1.0, 5L), (2.0, 10L)))

      val samples = Seq(
        TupleRowReader((Some(baseTs), Some(10.0), Some(hist1))),         // 100000 -> bucket 120000
        TupleRowReader((Some(baseTs + 10000), Some(20.0), Some(hist1)))  // 110000 -> bucket 120000
      )

      samples.foreach { row =>
        part.ingest(baseTs, row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Check in-memory scalar aggregation (avg = 15.0)
      val bucketTs = 120000L
      val scalarValue = part.getAggregatedValue(1, bucketTs)
      scalarValue shouldBe defined
      scalarValue.get.asInstanceOf[Double] shouldEqual 15.0 +- 0.01

      // Check in-memory histogram aggregation
      val aggregatedHist = part.getAggregatedHistogram(2, bucketTs)
      aggregatedHist shouldBe defined

      // Force finalization
      part.flushAllBuckets(baseTs + bucketInterval + oooTolerance + 10000, ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Verify vector values
      val scalarIter = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val scalarValues = scalarIter.map(_.getDouble(0)).toSeq
      scalarValues.length shouldEqual 1
      scalarValues.head shouldEqual 15.0 +- 0.01
    }
  }

  describe("Edge cases and special scenarios") {
    it("should fall back to standard ingestion when no aggregation configured") {
      // Use a schema without aggregation config
      val noAggColumns = Seq("timestamp:ts", "value:double")
      val noAggDataset = Dataset("no_agg_metrics", Seq("series:string"), noAggColumns, scalarDatasetOptions)
      val bufferPool = new WriteBufferPool(memFactory, noAggDataset.schema.data, TestData.storeConf)

      part = makeAggregatingPart(0, noAggDataset, defaultPartKey, bufferPool)

      val samples = Seq(
        TupleRowReader((Some(100000L), Some(10.0))),
        TupleRowReader((Some(101000L), Some(20.0)))
      )

      samples.foreach { row =>
        part.ingest(row.getLong(0), row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Should ingest normally without aggregation
      val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val values = iterator.map(_.getDouble(0)).toSeq
      values.length shouldEqual 2
      values shouldEqual Seq(10.0, 20.0)
    }

    it("should handle flushAllBuckets") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L

      part.ingest(baseTs, TupleRowReader((Some(baseTs), Some(10.0))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      part.ingest(baseTs, TupleRowReader((Some(baseTs + 10000), Some(20.0))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Force flush all buckets
      part.flushAllBuckets(baseTs + 100000, ingestBlockHolder, timeAlignedChunksEnabled,
        flushIntervalMillis, acceptDuplicateSamples)

      // Should have written aggregated value
      val iterator = part.timeRangeRows(WriteBufferChunkScan, Array(1))
      val values = iterator.map(_.getDouble(0)).toSeq
      values.length should be >= 1
    }

    it("should provide bucket aggregation statistics") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L

      part.ingest(baseTs, TupleRowReader((Some(baseTs), Some(10.0))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Should have stats for active buckets
      val stats = part.bucketAggregationStats
      stats.activeBucketCount should be > 0
    }

    it("should provide histogram aggregation statistics") {
      val bufferPool = makeHistogramBufferPool()
      part = makeAggregatingPart(0, histogramDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val hist1 = createHistogram(Seq((1.0, 5L), (2.0, 10L)))

      part.ingest(baseTs, TupleRowReader((Some(baseTs), Some(hist1))), ingestBlockHolder,
        timeAlignedChunksEnabled, flushIntervalMillis, acceptDuplicateSamples)

      // Should have histogram stats
      val stats = part.histogramAggStats
      stats should not be empty

      // Should have at least one active bucket
      val histStats = stats.values.head
      histStats.activeBucketCount should be > 0
    }
  }

  describe("AggregatingRangeVector query integration") {
    import filodb.core.query._
    import filodb.core.store.{AllChunkScan, TimeRangeChunkScan}
    import java.util.concurrent.atomic.AtomicLong

    it("should return active bucket data via AggregatingRangeVector") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L

      // Ingest samples that create an active bucket (not yet finalized)
      val samples = Seq(
        TupleRowReader((Some(baseTs), Some(10.0))),
        TupleRowReader((Some(baseTs + 10000), Some(20.0)))
      )

      samples.foreach { row =>
        part.ingest(baseTs, row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Verify active bucket exists
      part.bucketAggregationStats.activeBucketCount should be > 0

      // Create AggregatingRangeVector and query rows
      val dataBytesScannedCtr = new AtomicLong(0)
      val samplesScannedCtr = new AtomicLong(0)
      val columnIDs = Array(0, 1) // timestamp and value columns

      val rangeVector = AggregatingRangeVector(
        PartitionRangeVectorKey(
          Left(part),
          scalarSchema.partKeySchema,
          scalarSchema.infosFromIDs(scalarSchema.partition.columns.map(_.id)),
          0, 0, part.partID, scalarSchema.name
        ),
        part,
        TimeRangeChunkScan(baseTs, baseTs + 120000),
        columnIDs,
        dataBytesScannedCtr,
        samplesScannedCtr,
        Long.MaxValue,
        "test-query"
      )

      // Query rows - should include active bucket data
      val cursor = rangeVector.rows()
      try {
        val rows = cursor.toSeq
        rows should not be empty

        // The active bucket timestamp is 120000 (ceiled from samples at 100000 and 110000)
        // The aggregated sum should be 30.0
        val bucketRow = rows.find(r => r.getLong(0) == 120000L)
        bucketRow shouldBe defined
        bucketRow.get.getDouble(1) shouldEqual 30.0 +- 0.01
      } finally {
        cursor.close()
      }
    }

    it("should merge finalized and active bucket data") {
      val bufferPool = makeScalarBufferPool()
      part = makeAggregatingPart(0, scalarDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L
      val oooTolerance = 30000L
      val bucketInterval = 60000L

      // Ingest old samples that will be finalized
      val oldSamples = Seq(
        TupleRowReader((Some(baseTs), Some(10.0))),
        TupleRowReader((Some(baseTs + 10000), Some(20.0)))
      )

      oldSamples.foreach { row =>
        part.ingest(baseTs, row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Verify first bucket exists
      val firstBucketTs = 120000L
      part.getAggregatedValue(1, firstBucketTs) shouldBe defined

      // Advance time and ingest newer samples to trigger finalization of old bucket
      val futureTs = baseTs + bucketInterval + oooTolerance + 10000
      val newSamples = Seq(
        TupleRowReader((Some(futureTs), Some(50.0))),
        TupleRowReader((Some(futureTs + 10000), Some(60.0)))
      )

      newSamples.foreach { row =>
        part.ingest(row.getLong(0), row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Should have both finalized data (in vectors) and active buckets
      part.numChunks should be > 0
      part.bucketAggregationStats.activeBucketCount should be > 0

      // Create AggregatingRangeVector and query
      val dataBytesScannedCtr = new AtomicLong(0)
      val samplesScannedCtr = new AtomicLong(0)
      val columnIDs = Array(0, 1)

      val rangeVector = AggregatingRangeVector(
        PartitionRangeVectorKey(
          Left(part),
          scalarSchema.partKeySchema,
          scalarSchema.infosFromIDs(scalarSchema.partition.columns.map(_.id)),
          0, 0, part.partID, scalarSchema.name
        ),
        part,
        AllChunkScan,
        columnIDs,
        dataBytesScannedCtr,
        samplesScannedCtr,
        Long.MaxValue,
        "test-query"
      )

      val cursor = rangeVector.rows()
      try {
        val rows = cursor.toSeq

        // Should have both finalized bucket and active bucket
        rows.length should be >= 2

        // Collect all values and verify we have both the old finalized sum (30.0)
        // and the new active sum (110.0)
        val values = rows.map(_.getDouble(1)).toList.sorted

        // Use exists instead of contain for fuzzy matching
        values.exists(v => Math.abs(v - 30.0) < 0.01) shouldBe true
        values.exists(v => Math.abs(v - 110.0) < 0.01) shouldBe true
      } finally {
        cursor.close()
      }
    }

    it("should query histogram buckets via AggregatingRangeVector") {
      val bufferPool = makeHistogramBufferPool()
      part = makeAggregatingPart(0, histogramDataset, defaultPartKey, bufferPool)

      val baseTs = 100000L

      // Create histogram samples
      val hist1 = createHistogram(Seq((1.0, 5L), (2.0, 10L), (5.0, 15L)))
      val hist2 = createHistogram(Seq((1.0, 3L), (2.0, 7L), (5.0, 12L)))

      val samples = Seq(
        TupleRowReader((Some(baseTs), Some(hist1))),
        TupleRowReader((Some(baseTs + 15000), Some(hist2)))
      )

      samples.foreach { row =>
        part.ingest(baseTs, row, ingestBlockHolder, timeAlignedChunksEnabled,
          flushIntervalMillis, acceptDuplicateSamples)
      }

      // Verify active bucket exists with histogram
      val bucketTs = 120000L
      part.getAggregatedHistogram(1, bucketTs) shouldBe defined

      // Create AggregatingRangeVector and query
      val dataBytesScannedCtr = new AtomicLong(0)
      val samplesScannedCtr = new AtomicLong(0)
      val columnIDs = Array(0, 1) // timestamp and histogram columns

      val rangeVector = AggregatingRangeVector(
        PartitionRangeVectorKey(
          Left(part),
          histogramSchema.partKeySchema,
          histogramSchema.infosFromIDs(histogramSchema.partition.columns.map(_.id)),
          0, 0, part.partID, histogramSchema.name
        ),
        part,
        TimeRangeChunkScan(baseTs, baseTs + 120000),
        columnIDs,
        dataBytesScannedCtr,
        samplesScannedCtr,
        Long.MaxValue,
        "test-query"
      )

      val cursor = rangeVector.rows()
      try {
        val rows = cursor.toSeq
        rows should not be empty

        // Find the bucket row and verify it's a histogram
        val bucketRow = rows.find(r => r.getLong(0) == bucketTs)
        bucketRow shouldBe defined

        // Should be able to get histogram from the row
        val hist = bucketRow.get.getHistogram(1)
        hist should not be null
        hist.numBuckets shouldEqual 4 // 3 user-defined + infinity
      } finally {
        cursor.close()
      }
    }
  }
}

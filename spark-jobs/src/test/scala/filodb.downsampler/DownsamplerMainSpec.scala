package filodb.downsampler

import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.GlobalScheduler._
import filodb.core.MachineMetricsData
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder}
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{CustomRangeVectorKey, RawDataRangeVector}
import filodb.core.store.{AllChunkScan, SinglePartitionScan, StoreConfig}
import filodb.downsampler.BatchDownsampler.shardStats
import filodb.memory.format.PrimitiveVectorReader
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.vectors.{CustomBuckets, MutableHistogram}

/**
  * Spec tests downsampling round trip.
  * Each item in the spec depends on the previous step. Hence entire spec needs to be run in order.
  */
class DownsamplerMainSpec extends FunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  System.setProperty("config.file", "conf/timeseries-filodb-server.conf")

  val colStore = BatchDownsampler.cassandraColStore

  val storeConfig = StoreConfig(ConfigFactory.parseString( """
                  |flush-interval = 1h
                  |shard-mem-size = 1MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram),
                                     Map.empty, 100, storeConfig)
  var gaugePartKeyBytes: Array[Byte] = _
  var counterPartKeyBytes: Array[Byte] = _
  var histPartKeyBytes: Array[Byte] = _
  val lastSampleTime = 1574273042000L
  val downsampler = new Downsampler

  override def beforeAll(): Unit = {
    BatchDownsampler.downsampleDatasetRefs.values.foreach { ds =>
      colStore.initialize(ds, 4).futureValue
      colStore.truncate(ds, 4).futureValue

    }
    colStore.initialize(BatchDownsampler.rawDatasetRef, 4).futureValue
    colStore.truncate(BatchDownsampler.rawDatasetRef, 4).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
    downsampler.shutdown()
  }

  it ("should write gauge data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val seriesName = "myGauge"
    val seriesTags = Map("k".utf8 -> "v".utf8)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, seriesName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey,
      0, offheapMem.bufferPools(Schemas.gauge.schemaHash), shardStats,
      offheapMem.nativeMemoryManager, 1)

    gaugePartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(1574272801000L, 3d, seriesName, seriesTags),
      Seq(1574272802000L, 5d, seriesName, seriesTags),

      Seq(1574272861000L, 9d, seriesName, seriesTags),
      Seq(1574272862000L, 11d, seriesName, seriesTags),

      Seq(1574272921000L, 13d, seriesName, seriesTags),
      Seq(1574272922000L, 15d, seriesName, seriesTags),

      Seq(1574272981000L, 17d, seriesName, seriesTags),
      Seq(1574272982000L, 15d, seriesName, seriesTags),

      Seq(1574273041000L, 13d, seriesName, seriesTags),
      Seq(1574273042000L, 11d, seriesName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    colStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
  }

  it ("should write prom counter data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promCounter)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val seriesName = "myCounter"
    val seriesTags = Map("k".utf8 -> "v".utf8)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, seriesName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promCounter, partKey,
      0, offheapMem.bufferPools(Schemas.promCounter.schemaHash), shardStats,
      offheapMem.nativeMemoryManager, 1)

    counterPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(1574272801000L, 3d, seriesName, seriesTags),
      Seq(1574272801500L, 4d, seriesName, seriesTags),
      Seq(1574272802000L, 5d, seriesName, seriesTags),

      Seq(1574272861000L, 9d, seriesName, seriesTags),
      Seq(1574272861500L, 10d, seriesName, seriesTags),
      Seq(1574272862000L, 11d, seriesName, seriesTags),

      Seq(1574272921000L, 2d, seriesName, seriesTags),
      Seq(1574272921500L, 7d, seriesName, seriesTags),
      Seq(1574272922000L, 15d, seriesName, seriesTags),

      Seq(1574272981000L, 17d, seriesName, seriesTags),
      Seq(1574272981500L, 1d, seriesName, seriesTags),
      Seq(1574272982000L, 15d, seriesName, seriesTags),

      Seq(1574273041000L, 18d, seriesName, seriesTags),
      Seq(1574273042000L, 20d, seriesName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promCounter.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    colStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
  }

  it ("should write prom histogram data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val seriesName = "myHistogram"
    val seriesTags = Map("k".utf8 -> "v".utf8)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promHistogram, seriesName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promHistogram, partKey,
      0, offheapMem.bufferPools(Schemas.promHistogram.schemaHash), shardStats,
      offheapMem.nativeMemoryManager, 1)

    histPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(1574272801000L, 0d, 1d, MutableHistogram(bucketScheme, Array(0d, 0d, 1d)), seriesName, seriesTags),
      Seq(1574272801500L, 2d, 3d, MutableHistogram(bucketScheme, Array(0d, 2d, 3d)), seriesName, seriesTags),
      Seq(1574272802000L, 5d, 6d, MutableHistogram(bucketScheme, Array(2d, 5d, 6d)), seriesName, seriesTags),

      Seq(1574272861000L, 9d, 9d, MutableHistogram(bucketScheme, Array(2d, 5d, 9d)), seriesName, seriesTags),
      Seq(1574272861500L, 10d, 10d, MutableHistogram(bucketScheme, Array(2d, 5d, 10d)), seriesName, seriesTags),
      Seq(1574272862000L, 11d, 14d, MutableHistogram(bucketScheme, Array(2d, 8d, 14d)), seriesName, seriesTags),

      Seq(1574272921000L, 2d, 2d, MutableHistogram(bucketScheme, Array(0d, 0d, 2d)), seriesName, seriesTags),
      Seq(1574272921500L, 7d, 9d, MutableHistogram(bucketScheme, Array(1d, 7d, 9d)), seriesName, seriesTags),
      Seq(1574272922000L, 15d, 19d, MutableHistogram(bucketScheme, Array(1d, 15d, 19d)), seriesName, seriesTags),

      Seq(1574272981000L, 17d, 21d, MutableHistogram(bucketScheme, Array(2d, 16d, 21d)), seriesName, seriesTags),
      Seq(1574272981500L, 1d, 1d, MutableHistogram(bucketScheme, Array(0d, 1d, 1d)), seriesName, seriesTags),
      Seq(1574272982000L, 15d, 15d, MutableHistogram(bucketScheme, Array(0d, 15d, 15d)), seriesName, seriesTags),

      Seq(1574273041000L, 18d, 19d, MutableHistogram(bucketScheme, Array(1d, 16d, 19d)), seriesName, seriesTags),
      Seq(1574273042000L, 20d, 25d, MutableHistogram(bucketScheme, Array(4d, 20d, 25d)), seriesName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promHistogram.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    colStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
  }

  it ("should free all offheap memory") {
    offheapMem.free()
  }

  it ("should downsample raw data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.userTimeOverride", lastSampleTime.toString)
    downsampler.run(sparkConf)
  }

  it ("should migrate partKey data into the downsample dataset tables in cassandra using spark job") {
    // TODO in future PR
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(gaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual gaugePartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (1574272802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (1574272862000L, 9.0, 11.0, 20.0, 2.0, 10.0),
      (1574272922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (1574272982000L, 15.0, 17.0, 32.0, 2.0, 16.0),
      (1574273042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (1574272801000L, 3d),
      (1574272802000L, 5d),

      (1574272862000L, 11d),

      (1574272921000L, 2d),
      (1574272922000L, 15d),

      (1574272981000L, 17d),
      (1574272981500L, 1d),
      (1574272982000L, 15d),

      (1574273042000L, 20d)

    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual histPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3))

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    // time, sum, count, histogram
    downsampledData1 shouldEqual Seq(
      (1574272801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (1574272802000L, 5d, 6d, Seq(2d, 5d, 6d)),

      (1574272862000L, 11d, 14d, Seq(2d, 8d, 14d)),

      (1574272921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (1574272922000L, 15d, 19d, Seq(1d, 15d, 19d)),

      (1574272981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (1574272981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (1574272982000L, 15d, 15d, Seq(0d, 15d, 15d)),

      (1574273042000L, 20d, 25d, Seq(4d, 20d, 25d))
    )
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 5-min downsampled data") {
    val downsampledPartData2 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(gaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart2 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0, downsampledPartData2)

    downsampledPart2.partKeyBytes shouldEqual gaugePartKeyBytes

    val rv2 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart2, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData2 = rv2.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData2 shouldEqual Seq(
      (1574273042000L, 3.0, 17.0, 112.0, 10.0, 11.2)
    )
  }


  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1))

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (1574272801000L, 3d),

      (1574272862000L, 11d),

      (1574272921000L, 2d),

      (1574272981000L, 17d),
      (1574272981500L, 1d),

      (1574273042000L, 20d)
    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual histPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(2), ctrChunkInfo.vectorAddress(2)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3))

    val bucketScheme = Seq(3d, 10d, Double.PositiveInfinity)
    val downsampledData1 = rv1.rows.map { r =>
      val h = r.getHistogram(3)
      h.numBuckets shouldEqual 3
      (0 until h.numBuckets).map(i => h.bucketTop(i)) shouldEqual bucketScheme
      (r.getLong(0), r.getDouble(1), r.getDouble(2), (0 until h.numBuckets).map(i => h.bucketValue(i)))
    }.toList

    // time, sum, count, histogram
    downsampledData1 shouldEqual Seq(
      (1574272801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (1574272862000L, 11d, 14d, Seq(2d, 8d, 14d)),
      (1574272921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (1574272981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (1574272981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (1574273042000L, 20d, 25d, Seq(4d, 20d, 25d))
    )

  }

  it("should read and verify part key migration in cassandra for 5-min downsampled data") {
    // TODO in future PR
  }
}

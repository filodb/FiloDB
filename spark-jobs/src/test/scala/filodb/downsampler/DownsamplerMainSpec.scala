package filodb.downsampler

import java.io.File

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.GlobalScheduler._
import filodb.core.MachineMetricsData
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder, RecordSchema}
import filodb.core.downsample.DownsampledTimeSeriesStore
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition}
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query.{ColumnFilter, CustomRangeVectorKey, QueryContext, RawDataRangeVector}
import filodb.core.query.Filter.Equals
import filodb.core.store.{AllChunkScan, PartKeyRecord, SinglePartitionScan, StoreConfig}
import filodb.downsampler.chunk.{BatchDownsampler, Downsampler, DownsamplerSettings, OffHeapMemory}
import filodb.downsampler.index.{DSIndexJobSettings, IndexJobDriver}
import filodb.memory.format.{PrimitiveVectorReader, UnsafeUtils}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}
import filodb.query.{QueryConfig, QueryResult}
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}

/**
  * Spec tests downsampling round trip.
  * Each item in the spec depends on the previous step. Hence entire spec needs to be run in order.
  */
class DownsamplerMainSpec extends FunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val conf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf"))

  val settings = new DownsamplerSettings(conf)
  val dsIndexJobSettings = new DSIndexJobSettings(settings)
  val batchDownsampler = new BatchDownsampler(settings)

  val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)

  val rawColStore = batchDownsampler.rawCassandraColStore
  val downsampleColStore = batchDownsampler.downsampleCassandraColStore

  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString( """
                  |flush-interval = 1h
                  |shard-mem-size = 1MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram),
                                     Map.empty, 100, rawDataStoreConfig)
  val gaugeName = "my_gauge"
  var gaugePartKeyBytes: Array[Byte] = _

  val counterName = "my_counter"
  var counterPartKeyBytes: Array[Byte] = _

  val histName = "my_histogram"
  var histPartKeyBytes: Array[Byte] = _

  val gaugeLowFreqName = "my_gauge_low_freq"
  var gaugeLowFreqPartKeyBytes: Array[Byte] = _

  val lastSampleTime = 1574373042000L
  val downsampler = new Downsampler(settings, batchDownsampler)

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

//  //Index migration job, runs for current 2hours for testing. actual job migrates last 6 hour's index updates
  val currentHour = hour()
  val indexUpdater = new IndexJobDriver(currentHour - 2, currentHour, settings, dsIndexJobSettings)

  def pkMetricSchemaReader(pkr: PartKeyRecord): (String, String) = {
    val schemaId = RecordSchema.schemaID(pkr.partKey, UnsafeUtils.arayOffset)
    val partSchema = batchDownsampler.schemas(schemaId)
    val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
    (strPairs.filter(p => p._1 == "_metric_").head._2, partSchema.data.name)
  }

  override def beforeAll(): Unit = {
    batchDownsampler.downsampleRefsByRes.values.foreach { ds =>
      downsampleColStore.initialize(ds, 4).futureValue
      downsampleColStore.truncate(ds, 4).futureValue
    }
    rawColStore.initialize(batchDownsampler.rawDatasetRef, 4).futureValue
    rawColStore.truncate(batchDownsampler.rawDatasetRef, 4).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }

  it ("should write gauge data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey,
      0, offheapMem.bufferPools(Schemas.gauge.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    gaugePartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(1574372801000L, 3d, gaugeName, seriesTags),
      Seq(1574372802000L, 5d, gaugeName, seriesTags),

      Seq(1574372861000L, 9d, gaugeName, seriesTags),
      Seq(1574372862000L, 11d, gaugeName, seriesTags),

      Seq(1574372921000L, 13d, gaugeName, seriesTags),
      Seq(1574372922000L, 15d, gaugeName, seriesTags),

      Seq(1574372981000L, 17d, gaugeName, seriesTags),
      Seq(1574372982000L, 15d, gaugeName, seriesTags),

      Seq(1574373041000L, 13d, gaugeName, seriesTags),
      Seq(1574373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(gaugePartKeyBytes, 1574372801000L, 1574373042000L, Some(150))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200).futureValue
  }

  it ("should write low freq gauge data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeLowFreqName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey,
      0, offheapMem.bufferPools(Schemas.gauge.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    gaugeLowFreqPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(1574372801000L, 3d, gaugeName, seriesTags),
      Seq(1574372802000L, 5d, gaugeName, seriesTags),

      // skip next minute

      Seq(1574372921000L, 13d, gaugeName, seriesTags),
      Seq(1574372922000L, 15d, gaugeName, seriesTags),

      // skip next minute

      Seq(1574373041000L, 13d, gaugeName, seriesTags),
      Seq(1574373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(gaugeLowFreqPartKeyBytes, 1574372801000L, 1574373042000L, Some(150))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200).futureValue
  }

  it ("should write prom counter data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promCounter)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promCounter, counterName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promCounter, partKey,
      0, offheapMem.bufferPools(Schemas.promCounter.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    counterPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(1574372801000L, 3d, counterName, seriesTags),
      Seq(1574372801500L, 4d, counterName, seriesTags),
      Seq(1574372802000L, 5d, counterName, seriesTags),

      Seq(1574372861000L, 9d, counterName, seriesTags),
      Seq(1574372861500L, 10d, counterName, seriesTags),
      Seq(1574372862000L, 11d, counterName, seriesTags),

      Seq(1574372921000L, 2d, counterName, seriesTags),
      Seq(1574372921500L, 7d, counterName, seriesTags),
      Seq(1574372922000L, 15d, counterName, seriesTags),

      Seq(1574372981000L, 17d, counterName, seriesTags),
      Seq(1574372981500L, 1d, counterName, seriesTags),
      Seq(1574372982000L, 15d, counterName, seriesTags),

      Seq(1574373041000L, 18d, counterName, seriesTags),
      Seq(1574373042000L, 20d, counterName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promCounter.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(counterPartKeyBytes, 1574372801000L, 1574373042000L, Some(1))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200).futureValue
  }

  it ("should write prom histogram data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.promHistogram, histName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.promHistogram, partKey,
      0, offheapMem.bufferPools(Schemas.promHistogram.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    histPartKeyBytes = part.partKeyBytes

    val bucketScheme = CustomBuckets(Array(3d, 10d, Double.PositiveInfinity))
    val rawSamples = Stream( // time, sum, count, hist, name, tags
      Seq(1574372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histName, seriesTags),
      Seq(1574372801500L, 2d, 3d, LongHistogram(bucketScheme, Array(0L, 2, 3)), histName, seriesTags),
      Seq(1574372802000L, 5d, 6d, LongHistogram(bucketScheme, Array(2L, 5, 6)), histName, seriesTags),

      Seq(1574372861000L, 9d, 9d, LongHistogram(bucketScheme,   Array(2L, 5, 9)), histName, seriesTags),
      Seq(1574372861500L, 10d, 10d, LongHistogram(bucketScheme, Array(2L, 5, 10)), histName, seriesTags),
      Seq(1574372862000L, 11d, 14d, LongHistogram(bucketScheme, Array(2L, 8, 14)), histName, seriesTags),

      Seq(1574372921000L, 2d, 2d, LongHistogram(bucketScheme,   Array(0L, 0, 2)), histName, seriesTags),
      Seq(1574372921500L, 7d, 9d, LongHistogram(bucketScheme,   Array(1L, 7, 9)), histName, seriesTags),
      Seq(1574372922000L, 15d, 19d, LongHistogram(bucketScheme, Array(1L, 15, 19)), histName, seriesTags),

      Seq(1574372981000L, 17d, 21d, LongHistogram(bucketScheme, Array(2L, 16, 21)), histName, seriesTags),
      Seq(1574372981500L, 1d, 1d, LongHistogram(bucketScheme,   Array(0L, 1, 1)), histName, seriesTags),
      Seq(1574372982000L, 15d, 15d, LongHistogram(bucketScheme, Array(0L, 15, 15)), histName, seriesTags),

      Seq(1574373041000L, 18d, 19d, LongHistogram(bucketScheme, Array(1L, 16, 19)), histName, seriesTags),
      Seq(1574373042000L, 20d, 25d, LongHistogram(bucketScheme, Array(4L, 20, 25)), histName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promHistogram.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(histPartKeyBytes, 1574372801000L, 1574373042000L, Some(199))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200).futureValue
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
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    indexUpdater.run(sparkConf)
  }

  it ("should recover migrated partKey data and match the downsampled schema") {
    val metrics = Seq((counterName, Schemas.promCounter.name),
      (gaugeName, Schemas.dsGauge.name),
      (gaugeLowFreqName, Schemas.dsGauge.name),
      (histName, Schemas.promHistogram.name))
    val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")), 0)
    val tsSchemaMetric = Await.result(partKeys.map(pkMetricSchemaReader).toListL.runAsync, 1 minutes)
    tsSchemaMetric.sorted shouldEqual metrics
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas)
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (1574372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (1574372862000L, 9.0, 11.0, 20.0, 2.0, 10.0),
      (1574372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (1574372982000L, 15.0, 17.0, 32.0, 2.0, 16.0),
      (1574373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify low freq gauge in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugeLowFreqPartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugeLowFreqPartKeyBytes,
                                                                          batchDownsampler.schemas)
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugeLowFreqPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual dsGaugeLowFreqPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (1574372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (1574372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (1574373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
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
      (1574372801000L, 3d),
      (1574372802000L, 5d),

      (1574372862000L, 11d),

      (1574372921000L, 2d),
      (1574372922000L, 15d),

      (1574372981000L, 17d),
      (1574372981500L, 1d),
      (1574372982000L, 15d),

      (1574373042000L, 20d)

    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
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
      (1574372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (1574372802000L, 5d, 6d, Seq(2d, 5d, 6d)),

      (1574372862000L, 11d, 14d, Seq(2d, 8d, 14d)),

      (1574372921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (1574372922000L, 15d, 19d, Seq(1d, 15d, 19d)),

      (1574372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (1574372981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (1574372982000L, 15d, 15d, Seq(0d, 15d, 15d)),

      (1574373042000L, 20d, 25d, Seq(4d, 20d, 25d))
    )
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 5-min downsampled data") {
    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas)
    val downsampledPartData2 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart2 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0, downsampledPartData2)

    downsampledPart2.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv2 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart2, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData2 = rv2.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData2 shouldEqual Seq(
      (1574372982000L, 3.0, 17.0, 88.0, 8.0, 11.0),
      (1574373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
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
      (1574372801000L, 3d),

      (1574372862000L, 11d),

      (1574372921000L, 2d),

      (1574372981000L, 17d),
      (1574372981500L, 1d),

      (1574372982000L, 15.0d),

      (1574373042000L, 20.0d)
    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
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
      (1574372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (1574372862000L, 11d, 14d, Seq(2d, 8d, 14d)),
      (1574372921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (1574372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (1574372981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (1574372982000L, 15.0d, 15.0d, Seq(0.0, 15.0, 15.0)),
      (1574373042000L, 20.0d, 25.0d, Seq(4.0, 20.0, 25.0))
    )
  }

  it("should bring up DownsampledTimeSeriesShard and be able to read data using SelectRawPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, settings.rawDatasetIngestionConfig.downsampleConfig)

    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

    Seq(gaugeName, gaugeLowFreqName, counterName, histName).foreach { metricName =>
      val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(metricName))
      val exec = MultiSchemaPartitionsExec(QueryContext(sampleLimit = 1000), InProcessPlanDispatcher,
        batchDownsampler.rawDatasetRef, 0, queryFilters, AllChunkScan)

      val queryConfig = new QueryConfig(settings.filodbConfig.getConfig("query"))
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore, queryConfig)(queryScheduler)
        .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual 1
      res.result.foreach(_.rows.nonEmpty shouldEqual true)
    }

  }

  it("should bring up DownsampledTimeSeriesShard and be able to read specific columns " +
      "from gauge using MultiSchemaPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)
    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, settings.rawDatasetIngestionConfig.downsampleConfig)
    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue
    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq
    val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(gaugeName))
    val exec = MultiSchemaPartitionsExec(QueryContext(sampleLimit = 1000), InProcessPlanDispatcher,
      batchDownsampler.rawDatasetRef, 0, queryFilters, AllChunkScan,
      colName = Option("sum"))
    val queryConfig = new QueryConfig(settings.filodbConfig.getConfig("query"))
    val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
    val res = exec.execute(downsampleTSStore, queryConfig)(queryScheduler)
      .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
    queryScheduler.shutdown()
    res.result.size shouldEqual 1
    res.result.head.rows.map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual
      List((1574372982000L, 88.0), (1574373042000L, 24.0))
  }

}

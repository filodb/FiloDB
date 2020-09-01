package filodb.downsampler

import java.io.File
import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.{ConfigException, ConfigFactory}
import monix.execution.Scheduler
import monix.reactive.Observable
import org.apache.spark.{SparkConf, SparkException}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.cardbuster.CardinalityBuster
import filodb.core.GlobalScheduler._
import filodb.core.MachineMetricsData
import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordBuilder, RecordSchema}
import filodb.core.downsample.{DownsampledTimeSeriesStore, OffHeapMemory}
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition}
import filodb.core.memstore.FiloSchedulers.QuerySchedName
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.core.store.{AllChunkScan, PartKeyRecord, SinglePartitionScan, StoreConfig}
import filodb.downsampler.chunk.{BatchDownsampler, Downsampler, DownsamplerSettings}
import filodb.downsampler.index.{DSIndexJobSettings, IndexJobDriver}
import filodb.memory.format.{PrimitiveVectorReader, UnsafeUtils}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.vectors.{CustomBuckets, LongHistogram}
import filodb.query.QueryResult
import filodb.query.exec.{InProcessPlanDispatcher, MultiSchemaPartitionsExec}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec tests downsampling round trip.
  * Each item in the spec depends on the previous step. Hence entire spec needs to be run in order.
  */
class DownsamplerMainSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val conf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf"))

  val settings = new DownsamplerSettings(conf)
  val queryConfig = new QueryConfig(settings.filodbConfig.getConfig("query"))
  val dsIndexJobSettings = new DSIndexJobSettings(settings)
  val batchDownsampler = new BatchDownsampler(settings)

  val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
  val bulkSeriesTags = Map("_ws_".utf8 -> "bulk_ws".utf8, "_ns_".utf8 -> "bulk_ns".utf8)

  val rawColStore = batchDownsampler.rawCassandraColStore
  val downsampleColStore = batchDownsampler.downsampleCassandraColStore

  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString( """
                  |flush-interval = 1h
                  |shard-mem-size = 1MB
                  |ingestion-buffer-mem-size = 30MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram, Schemas.untyped),
                                     Map.empty, 100, rawDataStoreConfig)

  val untypedName = "my_untyped"
  var untypedPartKeyBytes: Array[Byte] = _

  val gaugeName = "my_gauge"
  var gaugePartKeyBytes: Array[Byte] = _

  val counterName = "my_counter"
  var counterPartKeyBytes: Array[Byte] = _

  val histName = "my_histogram"
  var histPartKeyBytes: Array[Byte] = _

  val gaugeLowFreqName = "my_gauge_low_freq"
  var gaugeLowFreqPartKeyBytes: Array[Byte] = _

  val lastSampleTime = 74373042000L
  val pkUpdateHour = hour(lastSampleTime)

  val metricNames = Seq(gaugeName, gaugeLowFreqName, counterName, histName, untypedName)

  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60

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

  it ("should write untyped data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.untyped)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val partKey = partBuilder.partKeyFromObjects(Schemas.untyped, untypedName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.untyped, partKey,
      0, offheapMem.bufferPools(Schemas.untyped.schemaHash), batchDownsampler.shardStats,
      offheapMem.nativeMemoryManager, 1)

    untypedPartKeyBytes = part.partKeyBytes

    val rawSamples = Stream(
      Seq(74372801000L, 3d, untypedName, seriesTags),
      Seq(74372802000L, 5d, untypedName, seriesTags),

      Seq(74372861000L, 9d, untypedName, seriesTags),
      Seq(74372862000L, 11d, untypedName, seriesTags),

      Seq(74372921000L, 13d, untypedName, seriesTags),
      Seq(74372922000L, 15d, untypedName, seriesTags),

      Seq(74372981000L, 17d, untypedName, seriesTags),
      Seq(74372982000L, 15d, untypedName, seriesTags),

      Seq(74373041000L, 13d, untypedName, seriesTags),
      Seq(74373042000L, 11d, untypedName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.untyped.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(untypedPartKeyBytes, 74372801000L, 74373042000L, Some(150))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
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
      Seq(74372801000L, 3d, gaugeName, seriesTags),
      Seq(74372802000L, 5d, gaugeName, seriesTags),

      Seq(74372861000L, 9d, gaugeName, seriesTags),
      Seq(74372862000L, 11d, gaugeName, seriesTags),

      Seq(74372921000L, 13d, gaugeName, seriesTags),
      Seq(74372922000L, 15d, gaugeName, seriesTags),

      Seq(74372981000L, 17d, gaugeName, seriesTags),
      Seq(74372982000L, 15d, gaugeName, seriesTags),

      Seq(74373041000L, 13d, gaugeName, seriesTags),
      Seq(74373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(gaugePartKeyBytes, 74372801000L, 74373042000L, Some(150))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
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
      Seq(74372801000L, 3d, gaugeName, seriesTags),
      Seq(74372802000L, 5d, gaugeName, seriesTags),

      // skip next minute

      Seq(74372921000L, 13d, gaugeName, seriesTags),
      Seq(74372922000L, 15d, gaugeName, seriesTags),

      // skip next minute

      Seq(74373041000L, 13d, gaugeName, seriesTags),
      Seq(74373042000L, 11d, gaugeName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.gauge.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(gaugeLowFreqPartKeyBytes, 74372801000L, 74373042000L, Some(150))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
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
      Seq(74372801000L, 3d, counterName, seriesTags),
      Seq(74372801500L, 4d, counterName, seriesTags),
      Seq(74372802000L, 5d, counterName, seriesTags),

      Seq(74372861000L, 9d, counterName, seriesTags),
      Seq(74372861500L, 10d, counterName, seriesTags),
      Seq(74372862000L, 11d, counterName, seriesTags),

      Seq(74372921000L, 2d, counterName, seriesTags),
      Seq(74372921500L, 7d, counterName, seriesTags),
      Seq(74372922000L, 15d, counterName, seriesTags),

      Seq(74372981000L, 17d, counterName, seriesTags),
      Seq(74372981500L, 1d, counterName, seriesTags),
      Seq(74372982000L, 15d, counterName, seriesTags),

      Seq(74373041000L, 18d, counterName, seriesTags),
      Seq(74373042000L, 20d, counterName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promCounter.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(counterPartKeyBytes, 74372801000L, 74373042000L, Some(1))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
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
      Seq(74372801000L, 0d, 1d, LongHistogram(bucketScheme, Array(0L, 0, 1)), histName, seriesTags),
      Seq(74372801500L, 2d, 3d, LongHistogram(bucketScheme, Array(0L, 2, 3)), histName, seriesTags),
      Seq(74372802000L, 5d, 6d, LongHistogram(bucketScheme, Array(2L, 5, 6)), histName, seriesTags),

      Seq(74372861000L, 9d, 9d, LongHistogram(bucketScheme, Array(2L, 5, 9)), histName, seriesTags),
      Seq(74372861500L, 10d, 10d, LongHistogram(bucketScheme, Array(2L, 5, 10)), histName, seriesTags),
      Seq(74372862000L, 11d, 14d, LongHistogram(bucketScheme, Array(2L, 8, 14)), histName, seriesTags),

      Seq(74372921000L, 2d, 2d, LongHistogram(bucketScheme, Array(0L, 0, 2)), histName, seriesTags),
      Seq(74372921500L, 7d, 9d, LongHistogram(bucketScheme, Array(1L, 7, 9)), histName, seriesTags),
      Seq(74372922000L, 15d, 19d, LongHistogram(bucketScheme, Array(1L, 15, 19)), histName, seriesTags),

      Seq(74372981000L, 17d, 21d, LongHistogram(bucketScheme, Array(2L, 16, 21)), histName, seriesTags),
      Seq(74372981500L, 1d, 1d, LongHistogram(bucketScheme, Array(0L, 1, 1)), histName, seriesTags),
      Seq(74372982000L, 15d, 15d, LongHistogram(bucketScheme, Array(0L, 15, 15)), histName, seriesTags),

      Seq(74373041000L, 18d, 19d, LongHistogram(bucketScheme, Array(1L, 16, 19)), histName, seriesTags),
      Seq(74373042000L, 20d, 25d, LongHistogram(bucketScheme, Array(4L, 20, 25)), histName, seriesTags)
    )

    MachineMetricsData.records(rawDataset, rawSamples).records.foreach { case (base, offset) =>
      val rr = new BinaryRecordRowReader(Schemas.promHistogram.ingestionSchema, base, offset)
      part.ingest( lastSampleTime, rr, offheapMem.blockMemFactory)
    }
    part.switchBuffers(offheapMem.blockMemFactory, true)
    val chunks = part.makeFlushChunks(offheapMem.blockMemFactory)

    rawColStore.write(rawDataset.ref, Observable.fromIterator(chunks)).futureValue
    val pk = PartKeyRecord(histPartKeyBytes, 74372801000L, 74373042000L, Some(199))
    rawColStore.writePartKeys(rawDataset.ref, 0, Observable.now(pk), 259200, pkUpdateHour).futureValue
  }

  val numShards = dsIndexJobSettings.numShards
  val bulkPkUpdateHours = {
    val start = pkUpdateHour / 6 * 6 // 6 is number of hours per downsample chunk
    start until start + 6
  }

  it("should simulate bulk part key records being written into raw for migration") {
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val schemas = Seq(Schemas.promHistogram, Schemas.gauge, Schemas.promCounter)
    case class PkToWrite(pkr: PartKeyRecord, shard: Int, updateHour: Long)
    val pks = for { i <- 0 to 10000 } yield {
      val schema = schemas(i % schemas.size)
      val partKey = partBuilder.partKeyFromObjects(schema, s"bulkmetric$i", bulkSeriesTags)
      val bytes = schema.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, partKey)
      PkToWrite(PartKeyRecord(bytes, i, i + 500, Some(-i)), i % numShards,
        bulkPkUpdateHours(i % bulkPkUpdateHours.size))
    }

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)
    pks.groupBy(k => (k.shard, k.updateHour)).foreach { case ((shard, updHour), shardPks) =>
      rawColStore.writePartKeys(rawDataset.ref, shard, Observable.fromIterable(shardPks).map(_.pkr),
        259200, updHour).futureValue
    }
  }

  it ("should free all offheap memory") {
    offheapMem.free()
  }

  it ("should downsample raw data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.userTimeOverride", Instant.ofEpochMilli(lastSampleTime).toString)
    val downsampler = new Downsampler(settings, batchDownsampler)
    downsampler.run(sparkConf).close()
  }

  it ("should migrate partKey data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.index.timeInPeriodOverride", Instant.ofEpochMilli(lastSampleTime).toString)
    sparkConf.set("spark.filodb.downsampler.index.toHourExclOverride", (pkUpdateHour + 6 + 1).toString)
    val indexUpdater = new IndexJobDriver(settings, dsIndexJobSettings)
    indexUpdater.run(sparkConf).close()
  }

  it ("should verify migrated partKey data and match the downsampled schema") {

    def pkMetricSchemaReader(pkr: PartKeyRecord): (String, String) = {
      val schemaId = RecordSchema.schemaID(pkr.partKey, UnsafeUtils.arayOffset)
      val partSchema = batchDownsampler.schemas(schemaId)
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      (strPairs.find(p => p._1 == "_metric_").get._2, partSchema.data.name)
    }

    val metrics = Set((counterName, Schemas.promCounter.name),
      (gaugeName, Schemas.dsGauge.name),
      (gaugeLowFreqName, Schemas.dsGauge.name),
      (histName, Schemas.promHistogram.name))
    val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")), 0)
    val tsSchemaMetric = Await.result(partKeys.map(pkMetricSchemaReader).toListL.runAsync, 1 minutes)
    tsSchemaMetric.filter(k => metricNames.contains(k._1)).toSet shouldEqual metrics
  }

  it("should verify bulk part key record migration and validate completeness of PK migration") {

    def pkMetricName(pkr: PartKeyRecord): String = {
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      strPairs.find(p => p._1 == "_metric_").head._2
    }
    val readKeys = (0 until 4).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
                                                     shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // readKeys should not contain untyped part key - we dont downsample untyped
    readKeys shouldEqual (0 to 10000).map(i => s"bulkmetric$i").toSet ++ (metricNames.toSet - untypedName)
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas).get
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData1, Some(1.minute.toMillis))

    downsampledPart1.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (74372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (74372862000L, 9.0, 11.0, 20.0, 2.0, 10.0),
      (74372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (74372982000L, 15.0, 17.0, 32.0, 2.0, 16.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  /*
  Tip: After running this spec, you can bring up the local downsample server and hit following URL on browser
  http://localhost:9080/promql/prometheus/api/v1/query_range?query=my_gauge%7B_ws_%3D%27my_ws%27%2C_ns_%3D%27my_ns%27%7D&start=74372801&end=74373042&step=10&verbose=true&spread=2
   */

  it("should read and verify low freq gauge in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val dsGaugeLowFreqPartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugeLowFreqPartKeyBytes,
                                                                          batchDownsampler.schemas).get
    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(dsGaugeLowFreqPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData1, Some(1.minute.toMillis))

    downsampledPart1.partKeyBytes shouldEqual dsGaugeLowFreqPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData1 shouldEqual Seq(
      (74372802000L, 3.0, 5.0, 8.0, 2.0, 4.0),
      (74372922000L, 13.0, 15.0, 28.0, 2.0, 14.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0,
      downsampledPartData1, Some(1.minute.toMillis))

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1))

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (74372801000L, 3d),
      (74372802000L, 5d),

      (74372862000L, 11d),

      (74372921000L, 2d),
      (74372922000L, 15d),

      (74372981000L, 17d),
      (74372981500L, 1d),
      (74372982000L, 15d),

      (74373042000L, 20d)

    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, Some(5.minutes.toMillis))

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
      (74372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (74372802000L, 5d, 6d, Seq(2d, 5d, 6d)),

      (74372862000L, 11d, 14d, Seq(2d, 8d, 14d)),

      (74372921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (74372922000L, 15d, 19d, Seq(1d, 15d, 19d)),

      (74372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (74372982000L, 15d, 15d, Seq(0d, 15d, 15d)),

      (74373042000L, 20d, 25d, Seq(4d, 20d, 25d))
    )
  }

  it("should read and verify gauge data in cassandra using PagedReadablePartition for 5-min downsampled data") {
    val dsGaugePartKeyBytes = RecordBuilder.buildDownsamplePartKey(gaugePartKeyBytes, batchDownsampler.schemas).get
    val downsampledPartData2 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(dsGaugePartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart2 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0,
      downsampledPartData2, Some(5.minutes.toMillis))

    downsampledPart2.partKeyBytes shouldEqual dsGaugePartKeyBytes

    val rv2 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart2, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData2 = rv2.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData2 shouldEqual Seq(
      (74372982000L, 3.0, 17.0, 88.0, 8.0, 11.0),
      (74373042000L, 11.0, 13.0, 24.0, 2.0, 12.0)
    )
  }

  it("should read and verify prom counter data in cassandra using PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(counterPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promCounter.downsample.get, 0, 0,
      downsampledPartData1, Some(5.minutes.toMillis))

    downsampledPart1.partKeyBytes shouldEqual counterPartKeyBytes

    val rv1 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart1, AllChunkScan, Array(0, 1))

    val ctrChunkInfo = downsampledPart1.infos(AllChunkScan).nextInfoReader
    PrimitiveVectorReader.dropped(ctrChunkInfo.vectorAccessor(1), ctrChunkInfo.vectorAddress(1)) shouldEqual true

    val downsampledData1 = rv1.rows.map { r =>
      (r.getLong(0), r.getDouble(1))
    }.toList

    // time, counter
    downsampledData1 shouldEqual Seq(
      (74372801000L, 3d),

      (74372862000L, 11d),

      (74372921000L, 2d),

      (74372981000L, 17d),
      (74372981500L, 1d),

      (74372982000L, 15.0d),

      (74373042000L, 20.0d)
    )
  }

  it("should read and verify prom histogram data in cassandra using " +
    "PagedReadablePartition for 5-min downsampled data") {

    val downsampledPartData1 = downsampleColStore.readRawPartitions(
      batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(histPartKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.promHistogram.downsample.get, 0, 0,
      downsampledPartData1, Some(5.minutes.toMillis))

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
      (74372801000L, 0d, 1d, Seq(0d, 0d, 1d)),
      (74372862000L, 11d, 14d, Seq(2d, 8d, 14d)),
      (74372921000L, 2d, 2d, Seq(0d, 0d, 2d)),
      (74372981000L, 17d, 21d, Seq(2d, 16d, 21d)),
      (74372981500L, 1d, 1d, Seq(0d, 1d, 1d)),
      (74372982000L, 15.0d, 15.0d, Seq(0.0, 15.0, 15.0)),
      (74373042000L, 20.0d, 25.0d, Seq(4.0, 20.0, 25.0))
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

      val querySession = QuerySession(QueryContext(), queryConfig)
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
        .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual 1
      res.result.foreach(_.rows.nonEmpty shouldEqual true)
    }

  }

  it("should bring up DownsampledTimeSeriesShard and NOT be able to read untyped data using SelectRawPartitionsExec") {

    val downsampleTSStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore,
      settings.filodbConfig)

    downsampleTSStore.setup(batchDownsampler.rawDatasetRef, settings.filodbSettings.schemas,
      0, rawDataStoreConfig, settings.rawDatasetIngestionConfig.downsampleConfig)

    downsampleTSStore.recoverIndex(batchDownsampler.rawDatasetRef, 0).futureValue

    val colFilters = seriesTags.map { case (t, v) => ColumnFilter(t.toString, Equals(v.toString)) }.toSeq

      val queryFilters = colFilters :+ ColumnFilter("_metric_", Equals(untypedName))
      val exec = MultiSchemaPartitionsExec(QueryContext(sampleLimit = 1000), InProcessPlanDispatcher,
        batchDownsampler.rawDatasetRef, 0, queryFilters, AllChunkScan)

      val querySession = QuerySession(QueryContext(), queryConfig)
      val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
      val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
        .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
      queryScheduler.shutdown()

      res.result.size shouldEqual 0
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
    val querySession = QuerySession(QueryContext(), queryConfig)
    val queryScheduler = Scheduler.fixedPool(s"$QuerySchedName", 3)
    val res = exec.execute(downsampleTSStore, querySession)(queryScheduler)
      .runAsync(queryScheduler).futureValue.asInstanceOf[QueryResult]
    queryScheduler.shutdown()
    res.result.size shouldEqual 1
    res.result.head.rows.map(r => (r.getLong(0), r.getDouble(1))).toList shouldEqual
      List((74372982000L, 88.0), (74373042000L, 24.0))
  }

  it ("should fail when cardinality buster is not configured with any delete filters") {

    // This test case is important to ensure that a run with missing configuration will not do unintended deletes
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val settings2 = new DownsamplerSettings(conf)
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)
    val caught = intercept[SparkException] {
      cardBuster.run(sparkConf).close()
    }
    caught.getCause.asInstanceOf[ConfigException.Missing].getMessage
      .contains("No configuration setting found for key 'cardbuster'") shouldEqual true
  }

  it ("should be able to bust cardinality by time filter in downsample tables with spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val deleteFilterConfig = ConfigFactory.parseString(
      s"""
         |filodb.cardbuster.delete-pk-filters = [
         | {
         |    _ns_ = "bulk_ns"
         |    _ws_ = "bulk_ws"
         | }
         |]
         |filodb.cardbuster.delete-startTimeGTE = "${Instant.ofEpochMilli(0).toString}"
         |filodb.cardbuster.delete-endTimeLTE = "${Instant.ofEpochMilli(600).toString}"
         |""".stripMargin)

    val settings2 = new DownsamplerSettings(deleteFilterConfig.withFallback(conf))
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)
    cardBuster.run(sparkConf).close()

    sparkConf.set("spark.filodb.cardbuster.inDownsampleTables", "true")
    cardBuster.run(sparkConf).close()
  }

  it("should verify bulk part key records are absent after card busting by time filter in downsample tables") {

    def pkMetricName(pkr: PartKeyRecord): String = {
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      strPairs.find(p => p._1 == "_metric_").head._2
    }

    val readKeys = (0 until 4).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
        shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // downsample set should not have a few bulk metrics
    readKeys.size shouldEqual 9904

    val readKeys2 = (0 until 4).flatMap { shard =>
      val partKeys = rawColStore.scanPartKeys(batchDownsampler.rawDatasetRef, shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // raw set should remain same since inDownsampleTables=true in
    readKeys2.size shouldEqual 10006
  }

  it ("should be able to bust cardinality in both raw and downsample tables with spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val deleteFilterConfig = ConfigFactory.parseString( """
                                                          |filodb.cardbuster.delete-pk-filters = [
                                                          | {
                                                          |    _ns_ = "bulk_ns"
                                                          |    _ws_ = "bulk_ws"
                                                          | }
                                                          |]
                                                        """.stripMargin)
    val settings2 = new DownsamplerSettings(deleteFilterConfig.withFallback(conf))
    val dsIndexJobSettings2 = new DSIndexJobSettings(settings2)
    val cardBuster = new CardinalityBuster(settings2, dsIndexJobSettings2)

    // first run for downsample tables
    cardBuster.run(sparkConf).close()

    // then run for raw tables
    sparkConf.set("spark.filodb.cardbuster.inDownsampleTables", "false")
    cardBuster.run(sparkConf).close()
  }

  it("should verify bulk part key records are absent after deletion in both raw and downsample tables") {

    def pkMetricName(pkr: PartKeyRecord): String = {
      val strPairs = batchDownsampler.schemas.part.binSchema.toStringPairs(pkr.partKey, UnsafeUtils.arayOffset)
      strPairs.find(p => p._1 == "_metric_").head._2
    }

    val readKeys = (0 until 4).flatMap { shard =>
      val partKeys = downsampleColStore.scanPartKeys(batchDownsampler.downsampleRefsByRes(FiniteDuration(5, "min")),
        shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // readKeys should not contain bulk PK records
    readKeys shouldEqual (metricNames.toSet - untypedName)

    val readKeys2 = (0 until 4).flatMap { shard =>
      val partKeys = rawColStore.scanPartKeys(batchDownsampler.rawDatasetRef, shard)
      Await.result(partKeys.map(pkMetricName).toListL.runAsync, 1 minutes)
    }.toSet

    // readKeys should not contain bulk PK records
    readKeys2 shouldEqual metricNames.toSet
  }


}

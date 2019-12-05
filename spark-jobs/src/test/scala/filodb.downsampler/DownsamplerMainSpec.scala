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
import filodb.memory.format.ZeroCopyUTF8String._

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

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge), Map.empty, 100, storeConfig)
  var partKeyBytes: Array[Byte] = _
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

  it ("should write data to cassandra") {

    val rawDataset = Dataset("prometheus", Schemas.gauge)

    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val seriesName = "myGauge"
    val seriesTags = Map("k".utf8 -> "v".utf8)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, seriesName, seriesTags)

    val part = new TimeSeriesPartition(0, Schemas.gauge, partKey,
      0, offheapMem.bufferPools(Schemas.gauge.schemaHash), shardStats,
      offheapMem.nativeMemoryManager, 1)

    partKeyBytes = part.partKeyBytes

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

  it ("should downsample raw data into the downsample dataset tables in cassandra using spark job") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.filodb.downsampler.userTimeOverride", lastSampleTime.toString)
    downsampler.run(sparkConf)
  }

  it ("should migrate partKey data into the downsample dataset tables in cassandra using spark job") {
    // TODO in future PR
  }

  it("should read and verify data in cassandra using PagedReadablePartition for 1-min downsampled data") {

    val downsampledPartData1 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(1, "min")),
      0,
      SinglePartitionScan(partKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart1 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0, downsampledPartData1)

    downsampledPart1.partKeyBytes shouldEqual partKeyBytes

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

  it("should read and verify data in cassandra using PagedReadablePartition for 5-min downsampled data") {
    val downsampledPartData2 = colStore.readRawPartitions(
      BatchDownsampler.downsampleDatasetRefs(FiniteDuration(5, "min")),
      0,
      SinglePartitionScan(partKeyBytes))
      .toListL.runAsync.futureValue.head

    val downsampledPart2 = new PagedReadablePartition(Schemas.gauge.downsample.get, 0, 0, downsampledPartData2)

    downsampledPart2.partKeyBytes shouldEqual partKeyBytes

    val rv2 = RawDataRangeVector(CustomRangeVectorKey.empty, downsampledPart2, AllChunkScan, Array(0, 1, 2, 3, 4, 5))

    val downsampledData2 = rv2.rows.map { r =>
      (r.getLong(0), r.getDouble(1), r.getDouble(2), r.getDouble(3), r.getDouble(4), r.getDouble(5))
    }.toList

    // time, min, max, sum, count, avg
    downsampledData2 shouldEqual Seq(
      (1574273042000L, 3.0, 17.0, 112.0, 10.0, 11.2)
    )
  }

  it("should read and verify part key migration in cassandra for 5-min downsampled data") {
    // TODO in future PR
  }
}

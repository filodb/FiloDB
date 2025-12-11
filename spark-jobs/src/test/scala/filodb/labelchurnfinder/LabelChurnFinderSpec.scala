package filodb.labelchurnfinder

import com.typesafe.config.{Config, ConfigFactory}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.downsample.OffHeapMemory
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.store.{PartKeyRecord, StoreConfig}
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.ZeroCopyUTF8String._
import monix.reactive.Observable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.io.File
import java.time.Instant
import filodb.labelchurnfinder.LcfTask.{ActiveCountColName, ChurnColName, LabelColName, TotalCountColName, WsNsColName}

class LabelChurnFinderSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val baseConf: Config = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf")).resolve()

  val now = 1752700000000L
  val jobConfig: Config = ConfigFactory.parseString(
    s"""
       |filodb.labelchurnfinder.pk-filters.0._ws_ = bulk_ws
       |filodb.labelchurnfinder.since-time = "${Instant.ofEpochMilli(now).toString}"
       |filodb.labelchurnfinder.dataset = prometheus
       |""".stripMargin)

  val rawDataStoreConfig: StoreConfig = StoreConfig(ConfigFactory.parseString( """
                                                                    |flush-interval = 1h
                                                                    |shard-mem-size = 1MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram,
    Schemas.deltaCounter, Schemas.deltaHistogram, Schemas.untyped,
    Schemas.otelDeltaHistogram, Schemas.otelCumulativeHistogram,
    Schemas.otelExpDeltaHistogram),
    Map.empty, 100, rawDataStoreConfig)

  val rawDataset: Dataset = Dataset("prometheus", Schemas.promCounter)

  val bulkSeriesTags = Map("_ws_".utf8 -> "bulk_ws".utf8, "_ns_".utf8 -> "bulk_ns".utf8)

  val settings = new DownsamplerSettings(jobConfig.withFallback(baseConf))
  val numShards = settings.numShards
  val colStore = new LcfTask(settings).colStore

  val numPods = 30
  val numInstances = 10
  val numNs = 2
  val numContainers = 20000

  override def beforeAll(): Unit = {
    colStore.initialize(rawDataset.ref, numShards, settings.rawDatasetIngestionConfig.resources).futureValue
    colStore.truncate(rawDataset.ref, numShards).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }

  it("should simulate bulk part key records being written into raw for processing") {
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val schemas = Seq(Schemas.promHistogram, Schemas.gauge, Schemas.promCounter, Schemas.deltaCounter,
      Schemas.deltaHistogram, Schemas.untyped, Schemas.otelDeltaHistogram,
      Schemas.otelCumulativeHistogram, Schemas.otelExpDeltaHistogram)
    case class PkToWrite(pkr: PartKeyRecord, updateHour: Long)
    val pks = for { i <- 0 to numContainers } yield {
      val schema = schemas(i % schemas.size)
      val partKey = partBuilder.partKeyFromObjects(schema, s"bulkmetric", bulkSeriesTags ++ Map(
        "_ws_".utf8 -> "bulk_ws".utf8,
        "_ns_".utf8 -> s"bulk_ns${i % numNs}".utf8,
        "pod".utf8 -> s"pod${i % numPods}".utf8,
        "instance".utf8 -> s"instance${i % numInstances}".utf8,
        "container".utf8 -> s"container$i".utf8))
      val bytes = schema.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, partKey)
      val startTime = i + now
      val endTime = if (i % 2 == 0) Long.MaxValue else  i + 500 + now
      PkToWrite(PartKeyRecord(bytes, startTime, endTime, i % numShards), 0)
    }

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)
    pks.groupBy(k => (k.pkr.shard, k.updateHour)).foreach { case ((shard, updHour), shardPks) =>
      colStore.writePartKeys(rawDataset.ref, shard, Observable.fromIterable(shardPks).map(_.pkr),
        259200, updHour).futureValue
    }
  }

  it ("should run LCF job for one namespace and workspace") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val filterConfig = ConfigFactory.parseString(
      s"""
         |filodb.labelchurnfinder.pk-filters.0._ns_ = bulk_ns0
         |filodb.labelchurnfinder.pk-filters.0._ws_ = "b.*_ws"
         |""".stripMargin)
    val settings2 = new DownsamplerSettings(filterConfig.withFallback(jobConfig.withFallback(baseConf)))
    val lcf = new LabelChurnFinder(settings2)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("LabelChurnFinder")
      .config(sparkConf)
      .getOrCreate()

    val result = lcf.computeChurn(spark).collect()
    result.length shouldEqual 6
    val cards = result.map { row => (row.getAs[List[String]](WsNsColName),
                                     row.getAs[List[String]](LabelColName),
                                     row.getAs[Long](ActiveCountColName),
                                     row.getAs[Long](TotalCountColName)) }
    cards shouldEqual Array(
      (List("bulk_ws", "bulk_ns0"), "_ns_", 1, 1),
      (List("bulk_ws", "bulk_ns0"), "_ws_", 1, 1),
      (List("bulk_ws", "bulk_ns0"), "instance", numInstances/2, numInstances/2),
      (List("bulk_ws", "bulk_ns0"), "container", 10075, 10075),
      (List("bulk_ws", "bulk_ns0"), "_metric_", 1, 1),
      (List("bulk_ws", "bulk_ns0"), "pod", numPods/2, numPods/2),
    )
    spark.stop()
  }

  it ("should run LCF job for multiple namespaces and workspaces") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val filterConfig = ConfigFactory.parseString(
      s"""
         |filodb.labelchurnfinder.pk-filters.0._ns_ = "bulk_ns.*"
         |filodb.labelchurnfinder.pk-filters.0._ws_ = "b.*_ws"
         |""".stripMargin)
    val settings2 = new DownsamplerSettings(filterConfig.withFallback(jobConfig.withFallback(baseConf)))
    val lcf = new LabelChurnFinder(settings2)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("LabelChurnFinder")
      .config(sparkConf)
      .getOrCreate()
    val result = lcf.computeChurn(spark).collect()
    result.length shouldEqual 12
    val cards = result.map { row => (row.getAs[List[String]](WsNsColName),
                                      row.getAs[List[String]](LabelColName),
                                      row.getAs[Long](ActiveCountColName),
                                      row.getAs[Long](TotalCountColName)) }
    cards shouldEqual Array(
      (List("bulk_ws", "bulk_ns0"), "_ns_", 1, 1),
      (List("bulk_ws", "bulk_ns0"), "_ws_", 1, 1),
      (List("bulk_ws", "bulk_ns1"), "_ns_", 0, 1),
      (List("bulk_ws", "bulk_ns0"), "instance", numInstances/2, numInstances/2),
      (List("bulk_ws", "bulk_ns1"), "_metric_", 0, 1),
      (List("bulk_ws", "bulk_ns1"), "container", 0, 10182),
      (List("bulk_ws", "bulk_ns0"), "container", 10075, 10075),
      (List("bulk_ws", "bulk_ns1"), "_ws_", 0, 1),
      (List("bulk_ws", "bulk_ns0"), "_metric_", 1, 1),
      (List("bulk_ws", "bulk_ns1"), "instance", 0, numInstances/2),
      (List("bulk_ws", "bulk_ns0"), "pod", numPods/2, numPods/2),
      (List("bulk_ws", "bulk_ns1"), "pod", 0, numPods/2)
    )
    spark.stop()
  }

  it ("should run LCF job for different time range") {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val filterConfig = ConfigFactory.parseString(
      s"""
         |filodb.labelchurnfinder.pk-filters.0._ns_ = "bulk_ns.*"
         |filodb.labelchurnfinder.pk-filters.0._ws_ = "b.*_ws"
         |filodb.labelchurnfinder.since-time = "${Instant.ofEpochMilli(now + 5000).toString}"
         |""".stripMargin)
    val settings2 = new DownsamplerSettings(filterConfig.withFallback(jobConfig.withFallback(baseConf)))
    val lcf = new LabelChurnFinder(settings2)
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("LabelChurnFinder")
      .config(sparkConf)
      .getOrCreate()
    val result = lcf.computeChurn(spark).collect()
    result.length shouldEqual 12
    val cards = result.map { row => (row.getAs[List[String]](WsNsColName),
                                      row.getAs[List[String]](LabelColName),
                                      row.getAs[Long](ActiveCountColName),
                                      row.getAs[Long](TotalCountColName)) }
    cards shouldEqual Array(
      (List("bulk_ws", "bulk_ns0"), "_ns_", 1, 1),
      (List("bulk_ws", "bulk_ns0"), "_ws_", 1, 1),
      (List("bulk_ws", "bulk_ns1"), "_ns_", 0, 1),
      (List("bulk_ws", "bulk_ns0"), "instance", numInstances/2, numInstances/2),
      (List("bulk_ws", "bulk_ns1"), "_metric_", 0, 1),
      (List("bulk_ws", "bulk_ns1"), "container", 0, 7901), // reduced from 9922 above
      (List("bulk_ws", "bulk_ns0"), "container", 10075, 10075),
      (List("bulk_ws", "bulk_ns1"), "_ws_", 0, 1),
      (List("bulk_ws", "bulk_ns0"), "_metric_", 1, 1),
      (List("bulk_ws", "bulk_ns1"), "instance", 0, numInstances/2),
      (List("bulk_ws", "bulk_ns0"), "pod", numPods/2, numPods/2),
      (List("bulk_ws", "bulk_ns1"), "pod", 0, 15))
    spark.stop()
  }

  it ("should identify high churn labels for taking actions on them") {
    // simulate a result DataFrame and check high churn labels
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("LabelChurnFinder")
      .config(sparkConf)
      .getOrCreate()
    val settings2 = new DownsamplerSettings(jobConfig.withFallback(baseConf))
    val lcf = new LabelChurnFinder(settings2)
    val data = Seq(
      (List("bulk_ws", "bulk_ns0"), "_ns_", 1L, 1L, 1.0),
      (List("bulk_ws", "bulk_ns0"), "_ws_", 1L, 1L, 1.0),
      (List("bulk_ws", "bulk_ns1"), "_ns_", 0L, 1L, Double.PositiveInfinity),
      (List("bulk_ws", "bulk_ns0"), "instance", 5000L, 5000L, 1.0),
      (List("bulk_ws", "bulk_ns1"), "_metric_", 0L, 1L, Double.PositiveInfinity),
      (List("bulk_ws", "bulk_ns1"), "container", 8000L, 16000L, 2.0),        // should be picked as HC
      (List("bulk_ws", "bulk_ns0"), "container", 10075L, 10075L, 1.0),
      (List("bulk_ws", "bulk_ns1"), "_ws_", 0L, 1L, Double.PositiveInfinity),
      (List("bulk_ws", "bulk_ns0"), "_metric_", 1L, 1L, 1.0),
      (List("bulk_ws", "bulk_ns1"), "instance", 5000L, 15000L, 3.0),         // should be picked as HC
      (List("bulk_ws", "bulk_ns0"), "pod", 15000L, 15000L, 1.0),
      (List("bulk_ws", "bulk_ns1"), "pod", 15L, 30L, 2.0)                    // should not be picked as HC (active < minAtsThreshold of 1000)
    )
    val df = spark.createDataFrame(data).toDF(WsNsColName, LabelColName, ActiveCountColName, TotalCountColName, ChurnColName)
    val highChurnLabels = lcf.computeHighChurnLabels(df)
    highChurnLabels.collect().length shouldEqual 2
  }
}

package filodb.labelchurnfinder

import com.typesafe.config.ConfigFactory
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.downsample.OffHeapMemory
import filodb.core.memstore.{TimeSeriesShardInfo, TimeSeriesShardStats}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.store.{PartKeyRecord, StoreConfig}
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.ZeroCopyUTF8String._
import monix.reactive.Observable
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.io.File

import scala.collection.mutable

class LabelChurnFinderSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val baseConf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf")).resolve()
  val jobConfig = ConfigFactory.parseString(
    s"""
       |filodb.labelchurnfinder.pk-filters.0._ws_ = bulk_ws
       |""".stripMargin)

  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString( """
                                                                    |flush-interval = 1h
                                                                    |shard-mem-size = 1MB
                """.stripMargin))

  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram,
    Schemas.deltaCounter, Schemas.deltaHistogram, Schemas.untyped,
    Schemas.otelDeltaHistogram, Schemas.otelCumulativeHistogram,
    Schemas.otelExpDeltaHistogram),
    Map.empty, 100, rawDataStoreConfig)

  val rawDataset = Dataset("prometheus", Schemas.promCounter)
  val shardStats = new TimeSeriesShardStats(rawDataset.ref, -1)
  val shardInfo = TimeSeriesShardInfo(0, shardStats, offheapMem.bufferPools, offheapMem.nativeMemoryManager)

  val bulkSeriesTags = Map("_ws_".utf8 -> "bulk_ws".utf8, "_ns_".utf8 -> "bulk_ns".utf8)
  val lastSampleTime = 74373042000L
  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60
  val pkUpdateHour = hour(lastSampleTime)
  val bulkPkUpdateHours = {
    val start = pkUpdateHour / 6 * 6 // 6 is number of hours per downsample chunk
    start until start + 6
  }

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
      val endTime = if (i % 2 == 0) Long.MaxValue else i + 500
      PkToWrite(PartKeyRecord(bytes, i, endTime, i % numShards), bulkPkUpdateHours(i % bulkPkUpdateHours.size))
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
    val result = lcf.run(sparkConf)
    result.size shouldEqual 6
    val cards = result.mapValues { sketch => (sketch.active.getEstimate.toInt, sketch.total.getEstimate.toInt) }
    cards shouldEqual mutable.HashMap(
      List("bulk_ws", "bulk_ns0", "container") -> ((10236, 10236)),
      List("bulk_ws", "bulk_ns0", "instance") -> ((numInstances/2,numInstances/2)),
      List("bulk_ws", "bulk_ns0", "_ns_") -> ((1,1)),
      List("bulk_ws", "bulk_ns0", "pod") -> ((numPods/2, numPods/2)),
      List("bulk_ws", "bulk_ns0", "_ws_") -> ((1,1)),
      List("bulk_ws", "bulk_ns0", "_metric_") -> ((1,1))
    )
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
    val result = lcf.run(sparkConf)
    result.size shouldEqual 12
    val cards = result.mapValues { sketch => (sketch.active.getEstimate.toInt, sketch.total.getEstimate.toInt) }
    cards shouldEqual mutable.HashMap(
      List("bulk_ws", "bulk_ns1", "_metric_") -> ((0,1)),
      List("bulk_ws", "bulk_ns0", "pod") -> ((numPods/2,numPods/2)),
      List("bulk_ws", "bulk_ns0", "_ns_") -> ((1,1)),
      List("bulk_ws", "bulk_ns1", "_ws_") -> ((0,1)),
      List("bulk_ws", "bulk_ns1", "_ns_") -> ((0,1)),
      List("bulk_ws", "bulk_ns1", "instance") -> ((0,numInstances/2)),
      List("bulk_ws", "bulk_ns0", "container") -> ((10236,10236)),
      List("bulk_ws", "bulk_ns0", "_ws_") -> ((1,1)),
      List("bulk_ws", "bulk_ns1", "pod") -> ((0,numPods/2)),
      List("bulk_ws", "bulk_ns0", "instance") -> ((numInstances/2,numInstances/2)),
      List("bulk_ws", "bulk_ns1", "container") -> ((0,9922)),
      List("bulk_ws", "bulk_ns0", "_metric_") -> ((1,1))
    )
  }


}

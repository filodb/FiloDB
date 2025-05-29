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

  val settings = new DownsamplerSettings(jobConfig.withFallback(baseConf))

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
  val shardInfo = TimeSeriesShardInfo(0, shardStats,
    offheapMem.bufferPools, offheapMem.nativeMemoryManager)

  val lcf = new LabelChurnFinder(settings)

  val bulkSeriesTags = Map("_ws_".utf8 -> "bulk_ws".utf8, "_ns_".utf8 -> "bulk_ns".utf8)
  val lastSampleTime = 74373042000L
  def hour(millis: Long = System.currentTimeMillis()): Long = millis / 1000 / 60 / 60
  val pkUpdateHour = hour(lastSampleTime)
  val bulkPkUpdateHours = {
    val start = pkUpdateHour / 6 * 6 // 6 is number of hours per downsample chunk
    start until start + 6
  }

  val numShards = settings.numShards
  val lcfTask = new LcfTask(settings)

  val numPods = 30
  val numInstances = 10

  override def beforeAll(): Unit = {
    lcfTask.colStore.initialize(rawDataset.ref, numShards, settings.rawDatasetIngestionConfig.resources).futureValue
    lcfTask.colStore.truncate(rawDataset.ref, numShards).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }

  it("should simulate bulk part key records being written into raw for migration") {
    val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
    val schemas = Seq(Schemas.promHistogram, Schemas.gauge, Schemas.promCounter, Schemas.deltaCounter,
      Schemas.deltaHistogram, Schemas.untyped, Schemas.otelDeltaHistogram,
      Schemas.otelCumulativeHistogram, Schemas.otelExpDeltaHistogram)
    case class PkToWrite(pkr: PartKeyRecord, updateHour: Long)
    val pks = for { i <- 0 to 10000 } yield {
      val schema = schemas(i % schemas.size)
      val partKey = partBuilder.partKeyFromObjects(schema, s"bulkmetric", bulkSeriesTags ++ Map(
        "_ws_".utf8 -> "bulk_ws".utf8,
        "_ns_".utf8 -> s"bulk_ns".utf8,
        "pod".utf8 -> s"pod${i % numPods}".utf8,
        "instance".utf8 -> s"instance${i % numInstances}".utf8,
        "container".utf8 -> s"container$i".utf8))
      val bytes = schema.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, partKey)
      val endTime = if (i % 2 == 0) Long.MaxValue else i + 500
      PkToWrite(PartKeyRecord(bytes, i, endTime, i % numShards), bulkPkUpdateHours(i % bulkPkUpdateHours.size))
    }

    val rawDataset = Dataset("prometheus", Schemas.promHistogram)
    pks.groupBy(k => (k.pkr.shard, k.updateHour)).foreach { case ((shard, updHour), shardPks) =>
      lcfTask.colStore.writePartKeys(rawDataset.ref, shard, Observable.fromIterable(shardPks).map(_.pkr),
        259200, updHour).futureValue
    }
  }

  it ("should run LCF job") {

    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    val result = lcf.run(sparkConf)
    result.size shouldEqual 6
    val cards = result.mapValues { sketch => (sketch.active.getEstimate.toInt, sketch.total.getEstimate.toInt) }
    cards shouldEqual mutable.HashMap(
      List("bulk_ws", "bulk_ns", "container") -> ((5075, 9862)),
      List("bulk_ws", "bulk_ns", "instance") -> ((numInstances/2,numInstances)),
      List("bulk_ws", "bulk_ns", "_ns_") -> ((1,1)),
      List("bulk_ws", "bulk_ns", "pod") -> ((numPods/2, numPods)),
      List("bulk_ws", "bulk_ns", "_ws_") -> ((1,1)),
      List("bulk_ws", "bulk_ns", "_metric_") -> ((1,1))
    )
  }


}

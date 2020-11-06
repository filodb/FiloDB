package filodb.repair

import java.io.File
import java.time.Instant
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import filodb.cassandra.DefaultFiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.GlobalConfig
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.downsample.OffHeapMemory
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesShardStats}
import filodb.core.metadata.{Dataset, Schema, Schemas}
import filodb.core.store.{PartKeyRecord, StoreConfig}
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

class PartitionKeysCopierSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val defaultPatience = PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  implicit val s = monix.execution.Scheduler.Implicits.global

  val configPath = "conf/timeseries-filodb-server.conf"

  private val sysConfig = GlobalConfig.systemConfig.getConfig("filodb")
  private val config = ConfigFactory.parseFile(new File(configPath)).getConfig("filodb").withFallback(sysConfig)

  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session
  lazy val colStore = new CassandraColumnStore(config, s, session)

  var gauge1PartKeyBytes: Array[Byte] = _
  var gauge2PartKeyBytes: Array[Byte] = _
  var gauge3PartKeyBytes: Array[Byte] = _
  var gauge4PartKeyBytes: Array[Byte] = _
  val rawDataStoreConfig = StoreConfig(ConfigFactory.parseString(
    """
      |flush-interval = 1h
      |shard-mem-size = 1MB
      |ingestion-buffer-mem-size = 30MB
                """.stripMargin))
  val offheapMem = new OffHeapMemory(Seq(Schemas.gauge, Schemas.promCounter, Schemas.promHistogram, Schemas.untyped),
    Map.empty, 100, rawDataStoreConfig)

  val datasetName = "prometheus"
  val targetDatasetName = "buddy_prometheus"
  val sourceDataset = Dataset(datasetName, Schemas.gauge)
  val targetDataset = Dataset(targetDatasetName, Schemas.gauge)
  val shardStats = new TimeSeriesShardStats(sourceDataset.ref, -1)

  val sparkConf = {
    val conf = new SparkConf(loadDefaults = true)
    conf.setMaster("local[2]")

    conf.set("spark.filodb.partitionkeys.copier.source.configFile", configPath)
    conf.set("spark.filodb.partitionkeys.copier.source.dataset", datasetName)

    conf.set("spark.filodb.partitionkeys.copier.target.configFile", configPath)
    conf.set("spark.filodb.partitionkeys.copier.target.dataset", targetDatasetName)

    conf.set("spark.filodb.partitionkeys.copier.ingestionTimeStart", "2020-10-13T00:00:00Z")
    conf.set("spark.filodb.partitionkeys.copier.ingestionTimeEnd", "2020-10-13T05:00:00Z")
    conf.set("spark.filodb.partitionkeys.copier.diskTimeToLive", "7d")
    conf
  }

  val numOfShards = PartitionKeysCopier.lookup(sparkConf).getShardNum

  // Examples: 2019-10-20T12:34:56Z  or  2019-10-20T12:34:56-08:00
  private def parseDateTime(str: String) = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(str))

  def getSeriesTags(workspace: String, namespace: String): Map[ZeroCopyUTF8String, ZeroCopyUTF8String] = {
    Map("_ws_".utf8 -> workspace.utf8, "_ns_".utf8 -> namespace.utf8)
  }

  override def beforeAll(): Unit = {
    colStore.initialize(sourceDataset.ref, numOfShards).futureValue
    colStore.truncate(sourceDataset.ref, numOfShards).futureValue

    colStore.initialize(targetDataset.ref, numOfShards).futureValue
    colStore.truncate(targetDataset.ref, numOfShards).futureValue
  }

  override def afterAll(): Unit = {
    offheapMem.free()
  }

  it("should write test data to cassandra source table") {
    def writePartKeys(pk: PartKeyRecord, shard: Int): Unit = {
      colStore.writePartKeys(sourceDataset.ref, shard, Observable.now(pk), 259200, 0L, false).futureValue
    }

    def tsPartition(schema: Schema,
                    gaugeName: String,
                    seriesTags: Map[ZeroCopyUTF8String, ZeroCopyUTF8String]): TimeSeriesPartition = {
      val partBuilder = new RecordBuilder(offheapMem.nativeMemoryManager)
      val partKey = partBuilder.partKeyFromObjects(schema, gaugeName, seriesTags)
      val part = new TimeSeriesPartition(0, schema, partKey,
        0, offheapMem.bufferPools(schema.schemaHash), shardStats,
        offheapMem.nativeMemoryManager, 1)
      part
    }

    for (shard <- 0 until numOfShards) {
      val ws: String = "my_ws_name_" + shard
      val ns: String = "my_ns_id"

      gauge1PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge1", getSeriesTags(ws + "1", ns + "1")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge1PartKeyBytes, 1507923801000L, Long.MaxValue, Some(150)), shard) // 2017

      gauge2PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge2", getSeriesTags(ws + "2", ns + "2")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge2PartKeyBytes, 1539459801000L, Long.MaxValue, Some(150)), shard) // 2018

      gauge3PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge3", getSeriesTags(ws + "3", ns + "3")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge3PartKeyBytes, 1602554400000L, 1602561600000L, Some(150)), shard) // 2020-10-13 02:00-04:00 GMT

      gauge4PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge4", getSeriesTags(ws + "4", ns + "4")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge4PartKeyBytes, 1602549000000L, 1602561600000L, Some(150)), shard) // 2020-10-13 00:30-04:00 GMT
    }
  }

  it("should run a simple Spark job") {
    PartitionKeysCopierMain.run(sparkConf).close()
  }

  it("verify data written onto cassandra target table") {
    def getWorkspace(map: Map[String, String]): String = {
      val ws: String = map.get("_ws_").get
      ws
    }

    def getNamespace(map: Map[String, String]): String = {
      val ws: String = map.get("_ns_").get
      ws
    }

    def getPartKeyMap(partKeyRecord: PartKeyRecord) : Map[String, String] = {
      val pk = partKeyRecord.partKey
      val pkPairs = Schemas.gauge.partKeySchema.toStringPairs(pk, UnsafeUtils.arayOffset)
      val map = pkPairs.map(a => a._1 -> a._2).toMap
      map
    }

    val startTime = parseDateTime(sparkConf.get("spark.filodb.partitionkeys.copier.ingestionTimeStart")).toEpochMilli()
    val endTime = parseDateTime(sparkConf.get("spark.filodb.partitionkeys.copier.ingestionTimeEnd")).toEpochMilli()

    for (shard <- 0 until numOfShards) {
      val partKeyRecords = Await.result(colStore.scanPartKeys(targetDataset.ref, shard).toListL.runAsync, Duration(1, "minutes"))

      // because there will be 2 records that meets the copier time period.
      partKeyRecords.size shouldEqual 2

      for (pkr <- partKeyRecords) {
        // verify all the records fall in the copier time period.
        pkr.startTime should be >= startTime
        pkr.endTime should be <= endTime
      }
    }

    // verify workspace/namespace names in shard=0.
    val shard0 = 0
    val partKeyRecordsShard0 = Await.result(colStore.scanPartKeys(targetDataset.ref, shard0).toListL.runAsync,
      Duration(1, "minutes"))
    partKeyRecordsShard0.size shouldEqual 2
    for (pkr <- partKeyRecordsShard0) {
      val map = getPartKeyMap(pkr)

      // verify workspace/namespace value for "my_gauge3" metric
      if ("my_gauge3".equals(map.get("_metric_").get)) {
        getWorkspace(map) shouldEqual "my_ws_name_03"
        getNamespace(map) shouldEqual "my_ns_id3"
      }

      // verify workspace/namespace value for "my_gauge4" metric
      if ("my_gauge4".equals(map.get("_metric_").get)) {
        getWorkspace(map) shouldEqual "my_ws_name_04"
        getNamespace(map) shouldEqual "my_ns_id4"
      }
    }

    // verify workspace/namespace names in shard=2.
    val shard2 = 2
    val partKeyRecordsShard2 = Await.result(colStore.scanPartKeys(targetDataset.ref, shard2).toListL.runAsync,
      Duration(1, "minutes"))
    partKeyRecordsShard2.size shouldEqual 2
    for (pkr <- partKeyRecordsShard2) {
      val map = getPartKeyMap(pkr)

      // verify workspace/namespace value for "my_gauge3" metric
      if ("my_gauge3".equals(map.get("_metric_").get)) {
        getWorkspace(map) shouldEqual "my_ws_name_23"
        getNamespace(map) shouldEqual "my_ns_id3"
      }

      // verify workspace/namespace value for "my_gauge4" metric
      if ("my_gauge4".equals(map.get("_metric_").get)) {
        getWorkspace(map) shouldEqual "my_ws_name_24"
        getNamespace(map) shouldEqual "my_ns_id4"
      }
    }
  }

  it("verify data deletes on cassandra target table") {
    val shard0 = 0
    val partKeyRecordsShard0 = Await.result(colStore.scanPartKeys(targetDataset.ref, shard0).toListL.runAsync,
      Duration(1, "minutes"))
    // making sure there is some data in at least one shard before starting the delete job.
    partKeyRecordsShard0.size shouldEqual 2

    // flag enabled to deleteFirst from target
    sparkConf.set("spark.filodb.partitionkeys.copier.deleteFirst", "true")
    sparkConf.set("spark.filodb.partitionkeys.copier.noCopy", "true")

    PartitionKeysCopierMain.run(sparkConf).close()

    for (shard <- 0 until numOfShards) {
      val partKeyRec = Await.result(colStore.scanPartKeys(targetDataset.ref, shard).toListL.runAsync,
        Duration(1, "minutes"))
      partKeyRec.size shouldEqual 0
    }
  }
}
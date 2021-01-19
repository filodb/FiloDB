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
  var gauge5PartKeyBytes: Array[Byte] = _
  var gauge6PartKeyBytes: Array[Byte] = _
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

    conf.set("spark.filodb.partitionkeys.copier.repairStartTime", "2020-10-13T00:00:00Z")
    conf.set("spark.filodb.partitionkeys.copier.repairEndTime", "2020-10-13T05:00:00Z")
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

      /*
      Repair window: 2020-10-13T00:00:00Z and 2020-10-13T05:00:00Z

      1. timeSeries born/died before repair window.             1507923801000 (October 13, 2017 7:43:21 PM) - 1510611624000 (November 13, 2017 10:20:24 PM) x
      2. timeSeries born before and died within repair window   1510611624000 (November 13, 2017 10:20:24 PM) - 1602561600000 (October 13, 2020 4:00:00 AM) √
      3. timeSeries born and died within repair window          1602554400000 (October 13, 2020 2:00:00 AM) - 1602561600000 (October 13, 2020 4:00:00 AM)   √
      4. timeSeries born within and died after repair window    1602561600000 (October 13, 2020 4:00:00 AM) - 1609855200000 (January 5, 2021 2:00:00 PM)    √
      5. timeSeries born and died after repair window           1609855200000 (January 5, 2021 2:00:00 PM) - 1610028000000 (January 7, 2021 2:00:00 PM)     x
      6. timeSeries born before and died after repair window    1507923801000 (October 13, 2017 7:43:21 PM) - 1610028000000 (January 7, 2021 2:00:00 PM)    x

      Result: 2, 3 and 4 should be repaired/migrated as per the requirement.
       */

      gauge1PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge1", getSeriesTags(ws + "1", ns + "1")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge1PartKeyBytes, 1507923801000L, 1510611624000L, Some(150)), shard)

      gauge2PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge2", getSeriesTags(ws + "2", ns + "2")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge2PartKeyBytes, 1510611624000L, 1602561600000L, Some(150)), shard)

      gauge3PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge3", getSeriesTags(ws + "3", ns + "3")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge3PartKeyBytes, 1602554400000L, 1602561600000L, Some(150)), shard)

      gauge4PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge4", getSeriesTags(ws + "4", ns + "4")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge4PartKeyBytes, 1602561600000L, 1609855200000L, Some(150)), shard)

      gauge5PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge5", getSeriesTags(ws + "5", ns + "5")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge5PartKeyBytes, 1609855200000L, 1610028000000L, Some(150)), shard)

      gauge6PartKeyBytes = tsPartition(Schemas.gauge, "my_gauge6", getSeriesTags(ws + "6", ns + "6")).partKeyBytes
      writePartKeys(PartKeyRecord(gauge6PartKeyBytes, 1507923801000L, 1610028000000L, Some(150)), shard)
    }
  }

  it("should run a simple Spark job") {
    PartitionKeysCopierMain.run(sparkConf).close()
  }

  it("should have copied data to cassandra target tables") {
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

    val startTime = parseDateTime(sparkConf.get("spark.filodb.partitionkeys.copier.repairStartTime")).toEpochMilli()
    val endTime = parseDateTime(sparkConf.get("spark.filodb.partitionkeys.copier.repairEndTime")).toEpochMilli()

    // verify data in index table.
    for (shard <- 0 until numOfShards) {
      val partKeyRecords = Await.result(colStore.scanPartKeys(targetDataset.ref, shard).toListL.runAsync, Duration(1, "minutes"))
      // because there will be 3 records that meets the copier time period.
      partKeyRecords.size shouldEqual 3
      for (pkr <- partKeyRecords) {
        // verify all the records fall in the copier time period.
        // either pkr.startTime or pkr.endTime should fall in the startTime:endTime window
        var metric = getPartKeyMap(pkr).get("_metric_").get
        val startTimeFallsInWindow = pkr.startTime >= startTime && pkr.startTime <= endTime
        val endTimeFallsInWindow = pkr.endTime >= startTime && pkr.endTime <= endTime
        if (startTimeFallsInWindow || endTimeFallsInWindow) {
          assert(true)
        } else {
          assert(false, "Either startTime or endTime doesn't fall in the migration window.")
        }
      }
    }

    // verify data in pk_by_update_time table
    val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
    for (shard <- 0 until numOfShards) {
      val partKeyRecords = Await.result(colStore.getPartKeysByUpdateHour(targetDataset.ref, shard, updateHour).toListL.runAsync, Duration(1, "minutes"))
      // because there will be 3 records that meets the copier time period.
      partKeyRecords.size shouldEqual 3
      for (pkr <- partKeyRecords) {
        // verify all the records fall in the copier time period.
        // either pkr.startTime or pkr.endTime should fall in the startTime:endTime window
        var metric = getPartKeyMap(pkr).get("_metric_").get
        val startTimeFallsInWindow = pkr.startTime >= startTime && pkr.startTime <= endTime
        val endTimeFallsInWindow = pkr.endTime >= startTime && pkr.endTime <= endTime
        if (startTimeFallsInWindow || endTimeFallsInWindow) {
          assert(true)
        } else {
          assert(false, "Either startTime or endTime doesn't fall in the migration window.")
        }
      }
    }

    // verify workspace/namespace names in shard=0.
    val shard0 = 0
    val partKeyRecordsShard0 = Await.result(colStore.scanPartKeys(targetDataset.ref, shard0).toListL.runAsync,
      Duration(1, "minutes"))
    partKeyRecordsShard0.size shouldEqual 3
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
    partKeyRecordsShard2.size shouldEqual 3
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
}
package filodb.repair

import java.lang.ref.Reference
import java.nio.ByteBuffer
import java.util

import com.datastax.driver.core.Row
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import com.typesafe.config.ConfigFactory
import filodb.cassandra.DefaultFiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.{GlobalConfig, TestData, store}
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.store.{ChunkSet, ChunkSetInfo}
import filodb.memory.BinaryRegionLarge
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.ZeroCopyUTF8String._
import monix.reactive.Observable
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class ChunkCopierSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  implicit val s = monix.execution.Scheduler.Implicits.global

  val configPath = "conf/timeseries-filodb-server.conf"

  val sysConfig = GlobalConfig.systemConfig.getConfig("filodb")
  val config = ConfigFactory.parseFile(new java.io.File(configPath))
    .getConfig("filodb").withFallback(sysConfig)

  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session

  it ("should run a simple Spark job") {
    // This test verifies that the configuration can be read and that Spark runs. A full test
    // that verifies chunks are copied correctly is found in CassandraColumnStoreSpec.
    val colStore = new CassandraColumnStore(config, s, session)

    val sourceDataset = Dataset("prometheus", Schemas.gauge)
    val targetDataset = Dataset("buddy_prometheus", Schemas.gauge)

    colStore.initialize(sourceDataset.ref, 1).futureValue
    colStore.truncate(sourceDataset.ref, 1).futureValue

    colStore.initialize(targetDataset.ref, 1).futureValue
    colStore.truncate(targetDataset.ref, 1).futureValue

    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)
    var partBuilder = new RecordBuilder(TestData.nativeMem)
    val partKey1 = partBuilder.partKeyFromObjects(Schemas.gauge, "stuff", seriesTags)
    partBuilder = new RecordBuilder(TestData.nativeMem)
    val partKey2 = partBuilder.partKeyFromObjects(Schemas.gauge, "more_stuff", seriesTags)

    // Fake time and value columns.
    val chunks = new ArrayBuffer[ByteBuffer]()
    chunks += ByteBuffer.allocate(1000)
    chunks += ByteBuffer.allocate(1000)

    val infoBuf = ByteBuffer.allocateDirect(100)
    val infoPtr = UnsafeUtils.addressFromDirectBuffer(infoBuf)
    val info = ChunkSetInfo(infoPtr)

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

    def writeChunk(partKey: Long, timeMillis: Long) = {
      infoBuf.clear()

      ChunkSetInfo.setChunkID(infoPtr, store.chunkID(timeMillis, timeMillis))
      ChunkSetInfo.setIngestionTime(infoPtr, timeMillis)
      ChunkSetInfo.setNumRows(infoPtr, 10) // fake
      ChunkSetInfo.setEndTime(infoPtr, timeMillis + 1) // fake

      val set = ChunkSet(info, partKey, Nil, chunks)

      colStore.write(sourceDataset.ref, Observable.fromIterable(Iterable(set))).futureValue
    }

    val repairTimestamps = Array(1602561600000L, 1602554400000L, 1602561600001L)

    writeChunk(partKey1, 1507923801000L)
    writeChunk(partKey1, 1602554400000L) // repair
    writeChunk(partKey1, 1602561600000L) // repair
    writeChunk(partKey1, 1602561600001L) // repair
    writeChunk(partKey1, 1609855200000L)
    writeChunk(partKey1, 1610028000000L)

    writeChunk(partKey2, 1507923801000L)
    writeChunk(partKey2, 1602554400000L) // repair
    writeChunk(partKey2, 1602561600000L) // repair
    writeChunk(partKey2, 1609855200000L)

    // Ensure that GC doesn't reclaim the native memory too soon.
    Reference.reachabilityFence(infoBuf)

    val part1Bytes = ByteBuffer.wrap(BinaryRegionLarge.asNewByteArray(partKey1))
    val part2Bytes = ByteBuffer.wrap(BinaryRegionLarge.asNewByteArray(partKey2))

    val sourceChunks1 = colStore.getOrCreateChunkTable(sourceDataset.ref).readAllChunksNoAsync(part1Bytes).all()
    sourceChunks1 should have size (6)
    val repairChunks1 = util.Arrays.asList(sourceChunks1.get(1), sourceChunks1.get(2),sourceChunks1.get(3))

    val sourceChunks2 = colStore.getOrCreateChunkTable(sourceDataset.ref).readAllChunksNoAsync(part2Bytes).all()
    sourceChunks2 should have size (4)
    val repairChunks2 = util.Arrays.asList(sourceChunks1.get(1), sourceChunks1.get(2))

    // Expect 0 chunk records in the target at this point.
    var chunks1 = colStore.getOrCreateChunkTable(targetDataset.ref).readAllChunksNoAsync(part1Bytes).all()
    chunks1 should have size (0)
    var chunks2 = colStore.getOrCreateChunkTable(targetDataset.ref).readAllChunksNoAsync(part2Bytes).all()
    chunks2 should have size (0)

    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")

    sparkConf.set("spark.filodb.chunkcopier.source.configFile", configPath)
    sparkConf.set("spark.filodb.chunkcopier.source.dataset", "prometheus")

    sparkConf.set("spark.filodb.chunkcopier.target.configFile", configPath)
    sparkConf.set("spark.filodb.chunkcopier.target.dataset", "buddy_prometheus")

    sparkConf.set("spark.filodb.chunkcopier.ingestionTimeStart", "2020-10-13T00:00:00Z")
    sparkConf.set("spark.filodb.chunkcopier.ingestionTimeEnd",   "2020-10-13T05:00:00Z")

    ChunkCopierMain.run(sparkConf).close()

    def checkRow(a: Row, b: Row): Unit = {
      val size = a.getColumnDefinitions().size()
      b.getColumnDefinitions().size() shouldBe size
      for (i <- 0 to size - 1) {
        a.getObject(i) shouldBe b.getObject(i)
      }
    }

    def checkRows(a: java.util.List[Row], b: java.util.List[Row]): Unit = {
      val size = a.size()
      size shouldBe b.size()
      for (i <- 0 to size - 1) {
        checkRow(a.get(i), b.get(i))
      }
    }

    chunks1 = colStore.getOrCreateChunkTable(targetDataset.ref).readAllChunksNoAsync(part1Bytes).all()
    checkRows(repairChunks1, chunks1)
    chunks2 = colStore.getOrCreateChunkTable(targetDataset.ref).readAllChunksNoAsync(part2Bytes).all()
    checkRows(repairChunks2, chunks2)

    val sourceIndex1 = colStore.getOrCreateIngestionTimeIndexTable(sourceDataset.ref).readAllRowsNoAsync(part1Bytes).all()
    sourceIndex1 should have size (6)
    val expectedIndex1 = util.Arrays.asList(sourceIndex1.get(1), sourceIndex1.get(2), sourceIndex1.get(3))
    val sourceIndex2 = colStore.getOrCreateIngestionTimeIndexTable(sourceDataset.ref).readAllRowsNoAsync(part2Bytes).all()
    sourceIndex2 should have size (4)
    val expectedIndex2 = util.Arrays.asList(sourceIndex2.get(1), sourceIndex2.get(2))

    val index1 = colStore.getOrCreateIngestionTimeIndexTable(targetDataset.ref).readAllRowsNoAsync(part1Bytes).all()
    checkRows(expectedIndex1, index1)
    val index2 = colStore.getOrCreateIngestionTimeIndexTable(targetDataset.ref).readAllRowsNoAsync(part2Bytes).all()
    checkRows(expectedIndex2, index2)
  }
}

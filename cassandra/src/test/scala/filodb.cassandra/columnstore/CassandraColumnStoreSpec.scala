package filodb.cassandra.columnstore

import java.lang.ref.Reference
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import com.datastax.driver.core.Row
import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import filodb.cassandra.DefaultFiloSessionProvider
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.store.{ChunkSet, ChunkSetInfo, ColumnStoreSpec, PartKeyRecord}
import filodb.memory.{BinaryRegionLarge, NativeMemoryManager}
import filodb.memory.format.{TupleRowReader, UnsafeUtils}
import filodb.memory.format.ZeroCopyUTF8String._

import java.nio.charset.StandardCharsets

class CassandraColumnStoreSpec extends ColumnStoreSpec {
  import NamesTestData._

  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session
  lazy val colStore = new CassandraColumnStore(config, s, session)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"), session)

  val nativeMemoryManager = new NativeMemoryManager(100000000L, Map.empty)
  val promDataset = Dataset("prometheus", Schemas.gauge)

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    colStore.initialize(promDataset.ref, 1).futureValue
    colStore.truncate(promDataset.ref, 1).futureValue
  }

  override def afterAll(): Unit = {
    super.afterAll()
    nativeMemoryManager.shutdown()
  }

  "getScanSplits" should "return splits from Cassandra" in {
    // Single split, token_start should equal token_end
    val singleSplits = colStore.getScanSplits(dataset.ref).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    singleSplits should have length (2)
    val split = singleSplits.head
    split.tokens should have length (1)
    // below condition not true for 2 splits, hence commented out. Not validating tokens since it may depend on cluster
//    split.tokens.head._1 should equal (split.tokens.head._2)
    split.replicas.size should equal (1)

    // Multiple splits.  Each split token start/end should not equal each other.
    val multiSplit = colStore.getScanSplits(dataset.ref, 2).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    multiSplit should have length (2)
    multiSplit.foreach { split =>
      split.tokens.head._1 should not equal (split.tokens.head._2)
      split.replicas.size should equal (1)
    }
  }

  "PartKey Reads, Writes and Deletes" should "work" in {
    val dataset = Dataset("prometheus", Schemas.gauge).ref

    colStore.initialize(dataset, 1).futureValue
    colStore.truncate(dataset, 1).futureValue

    val pks = (10000 to 30000).map(_.toString.getBytes(StandardCharsets.UTF_8))
                              .zipWithIndex.map { case (pk, i) => PartKeyRecord(pk, 5, 10, Some(i))}.toSet

    val updateHour = 10
    colStore.writePartKeys(dataset, 0, Observable.fromIterable(pks), 1.hour.toSeconds.toInt, 10, true )
      .futureValue shouldEqual Success

    val expectedKeys = pks.map(pk => new String(pk.partKey, StandardCharsets.UTF_8).toInt)

    val readData = colStore.getPartKeysByUpdateHour(dataset, 0, updateHour).toListL.runAsync.futureValue.toSet
    readData.map(pk => new String(pk.partKey, StandardCharsets.UTF_8).toInt) shouldEqual expectedKeys

    val readData2 = colStore.scanPartKeys(dataset, 0).toListL.runAsync.futureValue.toSet
    readData2.map(pk => new String(pk.partKey, StandardCharsets.UTF_8).toInt) shouldEqual expectedKeys

    val numDeleted = colStore.deletePartKeys(dataset, 0,
      Observable.fromIterable(readData2.map(_.partKey))).futureValue
    numDeleted shouldEqual readData2.size

    val readData3 = colStore.scanPartKeys(dataset, 0).toListL.runAsync.futureValue
    readData3.isEmpty shouldEqual true
  }

  "copyOrDeleteChunksByIngestionTimeRange" should "actually work" in {
    val dataset = Dataset("source", Schemas.gauge)

    colStore.initialize(dataset.ref, 1).futureValue
    colStore.truncate(dataset.ref, 1).futureValue

    val targetConfigPath = "spark-jobs/src/test/resources/timeseries-filodb-buddy-server.conf"
    val targetConfig = ConfigFactory.parseFile(new java.io.File(targetConfigPath))
      .getConfig("filodb").withFallback(GlobalConfig.systemConfig.getConfig("filodb"))
    val targetSession = new DefaultFiloSessionProvider(targetConfig.getConfig("cassandra")).session
    val targetColStore = new CassandraColumnStore(targetConfig, s, targetSession)

    targetColStore.initialize(dataset.ref, 1).futureValue
    targetColStore.truncate(dataset.ref, 1).futureValue

    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)

    var partBuilder = new RecordBuilder(TestData.nativeMem)
    val partKey1 = partBuilder.partKeyFromObjects(Schemas.gauge, "stuff", seriesTags)
    partBuilder = new RecordBuilder(TestData.nativeMem)
    val partKey2 = partBuilder.partKeyFromObjects(Schemas.gauge, "more_stuff", seriesTags)

    val startTimeMillis = System.currentTimeMillis()
    var timeMillis = startTimeMillis
    var partKey = partKey1

    // Fake time and value columns.
    val chunks = new ArrayBuffer[ByteBuffer]()
    chunks += ByteBuffer.allocate(1000)
    chunks += ByteBuffer.allocate(1000)

    val infoBuf = ByteBuffer.allocateDirect(100)
    val infoPtr = UnsafeUtils.addressFromDirectBuffer(infoBuf)
    val info = ChunkSetInfo(infoPtr)
    try {
      for (i <- 1 to 20) {
        if (i == 10) {
          partKey = partKey2
        }

        infoBuf.clear()

        ChunkSetInfo.setChunkID(infoPtr, store.chunkID(timeMillis, timeMillis))
        ChunkSetInfo.setIngestionTime(infoPtr, timeMillis)
        ChunkSetInfo.setNumRows(infoPtr, 10) // fake
        ChunkSetInfo.setEndTime(infoPtr, timeMillis + 1) // fake

        val set = ChunkSet(info, partKey, Nil, chunks)

        colStore.write(dataset.ref, Observable.fromIterable(Iterable(set))).futureValue

        timeMillis += 3600 * 1000
      }
    } finally {
      // Ensure that GC doesn't reclaim the native memory too soon.
      Reference.reachabilityFence(infoBuf)
    }

    val part1Bytes = ByteBuffer.wrap(BinaryRegionLarge.asNewByteArray(partKey1))
    val part2Bytes = ByteBuffer.wrap(BinaryRegionLarge.asNewByteArray(partKey2))

    val expectChunks1 = colStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part1Bytes).all()
    expectChunks1 should have size (9)
    val expectChunks2 = colStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part2Bytes).all()
    expectChunks2 should have size (11)

    // Expect 0 chunk records in the target at this point.
    var chunks1 = targetColStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part1Bytes).all()
    chunks1 should have size (0)
    var chunks2 = targetColStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part2Bytes).all()
    chunks2 should have size (0)

    colStore.copyOrDeleteChunksByIngestionTimeRange(
      dataset.ref,
      colStore.getScanSplits(dataset.ref).iterator,
      startTimeMillis, // ingestionTimeStart
      timeMillis,      // ingestionTimeEnd
      7,         // batchSize
      targetColStore,  // target
      3600 * 24 * 10) // diskTimeToLiveSeconds

    // Verify the copy.

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

    chunks1 = targetColStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part1Bytes).all()
    checkRows(expectChunks1, chunks1)
    chunks2 = targetColStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part2Bytes).all()
    checkRows(expectChunks2, chunks2)

    val expectIndex1 = colStore.getOrCreateIngestionTimeIndexTable(dataset.ref).readAllRowsNoAsync(part1Bytes).all()
    expectIndex1 should have size (9)
    val expectIndex2 = colStore.getOrCreateIngestionTimeIndexTable(dataset.ref).readAllRowsNoAsync(part2Bytes).all()
    expectIndex2 should have size (11)

    var index1 = targetColStore.getOrCreateIngestionTimeIndexTable(dataset.ref).readAllRowsNoAsync(part1Bytes).all()
    checkRows(expectIndex1, index1)
    var index2 = targetColStore.getOrCreateIngestionTimeIndexTable(dataset.ref).readAllRowsNoAsync(part2Bytes).all()
    checkRows(expectIndex2, index2)

    // Now delete from the source.

    colStore.copyOrDeleteChunksByIngestionTimeRange(
      dataset.ref,
      colStore.getScanSplits(dataset.ref).iterator,
      startTimeMillis, // ingestionTimeStart
      timeMillis,      // ingestionTimeEnd
      7,         // batchSize
      targetColStore,  // target
      0) // diskTimeToLiveSeconds is 0, which means delete

    chunks1 = targetColStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part1Bytes).all()
    chunks1 should have size (0)
    chunks2 = targetColStore.getOrCreateChunkTable(dataset.ref).readAllChunksNoAsync(part2Bytes).all()
    chunks2 should have size (0)

    index1 = targetColStore.getOrCreateIngestionTimeIndexTable(dataset.ref).readAllRowsNoAsync(part1Bytes).all()
    index1 should have size (0)
    index2 = targetColStore.getOrCreateIngestionTimeIndexTable(dataset.ref).readAllRowsNoAsync(part2Bytes).all()
    index2 should have size (0)
  }

  val configWithChunkCompress = ConfigFactory.parseString("cassandra.lz4-chunk-compress = true")
                                             .withFallback(config)
  val compressSession = new DefaultFiloSessionProvider(configWithChunkCompress.getConfig("cassandra")).session
  val lz4ColStore = new CassandraColumnStore(configWithChunkCompress, s, compressSession)

  "lz4-chunk-compress" should "write and read compressed chunks successfully" in {
    whenReady(lz4ColStore.write(dataset.ref, chunkSetStream(names take 3))) { response =>
      response should equal (Success)
    }

    val sourceChunks = chunkSetStream(names take 3).toListL.runAsync.futureValue

    val parts = lz4ColStore.readRawPartitions(dataset.ref, 0.millis.toMillis, partScan).toListL.runAsync.futureValue
    parts should have length (1)
    parts(0).chunkSetsTimeOrdered should have length (1)
    parts(0).chunkSetsTimeOrdered(0).vectors.toSeq shouldEqual sourceChunks.head.chunks
  }

  "getChunksByIngestionTimeRangeNoAsync" should "batch partitions properly" in {

    val gaugeName = "my_gauge"
    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8)
    val firstSampleTime = 74373042000L
    val partBuilder = new RecordBuilder(nativeMemoryManager)
    val ingestTime = 1594130687316L
    val chunksets = for {
      i <- 0 until 1050
      c <- 0 until 3
      partKey = partBuilder.partKeyFromObjects(Schemas.gauge, gaugeName + i, seriesTags)
    } yield {
      val rows = Seq(TupleRowReader((Some(firstSampleTime + c), Some(0.0d))))
      ChunkSet(Schemas.gauge.data, partKey, ingestTime, rows, nativeMemoryManager)
    }
    colStore.write(promDataset.ref, Observable.fromIterable(chunksets)).futureValue

    val batches = colStore.getChunksByIngestionTimeRangeNoAsync(
      promDataset.ref,
      colStore.getScanSplits(promDataset.ref).iterator,
      ingestTime - 1,
      ingestTime + 1,
      firstSampleTime - 1,
      firstSampleTime + 5,
      10L,
      100,
      5000
    ).toList

    batches.size shouldEqual 11 // 100 rows per batch, 1050 rows => 11 batches
    batches.zipWithIndex.foreach { case (b, i) =>
      b.size shouldEqual (if (i == 10) 50 else 100)
      b.foreach(_.chunkSetsTimeOrdered.size shouldEqual 3)
    }
  }
}

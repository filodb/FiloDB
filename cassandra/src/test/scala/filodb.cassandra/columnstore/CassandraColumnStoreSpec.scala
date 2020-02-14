package filodb.cassandra.columnstore

import scala.concurrent.duration._

import java.lang.ref.Reference
import java.nio.ByteBuffer

import com.typesafe.config.ConfigFactory
import monix.reactive.Observable

import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.core.metadata.{Dataset, Schemas}
import filodb.core.store.{ChunkSet, ChunkSetInfo}
import filodb.core.store.ColumnStoreSpec
import filodb.cassandra.metastore.CassandraMetaStore

class CassandraColumnStoreSpec extends ColumnStoreSpec {
  import NamesTestData._

  lazy val colStore = new CassandraColumnStore(config, s)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))

  "getScanSplits" should "return splits from Cassandra" in {
    // Single split, token_start should equal token_end
    val singleSplits = colStore.getScanSplits(dataset.ref).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    singleSplits should have length (1)
    val split = singleSplits.head
    split.tokens should have length (1)
    split.tokens.head._1 should equal (split.tokens.head._2)
    split.replicas.size should equal (1)

    // Multiple splits.  Each split token start/end should not equal each other.
    val multiSplit = colStore.getScanSplits(dataset.ref, 2).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    multiSplit should have length (2)
    multiSplit.foreach { split =>
      split.tokens.head._1 should not equal (split.tokens.head._2)
      split.replicas.size should equal (1)
    }
  }

  "copyChunksByIngestionTimeRange" should "actually work" in {
    val dataset = Dataset("source", Schemas.gauge)
    val targetDataset = Dataset("target", Schemas.gauge)

    colStore.initialize(dataset.ref, 1).futureValue
    colStore.truncate(dataset.ref, 1).futureValue

    colStore.initialize(targetDataset.ref, 1).futureValue
    colStore.truncate(targetDataset.ref, 1).futureValue

    val seriesTags = Map("_ws_".utf8 -> "my_ws".utf8, "_ns_".utf8 -> "my_ns".utf8)

    val partBuilder = new RecordBuilder(TestData.nativeMem)
    val partKey = partBuilder.partKeyFromObjects(Schemas.gauge, "stuff", seriesTags)

    val infoBuf = ByteBuffer.allocateDirect(100)
    val infoPtr = UnsafeUtils.addressFromDirectBuffer(infoBuf)
    val info = ChunkSetInfo(infoPtr)
    try {
      infoBuf.clear()

      // FIXME final def chunkID(startTime: Long, ingestionTime: Long): Long

      ChunkSetInfo.setChunkID(infoPtr, 0) // FIXME
      ChunkSetInfo.setIngestionTime(infoPtr, 0) // FIXME
      ChunkSetInfo.setNumRows(infoPtr, 10) // FIXME (just keep lying?)
      ChunkSetInfo.setEndTime(infoPtr, 10) // FIXME

      val set = ChunkSet(info, partKey, Nil, Nil) // FIXME: last is chunks: Seq[ByteBuffer]

      System.out.print("yo\n")
      colStore.write(dataset.ref, Observable.fromIterable(Iterable(set))).futureValue
      System.out.print("yo!\n")
    } finally {
      // Ensure that GC doesn't reclaim the native memory too soon.
      Reference.reachabilityFence(infoBuf)
    }

    colStore.copyChunksByIngestionTimeRange(
      dataset.ref,
      colStore.getScanSplits(dataset.ref).iterator,
      0,
      0,
      1,
      colStore,
      targetDataset.ref,
      1)

    /*
    colStore.copyChunksByIngestionTimeRange(
      datasetRef,
      splits: Iterator[ScanSplit],
      ingestionTimeStart: Long,
      ingestionTimeEnd: Long,
      batchSize: Int,
      target: CassandraColumnStore,
      targetDatasetRef: DatasetRef,
      diskTimeToLiveSeconds: Int)
     */
  }

  val configWithChunkCompress = ConfigFactory.parseString("cassandra.lz4-chunk-compress = true")
                                             .withFallback(config)
  val lz4ColStore = new CassandraColumnStore(configWithChunkCompress, s)

  "lz4-chunk-compress" should "write and read compressed chunks successfully" in {
    whenReady(lz4ColStore.write(dataset.ref, chunkSetStream(names take 3))) { response =>
      response should equal (Success)
    }

    val sourceChunks = chunkSetStream(names take 3).toListL.runAsync.futureValue

    val parts = lz4ColStore.readRawPartitions(dataset.ref, 0.millis.toMillis, partScan).toListL.runAsync.futureValue
    parts should have length (1)
    parts(0).chunkSets should have length (1)
    parts(0).chunkSets(0).vectors.toSeq shouldEqual sourceChunks.head.chunks
  }
}

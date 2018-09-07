package filodb.cassandra.columnstore

import com.typesafe.config.ConfigFactory

import filodb.core._
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

  val configWithChunkCompress = ConfigFactory.parseString("cassandra.lz4-chunk-compress = true")
                                             .withFallback(config)
  val lz4ColStore = new CassandraColumnStore(configWithChunkCompress, s)

  "lz4-chunk-compress" should "write and read compressed chunks successfully" in {
    whenReady(lz4ColStore.write(dataset, chunkSetStream(names take 3))) { response =>
      response should equal (Success)
    }

    val sourceChunks = chunkSetStream(names take 3).toListL.runAsync.futureValue

    val parts = lz4ColStore.readRawPartitions(dataset, Seq(0, 1, 2), partScan).toListL.runAsync.futureValue
    parts should have length (1)
    parts(0).chunkSets should have length (1)
    parts(0).chunkSets(0).vectors.toSeq shouldEqual sourceChunks.head.chunks
  }
}
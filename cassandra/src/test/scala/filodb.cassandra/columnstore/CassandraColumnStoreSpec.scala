package filodb.cassandra.columnstore

import com.typesafe.config.ConfigFactory
import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.store.ColumnStoreSpec
import filodb.core.Types

class CassandraColumnStoreSpec extends ColumnStoreSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  import filodb.core.store._
  import NamesTestData._

  val colStore = new CassandraColumnStore(config, global)

  "getScanSplits" should "return splits from Cassandra" in {
    // Single split, token_start should equal token_end
    val singleSplits = colStore.getScanSplits(datasetRef).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    singleSplits should have length (1)
    val split = singleSplits.head
    split.tokens should have length (1)
    split.tokens.head._1 should equal (split.tokens.head._2)
    split.replicas.size should equal (1)

    // Multiple splits.  Each split token start/end should not equal each other.
    val multiSplit = colStore.getScanSplits(datasetRef, 2).asInstanceOf[Seq[CassandraTokenRangeSplit]]
    multiSplit should have length (2)
    multiSplit.foreach { split =>
      split.tokens.head._1 should not equal (split.tokens.head._2)
      split.replicas.size should equal (1)
    }
  }

  val configWithChunkCompress = ConfigFactory.parseString("cassandra.lz4-chunk-compress = true")
                                             .withFallback(config)
  val lz4ColStore = new CassandraColumnStore(configWithChunkCompress, global)

  "lz4-chunk-compress" should "append new rows to a cached segment successfully" in {
    val state = getState()
    val segment = getWriterSegment()
    segment.addChunkSet(state, mapper(names take 3))
    whenReady(lz4ColStore.appendSegment(projection, segment, 0)) { response =>
      response should equal (Success)
    }

    val chunk = lz4ColStore.scanChunks(projection, schema, 0, partScan).toSeq.head
    chunk.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L))
    chunk.rowIterator().map(_.filoUTF8String(0)).toSeq should equal (utf8FirstNames take 3)
  }


}
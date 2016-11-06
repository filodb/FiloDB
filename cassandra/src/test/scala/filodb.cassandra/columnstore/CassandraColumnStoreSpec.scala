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

    // Writing segment2, last 3 rows, should get appended to first 3 in same segment
    val segment2 = getWriterSegment()
    segment2.addChunkSet(state, mapper(names drop 3))
    whenReady(lz4ColStore.appendSegment(projection, segment2, 0)) { response =>
      response should equal (Success)
    }

    whenReady(lz4ColStore.scanSegments(projection, schema, 0, partScan)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal (segment.segInfo.segment)
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal (Seq(24L, 28L, 25L, 40L, 39L, 29L))
      readSeg.rowIterator().map(_.getString(0)).toSeq should equal (firstNames)
    }
  }


}
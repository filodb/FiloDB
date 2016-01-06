package filodb.cassandra.columnstore

import com.websudos.phantom.testkit._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.store.ColumnStoreSpec
import filodb.core.Types

class CassandraColumnStoreSpec extends CassandraFlatSpec with ColumnStoreSpec {
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.websudos.phantom.dsl._
  import filodb.core.store._
  import SegmentSpec._

  val colStore = new CassandraColumnStore(config)
  implicit val keySpace = KeySpace(config.getString("cassandra.keyspace"))

  val (chunkTable, rowMapTable) =
    Await.result(colStore.asInstanceOf[CassandraColumnStore].getSegmentTables(dataset), 3 seconds)

  private def getChunkRowMap[K](segment: Segment[K]): Future[BinaryChunkRowMap] =
    rowMapTable.getChunkMaps(segment.partition, 0, segment.segmentId, segment.keyRange.binaryEnd).
                collect {
      case Seq(ChunkRowMapRecord(_, chunkIds, rowNums, nextId)) =>
        new BinaryChunkRowMap(chunkIds, rowNums, nextId)
      case x: Seq[_] => throw new RuntimeException("Got back unexpected chunkMaps " + x)
    }

  // NOTE: The test below purposefully does not use any of the read APIs so that if only the read code
  // breaks, this test can independently test for write failures
  "appendSegment" should "write a segment into an empty table" in {
    whenReady(colStore.appendSegment(projection, baseSegment, 0)) { response =>
      response should equal (Success)
    }

    whenReady(chunkTable.select(_.columnName, _.chunkId, _.data).fetch()) { data =>
      data should equal (Seq(("columnA", 0, bytes1),
                             ("columnA", 1, bytes1),
                             ("columnB", 0, bytes2),
                             ("columnB", 1, bytes2)))
    }

    whenReady(getChunkRowMap(baseSegment)) { binRowMap =>
      binRowMap.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 1))
      binRowMap.rowNumIterator.toSeq should equal (Seq(0, 1, 2, 0))
    }
  }

  "getScanSplits" should "return splits from Cassandra" in {
    // Single split, token_start should equal token_end
    val singleSplit = colStore.getScanSplits(dataset)
    singleSplit should have length (1)
    singleSplit.head("token_start") should equal (singleSplit.head("token_end"))
    singleSplit.head("replicas").split(",").size should equal (1)

    // Multiple splits.  Each split token start/end should not equal each other.
    val multiSplit = colStore.getScanSplits(dataset, Map("splits_per_node" -> "2"))
    multiSplit should have length (2)
    multiSplit.foreach { split =>
      split("token_start") should not equal (split("token_end"))
      split("replicas").split(",").size should equal (1)
    }
  }
}
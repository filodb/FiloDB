package filodb.core.cassandra

import com.typesafe.config.ConfigFactory
import com.websudos.phantom.testkit._
import java.nio.ByteBuffer
import org.scalatest.BeforeAndAfter
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import filodb.core.messages._
import filodb.core.datastore2.Types

class CassandraColumnStoreSpec extends CassandraFlatSpec with BeforeAndAfter {
  import scala.concurrent.ExecutionContext.Implicits.global
  import com.websudos.phantom.dsl._
  import filodb.core.datastore2._

  implicit val keySpace = KeySpace("unittest")
  val colStore = new CassandraColumnStore(ConfigFactory.load())
  val dataset = "foo"

  val (chunkTable, rowMapTable) = Await.result(colStore.getSegmentTables(dataset), 3 seconds)

  // First create the tables in C*
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(chunkTable.create.ifNotExists.future(), 3 seconds)
    Await.result(rowMapTable.create.ifNotExists.future(), 3 seconds)
  }

  before {
    Await.result(chunkTable.truncate.future(), 3 seconds)
    Await.result(rowMapTable.truncate.future(), 3 seconds)
    colStore.clearRowMapCache()
  }

  implicit val keyHelper = TimestampKeyHelper(10000L)
  val keyRange = KeyRange(dataset, "partition", 0L, 10000L)

  val bytes1 = ByteBuffer.wrap("apple".getBytes("UTF-8"))
  val bytes2 = ByteBuffer.wrap("orange".getBytes("UTF-8"))

  val rowIndex = new UpdatableChunkRowMap[Long]

  val baseSegment = new GenericSegment(keyRange, rowIndex)
  baseSegment.addChunks(0, Map("columnA" -> bytes1, "columnB" -> bytes2))
  baseSegment.addChunks(1, Map("columnA" -> bytes1, "columnB" -> bytes2))
  rowIndex.index = rowIndex.index ++
                     Map(500L -> (0 -> 0), 1000L -> (1 -> 0), 600L -> (0 -> 1), 700L -> (0 -> 2))

  private def getChunkRowMap[K](segment: Segment[K]): Future[BinaryChunkRowMap] =
    rowMapTable.getChunkMaps(segment.partition, 0, segment.segmentId, segment.keyRange.binaryEnd).
                collect {
      case Seq(ChunkRowMapRecord(_, chunkIds, rowNums, _)) => new BinaryChunkRowMap(chunkIds, rowNums)
      case x: Seq[_] => throw new RuntimeException("Got back unexpected chunkMaps " + x)
    }

  "appendSegment" should "write a segment into an empty table" in {
    whenReady(colStore.appendSegment(baseSegment, 0)) { response =>
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

  "readSegments" should "read segments back that were written" in {
    whenReady(colStore.appendSegment(baseSegment, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.readSegments(Set("columnA", "columnB"), keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      segments.head.keyRange should equal (baseSegment.keyRange)
      segments.head.getChunks.toSet should equal (baseSegment.getChunks.toSet)
      segments.head.index.rowNumIterator.toSeq should equal (baseSegment.index.rowNumIterator.toSeq)
    }
  }

  it should "return empty iterator if cannot find segment" in {
    whenReady(colStore.readSegments(Set("columnA", "columnB"), keyRange, 0)) { segIter =>
      segIter.toSeq should have length (0)
    }
  }

  it should "return empty segment if cannot find columns" in {
    whenReady(colStore.appendSegment(baseSegment, 0)) { response =>
      response should equal (Success)
    }

    whenReady(colStore.readSegments(Set("notACol"), keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      segments.head.getChunks.toSeq should equal (Nil)
    }
  }
}
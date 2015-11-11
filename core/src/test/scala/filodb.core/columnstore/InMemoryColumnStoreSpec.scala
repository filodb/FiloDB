package filodb.core.columnstore

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter}
import org.scalatest.concurrent.ScalaFutures

// TODO: Test other ColumnStore logic, especially read side and appendSegments
// This tests both ColumnStore and InMemoryColumnStore logic.
class InMemoryColumnStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {
  import SegmentSpec._

  private val toSegmentInfo = (t: (Long, Int)) => SegmentInfo(Some(t._1), None, t._2)

  val aSegments = Seq((10L, 10), (100L, 5), (150L, 20)).map(toSegmentInfo)
  val bSegments = Seq((0L, 3), (180L, 15)).map(toSegmentInfo)
  val sortedSegments = Seq((0L, 3), (10L, 10), (100L, 5), (150L, 20), (180L, 15)).map(toSegmentInfo)

  import scala.concurrent.ExecutionContext.Implicits.global
  val columnStore = new InMemoryColumnStore

  before {
    columnStore.clearSegmentCache()
    columnStore.clearProjectionData(dataset.projections.head)
  }

  describe("PartitionSegments") {
    it("should update partition segments where no dataset/partition existed") {
      val resp = columnStore.updatePartitionSegments(projection, 0, "A", 0, aSegments).futureValue
      resp shouldBe a [SegmentsUpdated]
      val (uuid, infos) = columnStore.readPartitionSegments(projection, 0, "A").futureValue
      infos should equal (aSegments)
      resp.asInstanceOf[SegmentsUpdated].newUuid should equal (uuid)
    }

    it("should add new partition segments to existing dataset/partition") {
      val resp = columnStore.updatePartitionSegments(projection, 0, "B", 0, aSegments).futureValue
      resp shouldBe a [SegmentsUpdated]

      resp match {
        case SegmentsUpdated(newUuid) =>
          columnStore.updatePartitionSegments(projection, 0, "B", newUuid, bSegments).
            futureValue shouldBe a [SegmentsUpdated]
      }
      val (_, infos2) = columnStore.readPartitionSegments(projection, 0, "B").futureValue
      infos2 should equal (sortedSegments)
    }

    it("should replace existing partition segment info") {
      val resp = columnStore.updatePartitionSegments(projection, 0, "B", 0, aSegments).futureValue
      resp match {
        case SegmentsUpdated(newUuid) =>
          val resp2 = columnStore.updatePartitionSegments(projection, 0, "B", newUuid, aSegments.take(1))
                        .futureValue
          resp2 shouldBe a [SegmentsUpdated]
      }

      val (uuid, infos) = columnStore.readPartitionSegments(projection, 0, "B").futureValue
      infos should equal (aSegments)
    }

    it("should return Nil when existing dataset/partition doesn't exist") {
      val (uuid, infos) = columnStore.readPartitionSegments(projection, 0, "notexists").futureValue
      (uuid, infos) should equal ((0, Nil))
    }

    it("should return NotApplied if prevUuid does not match") {
      columnStore.updatePartitionSegments(projection, 0, "C", 0, aSegments).
        futureValue shouldBe a [SegmentsUpdated]
      val (uuid, infos) = columnStore.readPartitionSegments(projection, 0, "C").futureValue

      columnStore.updatePartitionSegments(projection, 0, "C", uuid + 111, bSegments).futureValue should
          equal (NotApplied)
    }
  }
}
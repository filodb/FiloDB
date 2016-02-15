package filodb.core.store

import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures

trait MetaStoreSpec extends FunSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  import MetaStore._

  def metaStore: MetaStore
  def defaultPatience: PatienceConfig

  override def beforeAll() {
    super.beforeAll()
    metaStore.initialize().futureValue(defaultPatience)
  }

  before { metaStore.clearAllData().futureValue(defaultPatience) }

  describe("dataset API") {
    it("should create a new Dataset if one not there") {
      val dataset = Dataset("foo", Seq("key1", ":getOrElse key2 --"), "seg",
                            Seq("part1", ":getOrElse part2 00"))
      metaStore.newDataset(dataset).futureValue should equal (Success)

      metaStore.getDataset("foo").futureValue should equal (dataset)
    }

    it("should return AlreadyExists if dataset already exists") {
      val dataset = Dataset("foo", "autoid", "seg")
      metaStore.newDataset(dataset).futureValue should equal (Success)
      metaStore.newDataset(dataset).futureValue should equal (AlreadyExists)
    }

    it("should return NotFound if getDataset on nonexisting dataset") {
      metaStore.getDataset("notThere").failed.futureValue shouldBe a [NotFoundError]
    }
  }

  describe("column API") {
    it("should return IllegalColumnChange if an invalid column addition submitted") {
      val firstColumn = DataColumn(0, "first", "foo", 1, Column.ColumnType.StringColumn)
      whenReady(metaStore.newColumn(firstColumn)) { response =>
        response should equal (Success)
      }

      whenReady(metaStore.newColumn(firstColumn.copy(version = 0)).failed) { err =>
        err shouldBe an [IllegalColumnChange]
      } (patienceConfig)
    }

    val monthYearCol = DataColumn(1, "monthYear", "gdelt", 1, Column.ColumnType.LongColumn)
    it("should be able to create a Column and get the Schema") {
      metaStore.newColumn(monthYearCol).futureValue should equal (Success)
      metaStore.getSchema("gdelt", 10).futureValue should equal (Map("monthYear" -> monthYearCol))
    }
  }

}
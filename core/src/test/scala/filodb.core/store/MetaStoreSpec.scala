package filodb.core.store

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}

trait MetaStoreSpec extends FunSpec with Matchers
  with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {

  import MetaStore._

  def metaStore: MetaStore
  implicit def defaultPatience: PatienceConfig

  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  val fooRef = DatasetRef("foo", Some("unittest"))

  before { metaStore.clearAllData().futureValue }

  describe("dataset API") {
    it("should create a new Dataset if one not there") {
      val dataset = Dataset(fooRef, Seq("key1", ":getOrElse key2 --"),
                            Seq("part1", ":getOrElse part2 00"))
      metaStore.newDataset(dataset).futureValue should equal (Success)

      metaStore.getDataset(fooRef).futureValue should equal (dataset)
    }

    it("should return AlreadyExists if dataset already exists") {
      val dataset = Dataset("foo", "autoid")
      metaStore.newDataset(dataset).futureValue should equal (Success)
      metaStore.newDataset(dataset).futureValue should equal (AlreadyExists)
    }

    it("should return NotFound if getDataset on nonexisting dataset") {
      metaStore.getDataset(DatasetRef("notThere")).failed.futureValue shouldBe a[NotFoundError]
    }

    it("should return all datasets created") {
      for { i <- 0 to 2 } {
        val ref = DatasetRef(i.toString, Some((i % 2).toString))
        val dataset = Dataset(ref, Seq("key1", ":getOrElse key2 --"),
                              Seq("part1", ":getOrElse part2 00"))
        metaStore.newDataset(dataset).futureValue should equal (Success)
      }

      metaStore.getAllDatasets(Some("0")).futureValue.toSet should equal (
        Set(DatasetRef("0", Some("0")), DatasetRef("2", Some("0"))))
      metaStore.getAllDatasets(None).futureValue.toSet should equal (
        Set(DatasetRef("0", Some("0")), DatasetRef("1", Some("1")), DatasetRef("2", Some("0"))))
    }
  }

  describe("column API") {
    it("should return IllegalColumnChange if an invalid column addition submitted") {
      val firstColumn = DataColumn(0, "first", "foo", 1, Column.ColumnType.StringColumn)
      whenReady(metaStore.newColumn(firstColumn, fooRef)) { response =>
        response should equal (Success)
      }

      whenReady(metaStore.newColumn(firstColumn.copy(version = 0), fooRef).failed) { err =>
        err shouldBe an[IllegalColumnChange]
      }
    }

    val monthYearCol = DataColumn(1, "monthYear", "gdelt", 1, Column.ColumnType.LongColumn)
    val gdeltRef = DatasetRef("gdelt")

    it("should be able to create a Column and get the Schema") {
      metaStore.newColumn(monthYearCol, gdeltRef).futureValue should equal (Success)
      metaStore.getSchema(gdeltRef, 10).futureValue should equal (Map("monthYear" -> monthYearCol))
    }

    it("should return IllegalColumnChange if some column additions invalid") {
      val firstColumn = DataColumn(0, "first", "foo", 1, Column.ColumnType.StringColumn)
      val secondColumn = DataColumn(1, "second", "foo", 1, Column.ColumnType.IntColumn)
      whenReady(metaStore.newColumn(firstColumn, fooRef)) { response =>
        response should equal (Success)
      }

      val badColumn = firstColumn.copy(version = 0)  // lower version than first column!
      whenReady(metaStore.newColumns(Seq(badColumn, secondColumn), fooRef).failed) { err =>
        err shouldBe an[IllegalColumnChange]
      }
    }

    it("should getColumns and return valid list of columns or UndefinedColumns") {
      val firstColumn = DataColumn(0, "first", "foo", 1, Column.ColumnType.StringColumn)
      val secondColumn = DataColumn(1, "second", "foo", 1, Column.ColumnType.IntColumn)
      metaStore.newColumns(Seq(firstColumn, secondColumn), fooRef).futureValue should equal (Success)

      metaStore.getColumns(fooRef, 1, Seq("second", "first")).futureValue should equal (
                                                                      Seq(secondColumn, firstColumn))
      metaStore.getColumns(fooRef, 1, Seq("second", "baz")).failed.futureValue shouldBe an[UndefinedColumns]
    }

    it("should be able to add many new columns at once") {
      val columns = (0 to 129).map { i => DataColumn(i, i.toString, "foo", 1, Column.ColumnType.IntColumn) }
      metaStore.newColumns(columns, fooRef).futureValue should equal (Success)
    }

    it("deleteDatasets should delete both dataset and columns") {
      val dataset = Dataset("gdelt", "autoid")
      metaStore.newDataset(dataset).futureValue should equal (Success)
      metaStore.newColumn(monthYearCol, gdeltRef).futureValue should equal (Success)

      metaStore.deleteDataset(gdeltRef).futureValue should equal (Success)
      metaStore.getDataset(gdeltRef).failed.futureValue shouldBe a[NotFoundError]
      metaStore.getSchema(gdeltRef, 10).futureValue should equal (Map.empty)
    }
  }
}
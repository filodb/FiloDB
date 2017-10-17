package filodb.core.store

import org.scalatest.{FunSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._
import filodb.core.metadata.Dataset

trait MetaStoreSpec extends FunSpec with Matchers
with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {
  def metaStore: MetaStore
  implicit def defaultPatience: PatienceConfig

  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  val dataset = Dataset("foo", Seq("part:string"), Seq("timestamp:long", "value:double"), "timestamp")

  before { metaStore.clearAllData().futureValue }

  describe("dataset API") {
    it("should write a new Dataset if one doesn't exist") {
      metaStore.newDataset(dataset).futureValue shouldEqual Success
      metaStore.getDataset(dataset.ref).futureValue.copy(database=None) shouldEqual dataset
    }

    it("should return AlreadyExists if try to rewrite dataset with different data") {
      val badDataset = dataset.copy(rowKeyIDs = Nil)   // illegal dataset really
      metaStore.newDataset(dataset).futureValue shouldEqual Success
      metaStore.newDataset(badDataset).futureValue shouldEqual AlreadyExists
      metaStore.getDataset(dataset.ref).futureValue.copy(database=None) shouldEqual dataset
    }

    it("should return NotFound if getDataset on nonexisting dataset") {
      metaStore.getDataset(DatasetRef("notThere")).failed.futureValue shouldBe a[NotFoundError]
    }

    it("should return all datasets created") {
      for { i <- 0 to 2 } {
        val ds = dataset.copy(name = i.toString, database = Some((i % 2).toString))
        metaStore.newDataset(ds).futureValue should equal (Success)
      }

      metaStore.getAllDatasets(Some("0")).futureValue.toSet should equal (
        Set(DatasetRef("0", Some("0")), DatasetRef("2", Some("0"))))
      metaStore.getAllDatasets(None).futureValue.toSet should equal (
        Set(DatasetRef("0", Some("0")), DatasetRef("1", Some("1")), DatasetRef("2", Some("0"))))
    }

    it("deleteDatasets should delete dataset") {
      val ds = dataset.copy(name = "gdelt")
      metaStore.newDataset(ds).futureValue should equal (Success)

      metaStore.deleteDataset(ds.ref).futureValue should equal (Success)
      metaStore.getDataset(ds.ref).failed.futureValue shouldBe a [NotFoundError]
    }
  }
}
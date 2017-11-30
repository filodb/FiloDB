package filodb.core.store

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, Matchers}
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
      metaStore.deleteDataset(ds.ref).futureValue should equal (NotFound)

      metaStore.newDataset(ds).futureValue should equal (Success)

      metaStore.deleteDataset(ds.ref).futureValue should equal (Success)
      metaStore.getDataset(ds.ref).failed.futureValue shouldBe a [NotFoundError]
    }
  }

  describe("checkpoint api") {
    it("should allow reading and writing checkpoints for shard") {
      val ds = dataset.copy(name = "gdelt-" + UUID.randomUUID()) // Add uuid so tests can be rerun

      // when there is no data for a shard, then return Long.MinValue as the checkpoint
      metaStore.readEarliestCheckpoint(ds.ref, 2).futureValue shouldEqual Long.MinValue
      metaStore.readCheckpoints(ds.ref, 2).futureValue.isEmpty shouldBe true

      // should be able to insert checkpoints for new groups
      metaStore.writeCheckpoint(ds.ref, 2, 1, 10).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 2, 9).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 3, 13).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 4, 5).futureValue shouldEqual Success
      metaStore.readEarliestCheckpoint(ds.ref, 2).futureValue shouldEqual 5

      // should be able to update checkpoints for existing groups
      metaStore.writeCheckpoint(ds.ref, 2, 1, 12).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 2, 15).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 3, 17).futureValue shouldEqual Success
      metaStore.writeCheckpoint(ds.ref, 2, 4, 7).futureValue shouldEqual Success
      metaStore.readEarliestCheckpoint(ds.ref, 2).futureValue shouldEqual 7

      // should be able to retrieve raw offsets as well
      val offsets = metaStore.readCheckpoints(ds.ref, 2).futureValue
      offsets.size shouldEqual 4
      offsets(1) shouldEqual 12
      offsets(2) shouldEqual 15
      offsets(3) shouldEqual 17
      offsets(4) shouldEqual 7

      // should be able to clear the table
      metaStore.clearAllData().futureValue
      metaStore.readCheckpoints(ds.ref, 2).futureValue.isEmpty shouldBe true
    }
  }

  describe("IngestionConfig API") {
    val config1 = ConfigFactory.parseString("""brokers=["foo.bar.com:1234", "foo.baz.com:5678"]
                                              |auto.offset.commit=false""".stripMargin)
    val factory1 = "filodb.fake.SomeStore"
    val resource1 = ConfigFactory.parseString("""num-shards=100
                                                 min-num-nodes=16""")
    val resource2 = ConfigFactory.parseString("""num-shards=50
                                                 min-num-nodes=10""")
    val source1 = IngestionConfig(dataset.ref, resource1, factory1, config1)
    val source2 = IngestionConfig(DatasetRef("juju"), resource2, factory1, config1)

    it("should return Nil if no IngestionConfig persisted yet") {
      metaStore.readIngestionConfigs().futureValue shouldEqual Nil
    }

    it("should write and read back IngestionConfigs") {
      metaStore.writeIngestionConfig(source1).futureValue shouldEqual Success
      metaStore.readIngestionConfigs().futureValue shouldEqual Seq(source1)

      metaStore.writeIngestionConfig(source2).futureValue shouldEqual Success
      metaStore.readIngestionConfigs().futureValue.toSet shouldEqual Set(source1, source2)
    }

    it("should be able to delete ingestion configs") {
      metaStore.deleteIngestionConfig(dataset.ref).futureValue shouldEqual NotFound

      metaStore.writeIngestionConfig(source1).futureValue shouldEqual Success
      metaStore.readIngestionConfigs().futureValue shouldEqual Seq(source1)

      metaStore.deleteIngestionConfig(dataset.ref).futureValue shouldEqual Success
      metaStore.readIngestionConfigs().futureValue shouldEqual Nil
    }
  }
}
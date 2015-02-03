package filodb.core.cassandra

import com.websudos.phantom.testing.CassandraFlatSpec
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.metadata.Dataset
import filodb.core.messages._

class DatasetTableSpec extends CassandraFlatSpec with BeforeAndAfter {
  val keySpace = "test"

  // First create the datasets table
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(DatasetTableOps.create.future(), 3 seconds)
  }

  before {
    Await.result(DatasetTableOps.truncate.future(), 3 seconds)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  "DatasetTable" should "create a dataset successfully, then return AlreadyExists" in {
    whenReady(DatasetTableOps.createNewDataset("foo")) { response =>
      response should equal (Success)
    }

    // Second time around, dataset already exists
    whenReady(DatasetTableOps.createNewDataset("foo")) { response =>
      response should equal (AlreadyExists)
    }
  }

  it should "not delete a dataset if it is NotFound" in {
    whenReady(DatasetTableOps.deleteDataset("foo")) { response =>
      response should equal (NotFound)
    }
  }

  it should "delete an empty dataset" in {
    whenReady(DatasetTableOps.createNewDataset("foo")) { response =>
      response should equal (Success)
    }
    whenReady(DatasetTableOps.deleteDataset("foo")) { response =>
      response should equal (Success)
    }
  }

  it should "return StorageEngineException when trying to delete nonempty Dataset" in {
    val f = DatasetTableOps.insert
              .value(_.name, "bar")
              .value(_.partitions, Set("first"))
              .future
    whenReady(f) { result => result.wasApplied should equal (true) }

    whenReady(DatasetTableOps.deleteDataset("bar")) { response =>
      response.getClass should be (classOf[MetadataException])
    }
  }

  it should "return NotFound when trying to get nonexisting dataset" in {
    whenReady(DatasetTableOps.getDataset("foo")) { response =>
      response should equal (NotFound)
    }
  }

  it should "return the Dataset if it exists" in {
    val f = DatasetTableOps.insert
              .value(_.name, "bar")
              .value(_.partitions, Set("first"))
              .future
    whenReady(f) { result => result.wasApplied should equal (true) }

    whenReady(DatasetTableOps.getDataset("bar")) { response =>
      val Dataset.Result(dataset) = response
      dataset.name should equal ("bar")
      dataset.partitions should equal (Set("first"))
    }
  }
}
package filodb.cassandra.metastore

import com.websudos.phantom.dsl._
import com.websudos.phantom.testkit._
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.Dataset

class DatasetTableSpec extends CassandraFlatSpec with BeforeAndAfter {
 implicit val keySpace = KeySpace("unittest")

  // First create the datasets table
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(DatasetTable.create.ifNotExists.future(), 3 seconds)
  }

  before {
    Await.result(DatasetTable.truncate.future(), 3 seconds)
  }

  val fooDataset = Dataset("foo", "someSortCol")

  import scala.concurrent.ExecutionContext.Implicits.global

  "DatasetTable" should "create a dataset successfully, then return AlreadyExists" in {
    whenReady(DatasetTable.createNewDataset(fooDataset)) { response =>
      response should equal (Success)
    }

    // Second time around, dataset already exists
    whenReady(DatasetTable.createNewDataset(fooDataset)) { response =>
      response should equal (AlreadyExists)
    }
  }

  // Apparently, deleting a nonexisting dataset also returns success.  :/

  it should "delete a dataset" in {
    whenReady(DatasetTable.createNewDataset(fooDataset)) { response =>
      response should equal (Success)
    }
    whenReady(DatasetTable.deleteDataset("foo")) { response =>
      response should equal (Success)
    }

    whenReady(DatasetTable.getDataset("foo").failed) { err =>
      err shouldBe a [NotFoundError]
    }
  }

  it should "return NotFoundError when trying to get nonexisting dataset" in {
    whenReady(DatasetTable.getDataset("foo").failed) { err =>
      err shouldBe a [NotFoundError]
    }
  }

  it should "return the Dataset if it exists" in {
    val barDataset = Dataset("bar", "sortCol")
    DatasetTable.createNewDataset(barDataset).futureValue should equal (Success)

    whenReady(DatasetTable.getDataset("bar")) { dataset =>
      dataset should equal (barDataset)
    }
  }
}
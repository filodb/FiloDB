package filodb.cassandra.metastore

import com.typesafe.config.ConfigFactory
import com.websudos.phantom.dsl._
import com.websudos.phantom.testkit._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.Dataset

class DatasetTableSpec extends CassandraFlatSpec with BeforeAndAfter {
  val config = ConfigFactory.load("application_test.conf").getConfig("cassandra")
  val datasetTable = new DatasetTable(config)
  implicit val keySpace = KeySpace(config.getString("keyspace"))

  // First create the datasets table
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    datasetTable.initialize().futureValue(timeout)
  }

  before {
    datasetTable.clearAll().futureValue(timeout)
  }

  val fooDataset = Dataset("foo", "someSortCol")
  val timeout = Timeout(30 seconds)
  import scala.concurrent.ExecutionContext.Implicits.global

  "DatasetTable" should "create a dataset successfully, then return AlreadyExists" in {
    whenReady(datasetTable.createNewDataset(fooDataset),timeout) { response =>
      response should equal (Success)
    }

    // Second time around, dataset already exists
    whenReady(datasetTable.createNewDataset(fooDataset),timeout) { response =>
      response should equal (AlreadyExists)
    }
  }

  // Apparently, deleting a nonexisting dataset also returns success.  :/

  it should "delete a dataset" in {
    whenReady(datasetTable.createNewDataset(fooDataset),timeout) { response =>
      response should equal (Success)
    }
    whenReady(datasetTable.deleteDataset("foo"),timeout) { response =>
      response should equal (Success)
    }

    whenReady(datasetTable.getDataset("foo").failed,timeout) { err =>
      err shouldBe a [NotFoundError]
    }
  }

  it should "return NotFoundError when trying to get nonexisting dataset" in {
    whenReady(datasetTable.getDataset("foo").failed,timeout) { err =>
      err shouldBe a [NotFoundError]
    }
  }

  it should "return the Dataset if it exists" in {
    val barDataset = Dataset("bar", "sortCol")
    datasetTable.createNewDataset(barDataset).futureValue(timeout) should equal (Success)

    whenReady(datasetTable.getDataset("bar"),timeout) { dataset =>
      dataset should equal (barDataset)
    }
  }
}
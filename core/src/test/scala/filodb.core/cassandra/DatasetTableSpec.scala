package filodb.core.cassandra

import com.websudos.phantom.testing.CassandraFlatSpec
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._

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

  "DatasetTableRecord" should "create a dataset successfully, then return AlreadyExists" in {
    whenReady(DatasetTableOps.createNewDataset("foo")) { response =>
      response should equal (Success)
    }

    // Second time around, dataset already exists
    whenReady(DatasetTableOps.createNewDataset("foo")) { response =>
      response should equal (AlreadyExists)
    }
  }

  "DatasetTableRecord" should "not delete a dataset if it is NotFound" in {
    whenReady(DatasetTableOps.deleteDataset("foo")) { response =>
      response should equal (NotFound)
    }
  }
}
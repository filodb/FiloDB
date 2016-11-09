package filodb.cassandra.metastore

import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import filodb.cassandra.{AsyncTest, DefaultFiloSessionProvider}
import filodb.core._

class IngestionStateTableSpec extends FlatSpec with AsyncTest {

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb.cassandra")
  val ingestionStateTable = new IngestionStateTable(config, new DefaultFiloSessionProvider(config))

  // First create keyspace if not exist
  override def beforeAll(): Unit = {
    super.beforeAll()
    ingestionStateTable.createKeyspace(ingestionStateTable.keyspace)
  }

  val timeout = Timeout(30 seconds)

  "initialize" should "create ingestion_state table successfully" in {
    whenReady(ingestionStateTable.initialize(), timeout) { response =>
      response should equal (Success)
    }
  }

  "insertIngestionState" should "create an entry into table, then return already exists" in {
    whenReady(ingestionStateTable.insertIngestionState("nodeactor1", "test", "dataset1",
      0, "", "Started"), timeout) { response =>
      response should equal (Success)
    }

    // Second time around, entry for dataset already exists
    whenReady(ingestionStateTable.insertIngestionState("nodeactor1", "test", "dataset1", 0, "", "Started"), timeout) {
      response => response should equal (AlreadyExists)
    }

    whenReady(ingestionStateTable.insertIngestionState("nodeactor1", "test", "dataset2", 0, "", "Started"), timeout) {
      response => response should equal (Success)
    }

    whenReady(ingestionStateTable.insertIngestionState("nodeactor1", "test", "dataset3", 0, "", "Started"), timeout) {
      response => response should equal (Success)
    }
  }

  "updateIngestionState" should "modify state of ingestion for a given actor, dataset , walfilename" in {

    // without  exceptions
    whenReady(ingestionStateTable.updateIngestionState("nodeactor1", "test", "dataset1", "Completed", " "), timeout) {
      response =>  response should equal (Success)
    }

    // with exceptions
    whenReady(ingestionStateTable.updateIngestionState("nodeactor1", "test", "dataset2", "Failed",
      "Failed due to no Host available exception"), timeout) {
      response =>  response should equal (Success)
    }
  }

  "getIngestionStateByDataset" should "fetch entry for a given dataset" in {
    // valid dataset
    whenReady(ingestionStateTable.getIngestionStateByDataset("nodeactor1", "test", "dataset1", 0),
      timeout) { response =>
      response.length should equal(1)
    }

    // invalid dataset
    whenReady(ingestionStateTable.getIngestionStateByDataset("nodeactor1", "test", "invalidds", 0),
      timeout) { response =>
      response.length should equal(0)
    }
  }

  "getIngestionStateByActor" should "fetch entry for a given node actor path" in {
    // valid node actor path
    whenReady(ingestionStateTable.getIngestionStateByNodeActor("nodeactor1"), timeout) { response =>
      response.length should equal(3)
    }
    // invalid node actor path
    whenReady(ingestionStateTable.getIngestionStateByNodeActor("invalidactor"), timeout) { response =>
      response.length should equal(0)
    }
  }

  "deleteIngestationStateByDataset" should "fetch entry for a given dataset" in {
    // valid dataset
    whenReady(ingestionStateTable.deleteIngestionStateByDataset("nodeactor1", "test", "dataset1", 0), timeout) {
      response => response should equal(Success)
    }

    // invalid dataset
    whenReady(ingestionStateTable.deleteIngestionStateByDataset("nodeactor1", "test", "invalidds", 0), timeout) {
      response => response should equal(Success)
    }
  }

  "deleteIngestationStateByActor" should "remove entry for a given node actor path" in {
    // valid node actor path
    whenReady(ingestionStateTable.deleteIngestionStateByNodeActor("nodeactor1"), timeout) { response =>
      response should equal(Success)
    }
    // TODO @parekuti: Fix this invalid node actor path
    whenReady(ingestionStateTable.deleteIngestionStateByNodeActor("invalidactor"), timeout) { response =>
      response should equal(Success)
    }
  }

  "clearAll" should "truncate ingestion_state table successfully" in {
    whenReady(ingestionStateTable.clearAll(), timeout) { response =>
      response should equal(Success)
    }
  }

  "dropTable" should "delete ingestion_state table successfully" in {
    whenReady(ingestionStateTable.dropTable(), timeout) { response =>
      response should equal (Success)
    }

    // TODO @parekuti: Drop table for second time
  }

}


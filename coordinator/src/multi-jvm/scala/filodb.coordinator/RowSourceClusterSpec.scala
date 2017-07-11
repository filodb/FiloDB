package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._

import filodb.core._
import filodb.core.store.FilteredPartitionScan
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.coordinator.client.ClusterClient

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span, Seconds}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

object RowSourceClusterSpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  // this configuration will be used for all nodes
  // Override the memtable write interval and chunksize so that it will get back to us immediately
  // Otherwise default of 5s for write interval will kick in, making tests take a long time
  val globalConfig = ConfigFactory.parseString("""filodb.memtable.write.interval = 500 ms
                                                 |filodb.memtable.filo.chunksize = 70
                                                 |filodb.memtable.max-rows-per-table = 70""".stripMargin)
                       .withFallback(ConfigFactory.load("application_test.conf"))
  commonConfig(globalConfig)
}

// A multi-JVM RowSource spec to test out sending to multiple nodes
abstract class RowSourceClusterSpec extends MultiNodeSpec(RowSourceClusterSpecConfig)
  with FunSpecLike with Matchers with BeforeAndAfterAll
  with CoordinatorSetupWithFactory
  with StrictLogging
  with ImplicitSender with ScalaFutures {

  import akka.testkit._
  import DatasetCommands._
  import IngestionCommands._
  import GdeltTestData._
  import RowSourceClusterSpecConfig._
  import sources.CsvSourceActor

  override def initialParticipants = roles.size

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")
  val settings = CsvSourceActor.CsvSourceSettings()

  val address1 = node(first).address
  val address2 = node(second).address

  val dataset33 = dataset3.withName("gdelt2")
  metaStore.newDataset(dataset33).futureValue should equal (Success)
  val schemaMap = schema.map(c => c.name -> c).toMap
  val proj2 = RichProjection(dataset33, schema)
  val ref2 = proj2.datasetRef
  val columnNames = schema.map(_.name)
  schema.foreach { col => metaStore.newColumn(col, ref2).futureValue should equal (Success) }

  var clusterActor: ActorRef = _

  it("should start NodeClusterActor, CoordActors, join cluster, and wait for all nodes to enter barrier") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor
    clusterActor = singletonClusterActor("executor")

    enterBarrier("cluster-actor-started")

    cluster join address1
    awaitCond(cluster.state.members.size == 2)
    enterBarrier("both-nodes-joined-cluster")
  }

  private def getColStoreRows(): Int = {
    val paramSet = columnStore.getScanSplits(ref2, 1)
    val rowIt = columnStore.scanRows(proj2, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIt.toSeq.length
  }

  /**
   * What we're going to do here is to have BOTH nodes read from the SAME CSV.
   * If the routing works correctly, then each node will get two copies of each row, and half of the rows,
   * and with the row replacement logic, each node's InMemoryColumnStore will have half of the input rows.
   * If the routing does not work, or is turned off, then each node will get all the input rows.
   */
  it("should start ingestion and route rows to the right node") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))

    // Note: can only send 20 rows at a time before waiting for acks.  Therefore this tests memtable
    // ack on timer and ability for RowSource to handle waiting for acks repeatedly
    val csvActor = system.actorOf(CsvSourceActor.props(reader, dataset33, schemaMap, 0, clusterActor,
                                                       settings = settings.copy(maxUnackedBatches = 4,
                                                                                rowsToRead = 10)))

    coordinatorActor ! SetupIngestion(ref2, columnNames, 0)
    expectMsg(IngestionReady)
    enterBarrier("ingestion-ready")

    csvActor ! RowSource.Start
    expectMsg(60.seconds.dilated, RowSource.AllDone)
    enterBarrier("ingestion-done")

    // Flush all nodes and get the final count from each ColumnStore...
    runOn(first) {
      val client = new ClusterClient(clusterActor, "executor", "executor")
      client.flushCompletely(ref2, 0)
    }

    enterBarrier("all-nodes-flushed")

    runOn(second) {
      val numRows = getColStoreRows()
      // Find the test actor of the first node and send our results to it
      val node1testActor = system.actorSelection(node(first) / "system" / "testActor1")
      logger.debug(s"Node2  numRows = $numRows   node1testActor = $node1testActor")
      node1testActor ! numRows
    }

    runOn(first) {
      val numRowsFirst = getColStoreRows()
      logger.debug(s"Node1   numRows = $numRowsFirst")
      val numRowsSecond = expectMsgPF(5.seconds.dilated) { case i: Int => i }
      (numRowsFirst + numRowsSecond) should equal (99)
    }
  }
}

class RowSourceClusterSpecMultiJvmNode1 extends RowSourceClusterSpec
class RowSourceClusterSpecMultiJvmNode2 extends RowSourceClusterSpec
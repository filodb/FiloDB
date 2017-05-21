package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit.ImplicitSender
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
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
  import GdeltTestData._
  import NodeClusterActor._
  import RowSourceClusterSpecConfig._
  import sources.{CsvSourceActor, CsvSourceFactory}

  override def initialParticipants = roles.size

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")
  val settings = CsvSourceActor.CsvSourceSettings()

  val address1 = node(first).address
  val address2 = node(second).address

  metaStore.newDataset(dataset6).futureValue should equal (Success)
  val proj2 = RichProjection(dataset6, schema)
  val ref2 = proj2.datasetRef
  schema.foreach { col => metaStore.newColumn(col, ref2).futureValue should equal (Success) }

  var clusterActor: ActorRef = _

  val sampleReader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))
  val headerCols = CsvSourceActor.getHeaderColumns(sampleReader)

  it("should start NodeClusterActor, CoordActors, join cluster, and wait for all nodes to enter barrier") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor
    clusterActor = singletonClusterActor("worker")

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
    // Note: can only send 20 rows at a time before waiting for acks.  Therefore this tests memtable
    // ack on timer and ability for RowSource to handle waiting for acks repeatedly
    runOn(first) {
      val config = ConfigFactory.parseString(s"""header = true
                                             rows-to-read = 10
                                             max-unacked-batches = 4
                                             resource = "/GDELT-sample-test.csv"
                                             """)
      val msg = SetupDataset(dataset6.projections.head.dataset,
                             headerCols,
                             DatasetResourceSpec(1, 1),
                             IngestionSource(classOf[CsvSourceFactory].getName, config))
      clusterActor ! msg
      expectMsg(DatasetVerified)
    }

    enterBarrier("ingestion-ready")

    // TODO: find a way to wait for ingestion to be done via subscribing to node events
    Thread sleep 6000
    enterBarrier("ingestion-done")

    // Flush all nodes and get the final count from each ColumnStore...
    runOn(first) {
      val client = new ClusterClient(clusterActor, "worker", "worker")
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
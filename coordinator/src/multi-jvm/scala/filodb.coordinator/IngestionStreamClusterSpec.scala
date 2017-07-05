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

object IngestionStreamClusterSpecConfig extends MultiNodeConfig {
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

// A multi-JVM IngestionStream spec to test out sending to multiple nodes
abstract class IngestionStreamClusterSpec extends MultiNodeSpec(IngestionStreamClusterSpecConfig)
  with FunSpecLike with Matchers with BeforeAndAfterAll
  with CoordinatorSetupWithFactory
  with StrictLogging
  with ImplicitSender with ScalaFutures {

  import akka.testkit._
  import DatasetCommands._
  import GdeltTestData._
  import NodeClusterActor._
  import IngestionStreamClusterSpecConfig._
  import sources.{CsvStream, CsvStreamFactory}

  override def initialParticipants = roles.size

  override def beforeAll() = multiNodeSpecBeforeAll()

  override def afterAll() = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")
  val settings = CsvStream.CsvStreamSettings()

  val address1 = node(first).address
  val address2 = node(second).address

  metaStore.newDataset(dataset6).futureValue should equal (Success)
  val proj2 = RichProjection(dataset6, schema)
  val ref2 = proj2.datasetRef
  schema.foreach { col => metaStore.newColumn(col, ref2).futureValue should equal (Success) }

  var clusterActor: ActorRef = _

  val sampleReader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))
  val headerCols = CsvStream.getHeaderColumns(sampleReader)

  it("should start actors, join cluster, setup ingestion, and wait for all nodes to enter barrier") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor
    cluster join address1
    awaitCond(cluster.state.members.size == 2)
    enterBarrier("both-nodes-joined-cluster")

    clusterActor = singletonClusterActor("worker")
    enterBarrier("cluster-actor-started")

    runOn(first) {
      // Empty ingestion source - we're going to pump in records ourselves
      // 4 shards, 2 nodes, 2 nodes per shard
      val msg = SetupDataset(ref2,
                             headerCols,
                             DatasetResourceSpec(4, 2), noOpSource)
      clusterActor ! msg
      expectMsg(DatasetVerified)
    }
    enterBarrier("dataset-setup-done")
  }

  private def getColStoreRows(): Int = {
    val paramSet = columnStore.getScanSplits(ref2, 1)
    val rowIt = columnStore.scanRows(proj2, schema, 0, FilteredPartitionScan(paramSet.head))
    rowIt.toSeq.length
  }

  import IngestionStream._

  /**
   * Only one node is going to read the CSV, but we will get counts from both nodes
   */
  ignore("should start ingestion and route rows to the right node") {
    runOn(first) {
      // TODO: replace with waiting for node ready message
      Thread sleep 1000

      val config = ConfigFactory.parseString(s"""header = true
                                             batch-size = 10
                                             resource = "/GDELT-sample-test.csv"
                                             """)
      val stream = (new CsvStreamFactory).create(config, projection6, 0)
      val protocolActor = system.actorOf(IngestProtocol.props(clusterActor, projection6.datasetRef))
      stream.routeToShards(new ShardMapper(1), projection6, protocolActor)

      // TODO: find a way to wait for ingestion to be done via subscribing to node events
      Thread sleep 3000
    }

    enterBarrier("ingestion-done")

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

class IngestionStreamClusterSpecMultiJvmNode1 extends IngestionStreamClusterSpec
class IngestionStreamClusterSpecMultiJvmNode2 extends IngestionStreamClusterSpec
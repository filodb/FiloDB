package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

import filodb.core._

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

// A multi-JVM IngestionStream spec to test out routing to multiple nodes
// and distributed querying across nodes
abstract class IngestionStreamClusterSpec extends ClusterSpec(IngestionStreamClusterSpecConfig) {
  import akka.testkit._
  import GdeltTestData._
  import NodeClusterActor._
  import IngestionStreamClusterSpecConfig._
  import sources.CsvStreamFactory

  import cluster.ec

  override def initialParticipants = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  val config = globalConfig.getConfig("filodb")

  val address1 = node(first).address
  val address2 = node(second).address

  private lazy val coordinatorActor = cluster.coordinatorActor
  private lazy val metaStore = cluster.metaStore

  metaStore.newDataset(dataset6).futureValue should equal (Success)

  var clusterActor: ActorRef = _
  var mapper: ShardMapper = _

  private val resources = DatasetResourceSpec(4, 2)

  it("should start actors, join cluster, setup ingestion, and wait for all nodes to enter barrier") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor
    cluster join address1
    awaitCond(cluster.isJoined)
    enterBarrier("both-nodes-joined-cluster")

    clusterActor = cluster.clusterSingletonProxy("worker", withManager = true)
    enterBarrier("cluster-actor-started")

    runOn(first) {
      // Empty ingestion source - we're going to pump in records ourselves
      // 4 shards, 2 nodes, 2 nodes per shard
      val msg = SetupDataset(dataset6.ref, resources, noOpSource)
      clusterActor ! msg
      // It takes a _really_ long time for the cluster actor singleton to start.
      expectMsg(30.seconds.dilated, DatasetVerified)
    }
    enterBarrier("dataset-setup-done")
  }

  import IngestionStream._
  import QueryCommands._

  /**
   * Only one node is going to read the CSV, but we will get counts from both nodes and all shards
   */
  it("should start ingestion, route to shards, and do distributed querying") {
    runOn(first) {

      val protocolActor = system.actorOf(IngestProtocol.props(clusterActor, dataset6.ref))

      // We have to subscribe and get our own copy of the ShardMap so that routeToShards can route
      clusterActor ! SubscribeShardUpdates(dataset6.ref)
      expectMsgPF(3.seconds.dilated) {
        case CurrentShardSnapshot(ref, newMap) if ref == dataset6.ref => mapper = newMap
      }

      val config = ConfigFactory.parseString(s"""header = true
                                             batch-size = 10
                                             resource = "/GDELT-sample-test.csv"
                                             """)
      val stream = (new CsvStreamFactory).create(config, dataset6, 0)

      receiveWhile(messages = resources.numShards) {
        case e: IngestionStarted =>
          e.shard should be < (4)
          mapper.updateFromEvent(e)
      }

      // Now, route records to all different shards and nodes across cluster
      stream.routeToShards(mapper, dataset6, protocolActor)
    }

    enterBarrier("ingestion-done")

    // Both nodes can execute a distributed query to all shards and should get back the same answer
    // Count all the records in every partition in every shard
    // counting a non-partition column... can't count a partition column yet
    val durationForCI = 30.seconds.dilated
    val query = AggregateQuery(dataset6.ref, QueryArgs("count", "MonthYear"), FilteredPartitionQuery(Nil))

    def func: Array[Int] = {
      coordinatorActor ! query
      val answer = expectMsgClass(durationForCI, classOf[AggregateResponse[Int]])
      if (answer.elements.nonEmpty) answer.elementClass should equal (classOf[Int])
      answer.elements
    }

    awaitCond(func sameElements Array(99), max = durationForCI, interval = durationForCI / 3)

    enterBarrier("finished")
  }
}

class IngestionStreamClusterSpecMultiJvmNode1 extends IngestionStreamClusterSpec
class IngestionStreamClusterSpecMultiJvmNode2 extends IngestionStreamClusterSpec
package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.remote.testkit.MultiNodeConfig
import com.typesafe.config.ConfigFactory

import filodb.core._
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, ResultSchema, Tuple, TupleResult, VectorListResult}

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
                       .withFallback(ConfigFactory.parseResources("application_test.conf"))
                       .withFallback(ConfigFactory.load("filodb-defaults.conf"))
  commonConfig(globalConfig)
}

// A multi-JVM IngestionStream spec to test out routing to multiple nodes
// and distributed querying across nodes
abstract class IngestionStreamClusterSpec extends ClusterSpec(IngestionStreamClusterSpecConfig) {
  import akka.testkit._
  import cluster.ec

  import IngestionStreamClusterSpecConfig._
  import NodeClusterActor._
  import sources.CsvStreamFactory
  import GdeltTestData._
  import client.LogicalPlan._

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

    clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
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
  import client.QueryCommands._

  /**
   * Only one node is going to read the CSV, but we will get counts from both nodes and all shards
   */
  it("should start ingestion and route to shards") {
    runOn(first) {

      val protocolActor = system.actorOf(IngestProtocol.props(clusterActor, dataset6.ref))

      // We have to subscribe and get our own copy of the ShardMap so that routeToShards can route
      clusterActor ! SubscribeShardUpdates(dataset6.ref)
      expectMsgPF(10.seconds.dilated) {
        case CurrentShardSnapshot(ref, newMap) if ref == dataset6.ref => mapper = newMap
      }

      val config = ConfigFactory.parseString(s"""header = true
                                             batch-size = 10
                                             resource = "/GDELT-sample-test.csv"
                                             """)
      val stream = (new CsvStreamFactory).create(config, dataset6, 0, None)

      // Now, route records to all different shards and nodes across cluster
      stream.routeToShards(mapper, dataset6, protocolActor)

      expectMsgPF(10.seconds.dilated) {
        case CurrentShardSnapshot(ref, map) if ref == dataset6.ref =>
          map.statuses.toSeq shouldEqual Seq.fill(4)(ShardStatusActive)
      }
    }

    enterBarrier("ingestion-done")
  }

  it("should do distributed querying") {
    // Both nodes can execute a distributed query to all shards and should get back the same answer
    // Count all the records in every partition in every shard
    // counting a non-partition column... can't count a partition column yet
    val durationForCI = 30.seconds.dilated
    val query = LogicalPlanQuery(dataset6.ref,
                  simpleAgg("count", childPlan=PartitionsRange.all(FilteredPartitionQuery(Nil), Seq("MonthYear"))))

    def func: Int = {
      coordinatorActor ! query
      expectMsgPF(durationForCI) {
        case QueryResult(_, TupleResult(schema, Tuple(None, bRec))) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("result", ColumnType.IntColumn)), 0)
          bRec.getInt(0)
      }
    }

    awaitCond(func == 99, max = durationForCI, interval = durationForCI / 3)
    enterBarrier("aggregate query done")

    val q2 = LogicalPlanQuery(dataset6.ref,
               PartitionsRange.all(FilteredPartitionQuery(Nil), Seq("Actor2Code", "MonthYear")))
    coordinatorActor ! q2
    expectMsgPF(durationForCI) {
      case QueryResult(_, VectorListResult(keyRange, ResultSchema(schema, 1), vectors)) =>
        keyRange shouldEqual None
        // always add row key which is GLOBALEVENTID to the front of the schema
        schema shouldEqual Seq(ColumnInfo("GLOBALEVENTID", ColumnType.IntColumn),
                               ColumnInfo("Actor2Code", ColumnType.StringColumn),
                               ColumnInfo("MonthYear", ColumnType.IntColumn))
        vectors should have length (59)
        vectors.map(_.info.get.shardNo).toSet shouldEqual Set(0, 1, 2, 3)
        // There are exactly 17 entries in the group with blank ActorCode/ActorName
        vectors.map(_.readers.head.info.numRows).max shouldEqual 17
    }

    enterBarrier("QueryEngine / ExecPlan raw query done")
  }
}

class IngestionStreamClusterSpecMultiJvmNode1 extends IngestionStreamClusterSpec
class IngestionStreamClusterSpecMultiJvmNode2 extends IngestionStreamClusterSpec
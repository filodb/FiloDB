package filodb.coordinator

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.pattern.ask
import akka.remote.testkit.MultiNodeConfig
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import filodb.core._
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, ResultSchema, Tuple, TupleResult}

object ClusterRecoverySpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  // this configuration will be used for all nodes
  val globalConfig = ConfigFactory.parseString("""filodb.memstore.groups-per-shard = 4""".stripMargin)
                       .withFallback(ConfigFactory.load("application_test.conf"))
  commonConfig(globalConfig)
}

/**
 * A cluster recovery (auto restart of previous ingestion streams, checkpoints) test.
 */
abstract class ClusterRecoverySpec extends ClusterSpec(ClusterRecoverySpecConfig) {
  import akka.testkit._

  import ClusterRecoverySpecConfig._
  import NodeClusterActor._
  import sources.CsvStreamFactory
  import GdeltTestData._
  import client.LogicalPlan._

  override def initialParticipants = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  val address1 = node(first).address
  val address2 = node(second).address

  private lazy val coordinatorActor = cluster.coordinatorActor
  private lazy val metaStore = cluster.metaStore

  // 2 shards, 2 nodes == 1 shard per node
  val sourceConfig = ConfigFactory.parseString(s"""header = true
                                                   batch-size = 10
                                                   noflush = true
                                                   resource = "/GDELT-sample-test.csv"
                                                   """)
  val shards = 2
  private val setup = SetupDataset(dataset6.ref,
                                   DatasetResourceSpec(shards, shards),
                                   IngestionSource(classOf[CsvStreamFactory].getName, sourceConfig))

  metaStore.newDataset(dataset6).futureValue shouldEqual Success
  metaStore.writeIngestionConfig(setup.config).futureValue shouldEqual Success

  var clusterActor: ActorRef = _
  var mapper: ShardMapper = _

  import client.QueryCommands._

  private def hasAllShardsStopped(mapper: ShardMapper): Boolean = {
    val statuses = mapper.shardValues.map(_._2)
    statuses.forall(_ == ShardStatusStopped)
  }

  it("should start actors, join cluster, automatically start prev ingestion") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor
    cluster join address1
    awaitCond(cluster.isJoined)
    enterBarrier("both-nodes-joined-cluster")

    clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
    // wait for dataset to get registered automatically
    // NOTE: unfortunately the delay seems to be needed in order to query the ClusterActor successfully
    Thread sleep 3000
    implicit val timeout: Timeout = cluster.settings.InitializationTimeout
    def func: Future[Seq[DatasetRef]] = (clusterActor ? ListRegisteredDatasets).mapTo[Seq[DatasetRef]]
    awaitCond(func.futureValue == Seq(dataset6.ref), interval=250.millis, max=10.seconds)
    enterBarrier("cluster-actor-recovery-started")

    clusterActor ! SubscribeShardUpdates(dataset6.ref)
    expectMsgPF(10.seconds.dilated) {
      case CurrentShardSnapshot(ref, newMap) if ref == dataset6.ref => mapper = newMap
    }

    // wait for all ingestion to be stopped, keep receiving status messages
    while(!hasAllShardsStopped(mapper)) {
      mapper.updateFromEvent(expectMsgClass(classOf[ShardEvent]))
    }
    enterBarrier("ingestion-stopped")

    val query = LogicalPlanQuery(dataset6.ref,
                  simpleAgg("count", childPlan=PartitionsRange.all(FilteredPartitionQuery(Nil), Seq("MonthYear"))))

    coordinatorActor ! query
    expectMsgPF(10.seconds.dilated) {
      case QueryResult(_, TupleResult(schema, Tuple(None, bRec))) =>
        schema shouldEqual ResultSchema(Seq(ColumnInfo("result", ColumnType.IntColumn)), 0)
        bRec.getInt(0) shouldEqual (99 * 2)
    }
  }
}

class ClusterRecoverySpecMultiJvmNode1 extends ClusterRecoverySpec
class ClusterRecoverySpecMultiJvmNode2 extends ClusterRecoverySpec
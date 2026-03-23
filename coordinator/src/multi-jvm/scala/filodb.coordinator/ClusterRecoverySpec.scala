package filodb.coordinator

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.ActorRef
import akka.pattern.ask
import akka.remote.testkit.MultiNodeConfig
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core._
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, PlannerParams, QueryContext}

object ClusterRecoverySpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  // Combined dataset/stream definition, store config
  // 2 shards, 2 nodes == 1 shard per node
  val ourConf = s"""
  filodb {
    memstore.groups-per-shard = 4
    partition-schema {
      columns = ["Actor2Code:string", "Actor2Name:string"]
      predefined-keys = []
      ${GdeltTestData.datasetOptionConfig}
    }
    schemas {
      gdelt {
        columns = ["GLOBALEVENTID:long", "SQLDATE:long", "MonthYear:int",
                        "Year:int", "NumArticles:int", "AvgTone:double"]
        value-column = "AvgTone"
        downsamplers = []
      }
    }
    inline-dataset-configs = [
      {
        dataset = "gdelt"
        schema = "gdelt"
        num-shards = 2
        min-num-nodes = 2
        sourcefactory = "${classOf[sources.CsvStreamFactory].getName}"
        sourceconfig {
          header = true
          batch-size = 10
          noflush = true
          resource = "/GDELT-sample-test.csv"
          shutdown-ingest-after-stopped = false
          ${TestData.sourceConfStr}
        }
      }
    ]
  }"""

  // this configuration will be used for all nodes
  val globalConfig = ConfigFactory.parseString(ourConf)
                       .withFallback(ConfigFactory.parseResources("application_test.conf"))
                       .withFallback(ConfigFactory.load("filodb-defaults.conf"))
  FilodbSettings.initialize(globalConfig)
  commonConfig(globalConfig)
}

/**
 * A cluster recovery (auto restart of previous ingestion streams, checkpoints) test.
 * Also a good integration test for cluster and coordinator startup, etc.
 * NOTE: since we moved to static configs every startup is a "recovery".
 */
abstract class ClusterRecoverySpec extends ClusterSpec(ClusterRecoverySpecConfig) {
  import akka.testkit._

  import ClusterRecoverySpecConfig._
  import filodb.query._
  import GdeltTestData._
  import NodeClusterActor._

  override def initialParticipants: Int = roles.size

  override def beforeAll(): Unit = multiNodeSpecBeforeAll()

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  val address1 = node(first).address
  val address2 = node(second).address

  private lazy val coordinatorActor = cluster.coordinatorActor
  private lazy val metaStore = cluster.metaStore

  implicit val patience: PatienceConfig =   // make sure futureValue has long enough time
    PatienceConfig(timeout = Span(120, Seconds), interval = Span(500, Millis))

  var clusterActor: ActorRef = _
  var mapper: ShardMapper = _

  import client.QueryCommands._

  private def hasAllShardsStopped(mapper: ShardMapper): Boolean = {
    val statuses = mapper.shardValues.map(_._2)
    statuses.forall(_ == ShardStatusStopped)
  }

  // Temporarily ignore this test, it always seems to fail in Travis.  Seems like in Travis the shards are
  // never assigned.
  it("should start actors, join cluster, automatically start prev ingestion") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor
    cluster join address1
    awaitCond(cluster.isJoined)
    clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
    enterBarrier("both-nodes-joined-cluster")

    // wait for dataset to get registered automatically
    // NOTE: the delay seems to be needed in order to query the ClusterActor successfully
    Thread sleep 3000
    implicit val timeout: Timeout = cluster.settings.InitializationTimeout * 2
    // Bug fix: the awaitCond was previously commented out, leaving only the 3-second sleep above.
    // If the dataset isn't registered yet when SubscribeShardUpdates is sent, NodeClusterActor
    // replies with DatasetUnknown instead of CurrentShardSnapshot, causing expectMsgPF to fail
    // immediately (it doesn't retry on non-matching messages).  That node then misses
    // enterBarrier("ingestion-stopped"), the other node times out waiting at that barrier, and
    // both JVMs exit with Akka's BARRIER_TIMEOUT_EXIT_CODE = 189.
    awaitCond({
      Await.result((clusterActor ? ListRegisteredDatasets).mapTo[Seq[DatasetRef]], timeout.duration)
           .contains(dataset6.ref)
    }, max = timeout.duration, interval = 500.millis)
    enterBarrier("cluster-actor-recovery-started")

    clusterActor ! SubscribeShardUpdates(dataset6.ref)
    expectMsgPF(10.seconds.dilated) {
      case CurrentShardSnapshot(ref, newMap) if ref == dataset6.ref => mapper = newMap
    }
    info(s"Initial shardmapper snapshot = $mapper")

    // Bug fix: replaced the while-loop + expectMsgPF(10s) pattern with fishForMessage.
    // The old loop called expectMsgPF inside a while(!hasAllShardsStopped) condition.
    // expectMsgPF fails immediately on any non-matching message and also fails on timeout
    // (e.g. if shards land in Error/Down state instead of Stopped, the condition never
    // becomes true and the 10-second per-iteration timeout fires, again causing the barrier
    // miss -> exit code 189).  fishForMessage keeps consuming snapshots until the predicate
    // returns true, with a single overall timeout covering the whole wait.
    fishForMessage(90.seconds.dilated, hint = "waiting for all shards to stop") {
      case CurrentShardSnapshot(ref, newMap) if ref == dataset6.ref =>
        mapper = newMap
        val stopped = hasAllShardsStopped(mapper)
        if (!stopped) println(s"Not all shards stopped yet, mapper is now $mapper")
        stopped
    }
    cluster.memStore.refreshIndexForTesting(dataset6.ref)
    enterBarrier("ingestion-stopped")

    // val query = LogicalPlanQuery(dataset6.ref,
    //               simpleAgg("count", childPlan=PartitionsRange.all(FilteredPartitionQuery(Nil), Seq("MonthYear"))))

    val qOpt = QueryContext(plannerParams = PlannerParams(shardOverrides = Some(Seq(0, 1))))
    val q2 = LogicalPlan2Query(dataset6.ref,
               PeriodicSeriesWithWindowing(
                 RawSeries(AllChunksSelector, Nil, Seq("AvgTone"), Some(300000), None),
                 100L, 1000L, 100L, window = 1000L, function = RangeFunctionId.CountOverTime), qOpt)
    coordinatorActor ! q2
    expectMsgPF(10.seconds.dilated) {
      case QueryResult(_, schema, vectors, _, _, _, _) =>
        schema.columns shouldEqual Seq(ColumnInfo("GLOBALEVENTID", ColumnType.LongColumn, false),
                                       ColumnInfo("value", ColumnType.DoubleColumn, true))
        // query is counting each partition....
        vectors should have length (59 * 2)
        // vectors(0).rows().map(_.getDouble(1)).toSeq shouldEqual Seq(575.24)
        // TODO:  actually change logicalPlan above to sum up individual counts for us
        vectors.map(_.rows().map(_.getDouble(1).toInt).toSeq.head).sum shouldEqual (99 * 2)
    }
  }
}

class ClusterRecoverySpecMultiJvmNode1 extends ClusterRecoverySpec
class ClusterRecoverySpecMultiJvmNode2 extends ClusterRecoverySpec
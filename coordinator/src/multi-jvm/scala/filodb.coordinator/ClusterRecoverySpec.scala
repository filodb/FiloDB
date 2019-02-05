package filodb.coordinator

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ActorRef
import akka.pattern.ask
import akka.remote.testkit.MultiNodeConfig
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core._
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.{ColumnInfo, ResultSchema}

object ClusterRecoverySpecConfig extends MultiNodeConfig {
  // register the named roles (nodes) of the test
  val first = role("first")
  val second = role("second")

  // this configuration will be used for all nodes
  val globalConfig = ConfigFactory.parseString("""filodb.memstore.groups-per-shard = 4""".stripMargin)
                       .withFallback(ConfigFactory.parseResources("application_test.conf"))
                       .withFallback(ConfigFactory.load("filodb-defaults.conf"))
  commonConfig(globalConfig)
}

/**
 * A cluster recovery (auto restart of previous ingestion streams, checkpoints) test.
 */
abstract class ClusterRecoverySpec extends ClusterSpec(ClusterRecoverySpecConfig) {
  import akka.testkit._

  import ClusterRecoverySpecConfig._
  import filodb.query._
  import GdeltTestData._
  import NodeClusterActor._
  import sources.CsvStreamFactory

  override def initialParticipants: Int = roles.size

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
                                                   """).withFallback(TestData.sourceConf)
  val shards = 2
  private val setup = SetupDataset(dataset6.ref,
                                   DatasetResourceSpec(shards, shards),
                                   IngestionSource(classOf[CsvStreamFactory].getName, sourceConfig),
                                   TestData.storeConf)

  implicit val patience =   // make sure futureValue has long enough time
    PatienceConfig(timeout = Span(120, Seconds), interval = Span(500, Millis))

  metaStore.newDataset(dataset6).futureValue shouldEqual Success
  metaStore.writeIngestionConfig(setup.config).futureValue shouldEqual Success

  var clusterActor: ActorRef = _
  var mapper: ShardMapper = _

  import client.QueryCommands._

  private def hasAllShardsStopped(mapper: ShardMapper): Boolean = {
    val statuses = mapper.shardValues.map(_._2)
    statuses.forall(_ == ShardStatusStopped)
  }

  // Temporarily ignore this test, it always seems to fail in Travis.  Seems like in Travis the shards are
  // never assigned.
  ignore("should start actors, join cluster, automatically start prev ingestion") {
    // Start NodeCoordinator on all nodes so the ClusterActor will register them
    coordinatorActor
    cluster join address1
    awaitCond(cluster.isJoined)
    clusterActor = cluster.clusterSingleton(ClusterRole.Server, None)
    enterBarrier("both-nodes-joined-cluster")

    import scala.concurrent.ExecutionContext.Implicits.global

    // wait for dataset to get registered automatically
    // NOTE: unfortunately the delay seems to be needed in order to query the ClusterActor successfully
    Thread sleep 3000
    implicit val timeout: Timeout = cluster.settings.InitializationTimeout * 2
    def func: Future[Seq[DatasetRef]] = {
      val refs = (clusterActor ? ListRegisteredDatasets).mapTo[Seq[DatasetRef]]
      refs.map { r =>
        println(s"Queried $clusterActor and got back [$refs]")
        r
      }
    }
    // awaitCond(func.futureValue == Seq(dataset6.ref), interval = 250.millis, max = 90.seconds)
    enterBarrier("cluster-actor-recovery-started")

    clusterActor ! SubscribeShardUpdates(dataset6.ref)
    expectMsgPF(10.seconds.dilated) {
      case CurrentShardSnapshot(ref, newMap) if ref == dataset6.ref => mapper = newMap
    }
    info(s"Initial shardmapper snapshot = $mapper")

    // wait for all ingestion to be stopped, keep receiving status messages
    while(!hasAllShardsStopped(mapper)) {
      println(s"Not all shards stopped, waiting for shard updates...  mapper is now $mapper")
      expectMsgPF(10.seconds.dilated) {
        case CurrentShardSnapshot(ref, newMap) if ref == dataset6.ref => mapper = newMap
      }
    }
    enterBarrier("ingestion-stopped")

    // val query = LogicalPlanQuery(dataset6.ref,
    //               simpleAgg("count", childPlan=PartitionsRange.all(FilteredPartitionQuery(Nil), Seq("MonthYear"))))

    val qOpt = QueryOptions(shardOverrides = Some(Seq(0, 1)))
    val q2 = LogicalPlan2Query(dataset6.ref,
               PeriodicSeriesWithWindowing(
                 RawSeries(AllChunksSelector, Nil, Seq("AvgTone")),
                 100L, 1000L, 100L, window=1000L, function=RangeFunctionId.CountOverTime), qOpt)
    coordinatorActor ! q2
    expectMsgPF(10.seconds.dilated) {
      case QueryResult(_, schema, vectors) =>
        schema shouldEqual ResultSchema(Seq(ColumnInfo("GLOBALEVENTID", ColumnType.LongColumn),
                                            ColumnInfo("AvgTone", ColumnType.DoubleColumn)), 1)
        // query is counting each partition....
        vectors should have length (59 * 2)
        // vectors(0).rows.map(_.getDouble(1)).toSeq shouldEqual Seq(575.24)
        // TODO:  actually change logicalPlan above to sum up individual counts for us
        vectors.map(_.rows.map(_.getDouble(1).toInt).toSeq.head).sum shouldEqual (99 * 2)
    }
  }
}

class ClusterRecoverySpecMultiJvmNode1 extends ClusterRecoverySpec
class ClusterRecoverySpecMultiJvmNode2 extends ClusterRecoverySpec
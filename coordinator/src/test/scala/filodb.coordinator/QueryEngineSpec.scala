package filodb.coordinator

import java.net.InetAddress

import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef, AddressFromURIString, PoisonPill, Props}
import akka.pattern.gracefulStop
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.coordinator.client.LogicalPlan
import filodb.coordinator.client.QueryCommands.KeyRangeQuery
import filodb.coordinator.queryengine.Engine
import filodb.coordinator.queryengine.Engine.ExecArgs
import filodb.core.{DatasetRef, MachineMetricsData}
import filodb.core.metadata.{Column, Dataset}
import filodb.core.query._

object QueryEngineSpec extends ActorSpecConfig

class QueryEngineSpec  extends ActorTest(QueryEngineSpec.getNewSystem)
  with ScalaFutures with BeforeAndAfterEach {

  import akka.testkit._

  import Column.ColumnType._
  import LogicalPlan._
  import NodeClusterActor._
  import client.DatasetCommands._
  import client.IngestionCommands._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.parseString(
                      """filodb.memtable.flush-trigger-rows = 100
                         filodb.memtable.max-rows-per-table = 100
                         filodb.memtable.noactivity.flush.interval = 2 s
                         filodb.memtable.write.interval = 500 ms""")
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")

  private val host = InetAddress.getLocalHost.getHostAddress
  private val selfAddress = AddressFromURIString(s"akka.tcp://${system.name}@$host:2552")
  private val cluster = FilodbCluster(system)
  private lazy val memStore = cluster.memStore
  private lazy val metaStore = cluster.metaStore

  implicit val ec = cluster.ec

  val strategy = DefaultShardAssignmentStrategy
  protected val shardManager = new ShardManager(DefaultShardAssignmentStrategy)

  val timeScan = KeyRangeQuery(Seq(110000L), Seq(130000L))

  val clusterActor = system.actorOf(Props(new Actor {
    def receive: Receive = {
      case SubscribeShardUpdates(ref) => shardManager.subscribe(sender(), ref)
    }
  }))
  var coordinatorActor: ActorRef = _
  var probe: TestProbe = _
  var shardMap = new ShardMapper(1)
  val nodeCoordProps = NodeCoordinatorActor.props(metaStore, memStore, config)

  override def beforeAll(): Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  override def beforeEach(): Unit = {
    metaStore.clearAllData().futureValue
    memStore.reset()
    shardMap.clear()

    coordinatorActor = system.actorOf(nodeCoordProps, s"test-node-coord-${System.nanoTime}")
    coordinatorActor ! CoordinatorRegistered(clusterActor)

    shardManager.addMember(selfAddress, coordinatorActor)
    probe = TestProbe()
  }

  override def afterEach(): Unit = {
    shardManager.reset()
    gracefulStop(coordinatorActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  def startIngestion(dataset: Dataset, numShards: Int): Unit = {
    val resources = DatasetResourceSpec(numShards, 1)
    val noOpSource = IngestionSource(classOf[NoOpStreamFactory].getName)
    val sd = SetupDataset(dataset.ref, resources, noOpSource)
    shardManager.addDataset(sd, dataset, self)
    shardManager.subscribe(probe.ref, dataset.ref)
    probe.expectMsgPF() { case CurrentShardSnapshot(ds, mapper) =>
      shardMap = mapper
    }
  }

  describe("QueryActor commands and responses") {
    import MachineMetricsData._
    import client.QueryCommands._
    implicit val askTimeout = Timeout(5.seconds)

    def setupTimeSeries(numShards: Int = 1): DatasetRef = {
      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetCreated)

      startIngestion(MachineMetricsData.dataset1, numShards)
      dataset1.ref
    }

    it("should concatenate Tuples/Vectors from multiple shards") {
      val ref = setupTimeSeries(2)
      // Same series is ingested into two shards.  I know, this should not happen in real life.
      probe.send(coordinatorActor, IngestRows(ref, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))
      probe.send(coordinatorActor, IngestRows(ref, 1, records(linearMultiSeries(130000L)).take(20)))
      probe.expectMsg(Ack(19L))

      val series2 = (2 to 4).map(n => s"Series $n")
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2.toSet.asInstanceOf[Set[Any]])))
      val logPlan = PartitionsInstant(FilteredPartitionQuery(multiFilter), Seq("min"))
      val execPlan = Engine.materialize(logPlan, new ExecArgs(dataset1, shardMap)).get
      val execPlanQuery = new ExecPlanQuery(dataset1.ref, execPlan, 1000)
      probe.send(coordinatorActor, execPlanQuery)
      val info1 = probe.expectMsgPF(3.seconds.dilated) {
        case QueryResult(_, TupleListResult(schema, tuples)) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("min", DoubleColumn)), 1)
          // We should get tuples from both shards
          tuples should have length (6)
          // Group by partition key
          val groupedByKey = tuples.groupBy(_.info.get.partKey)
          // Each grouping should have two tuples, one from each shard
          groupedByKey.map(_._2.length) shouldEqual Seq(2, 2, 2)
          val series2Key = dataset1.partKey("Series 2")
          groupedByKey(series2Key).map(_.info.get.shardNo).toSet shouldEqual Set(0, 1)
          groupedByKey(series2Key).map(_.data.getLong(0)).toSet shouldEqual Set(122000L, 142000L)
          groupedByKey(series2Key).map(_.data.getDouble(1)).toSet shouldEqual Set(23.0, 13.0)
      }
    }
  }

}

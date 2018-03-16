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

import filodb.coordinator.client._
import filodb.coordinator.queryengine.Engine
import filodb.core._
import filodb.core.metadata.{Dataset, Column}
import filodb.core.query._
import filodb.core.store._
import filodb.memory.format.ZeroCopyUTF8String

object NodeCoordinatorActorSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class NodeCoordinatorActorSpec extends ActorTest(NodeCoordinatorActorSpec.getNewSystem)
  with ScalaFutures with BeforeAndAfterEach {

  import akka.testkit._

  import client.DatasetCommands._
  import client.IngestionCommands._
  import NodeClusterActor._
  import GdeltTestData._
  import LogicalPlan._
  import Column.ColumnType._

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


  val clusterActor = system.actorOf(Props(new Actor {
    def receive: Receive = {
      case SubscribeShardUpdates(ref) => shardManager.subscribe(sender(), ref)
      case e: ShardEvent              => shardManager.updateFromShardEventNoPublish(e)
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

  describe("NodeCoordinatorActor DatasetOps commands") {
    it("should be able to create new dataset") {
      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetCreated)
    }

    it("should return DatasetAlreadyExists creating dataset that already exists") {
      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetCreated)

      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetAlreadyExists)
    }
  }

  describe("QueryActor commands and responses") {
    import client.QueryCommands._
    import MachineMetricsData._

    def setupTimeSeries(numShards: Int = 1): DatasetRef = {
      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetCreated)

      startIngestion(MachineMetricsData.dataset1, numShards)
      dataset1.ref
    }

    it("should return UnknownDataset if attempting to query before ingestion set up") {
      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetCreated)

      val ref = MachineMetricsData.dataset1.ref
      val q1 = LogicalPlanQuery(ref, PartitionsInstant(SinglePartitionQuery(Seq("Series 1")), Seq("min")))
      probe.send(coordinatorActor, q1)
      probe.expectMsg(UnknownDataset)
    }

    it("should return chunks when querying all samples after ingesting rows") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(multiSeriesData()).take(20)))
      probe.expectMsg(Ack(19L))

      // Query existing partition: Series 1
      val q1 = LogicalPlanQuery(ref, PartitionsRange.all(SinglePartitionQuery(Seq("Series 1")), Seq("min")))
      probe.send(coordinatorActor, q1)
      val info1 = probe.expectMsgPF(3.seconds.dilated) {
        case QueryResult(_, VectorListResult(rangeOpt, schema, partVectorSeq)) =>
          rangeOpt shouldEqual None
          schema shouldEqual ResultSchema(Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("min", DoubleColumn)), 1)
          partVectorSeq should have length (1)
          partVectorSeq.head.readers should have length (1)
          partVectorSeq.head.info.get.partKey shouldEqual dataset1.partKey("Series 1")
      }

      // Query nonexisting partition
      val q2 = LogicalPlanQuery(ref, PartitionsRange.all(SinglePartitionQuery(Seq("NotSeries")), Seq("min")))
      probe.send(coordinatorActor, q2)
      val info2 = probe.expectMsgPF(3.seconds.dilated) {
        case QueryResult(_, VectorListResult(None, ResultSchema(schema, 1), Nil)) =>
          schema shouldEqual Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("min", DoubleColumn))
      }
    }

    it("should return BadArgument/BadQuery if wrong type of partition key passed") {
      val ref = setupTimeSeries()
      val q1 = LogicalPlanQuery(ref, PartitionsRange.all(SinglePartitionQuery(Seq(-1)), Seq("min")))
      probe.send(coordinatorActor, q1)
      probe.expectMsgClass(classOf[BadQuery])
    }

    it("should return BadQuery if aggregation function not defined") {
      val ref = setupTimeSeries()
      val q1 = LogicalPlanQuery(ref, simpleAgg("not-a-func", childPlan=
                 PartitionsRange.all(SinglePartitionQuery(Seq("Series 1")), Seq("foo"))))
      probe.send(coordinatorActor, q1)
      probe.expectMsg(BadQuery("No such aggregation function not-a-func"))

      // Need time-group-min (eg dashes)
      val q2 = LogicalPlanQuery(ref, simpleAgg("TimeGroupMin", childPlan=
                 PartitionsRange.all(SinglePartitionQuery(Seq("Series 1")), Seq("min"))))
      probe.send(coordinatorActor, q2)
      probe.expectMsg(BadQuery("No such aggregation function TimeGroupMin"))
    }

    // Don't have a function that returns this yet.  time_group_* _used_ to but doesn't anymore
    ignore("should return WrongNumberOfArgs if number of arguments wrong") {
      val ref = setupTimeSeries()
      val q1 = LogicalPlanQuery(ref, simpleAgg("time_group_avg", childPlan=
                 PartitionsRange.all(SinglePartitionQuery(Seq("Series 1")), Seq("min"))))
      probe.send(coordinatorActor, q1)
      probe.expectMsg(WrongNumberOfArgs(2, 5))
    }

    it("should return BadArgument if arguments could not be parsed successfully") {
      val ref = setupTimeSeries()
      val q1 = LogicalPlanQuery(ref, simpleAgg("time_group_avg", Seq("a1b"), childPlan=
                 PartitionsRange.all(SinglePartitionQuery(Seq("Series 1")), Seq("min"))))
      probe.send(coordinatorActor, q1)
      probe.expectMsgClass(classOf[BadArgument])
    }

    val timeScan = KeyRangeQuery(Seq(110000L), Seq(130000L))

    it("should return BadArgument if wrong types of columns are passed") {
      val ref = setupTimeSeries()
      // Test 1: Cannot pass in a non-double column for time_group_avg
      val q1 = LogicalPlanQuery(ref, simpleAgg("time_group_avg", Seq("100"), childPlan=
                 PartitionsRange(SinglePartitionQuery(Seq("Series 1")), timeScan, Seq("timestamp"))))
      probe.send(coordinatorActor, q1)
      val msg = probe.expectMsgClass(classOf[BadArgument])
      msg.msg should include ("not in allowed set")
    }

    it("should return BadQuery if time function used on a non-timeseries dataset") {
      import GdeltTestData._

      probe.send(coordinatorActor, CreateDataset(dataset4))
      probe.expectMsg(DatasetCreated)

      // No need to initialize ingestion, because this test doesn't query data itself

      val ref4 = dataset4.ref
      probe.send(coordinatorActor, DatasetSetup(dataset4.asCompactString))

      // case 1: scan all data in partition, but no timestamp column ->
      val q1 = LogicalPlanQuery(ref4, simpleAgg("time_group_avg", childPlan=
                 PartitionsRange.all(FilteredPartitionQuery(Nil), Seq("AvgTone"))))
      probe.send(coordinatorActor, q1)
      val msg = probe.expectMsgClass(classOf[BadQuery])
      msg.msg should include ("time-based functions")

      // case 2: using a time-based range scan should not be valid for non-timeseries dataset
      val q2 = LogicalPlanQuery(ref4, simpleAgg("time_group_avg", childPlan=
                 PartitionsRange(FilteredPartitionQuery(Nil), MostRecentTime(5000), Seq("AvgTone"))))
      probe.send(coordinatorActor, q2)
      val msg2 = probe.expectMsgClass(classOf[BadQuery])
      msg2.msg should include ("Not a time")
    }

    it("should return results in QueryResult if valid LogicalPlanQuery") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      val series = (1 to 3).map(n => Seq(s"Series $n"))
      val q1 = LogicalPlanQuery(ref, simpleAgg("time_group_avg", Seq("2"), childPlan=
                 PartitionsRange(MultiPartitionQuery(series), timeScan, Seq("min"))))
      probe.send(coordinatorActor, q1)
      probe.expectMsgPF() {
        case QueryResult(_, VectorResult(schema, PartitionVector(None, readers))) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("result", DoubleColumn)), 0)
          readers should have length (1)
          readers.head.vectors.size shouldEqual 1
          readers.head.vectors(0).toSeq shouldEqual Seq(13.0, 23.0)
      }

      // Try a filtered partition query
      import ZeroCopyUTF8String._
      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = LogicalPlanQuery(ref, simpleAgg("time_group_avg", Seq("2"), childPlan=
                 PartitionsRange(FilteredPartitionQuery(multiFilter), timeScan, Seq("min"))))
      probe.send(coordinatorActor, q2)
      probe.expectMsgPF() {
        case QueryResult(_, VectorResult(schema, PartitionVector(None, readers))) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("result", DoubleColumn)), 0)
          readers should have length (1)
          readers.head.vectors.size shouldEqual 1
          readers.head.vectors(0).toSeq shouldEqual Seq(14.0, 24.0)
      }

      // What if filter returns no results?
      val filter3 = Seq(ColumnFilter("series", Filter.Equals("foobar".utf8)))
      val q3 = LogicalPlanQuery(ref, simpleAgg("sum", childPlan=
                 PartitionsRange(FilteredPartitionQuery(filter3), timeScan, Seq("min"))))
      probe.send(coordinatorActor, q3)
      probe.expectMsgPF() {
        case QueryResult(_, TupleResult(schema, Tuple(None, bRec))) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("result", DoubleColumn)), 0)
          bRec.getDouble(0) shouldEqual 0.0
      }
    }

    it("should aggregate from multiple shards") {
      val ref = setupTimeSeries(2)
      probe.send(coordinatorActor, IngestRows(ref, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))
      probe.send(coordinatorActor, IngestRows(ref, 1, records(linearMultiSeries(130000L)).take(20)))
      probe.expectMsg(Ack(19L))

      // Should return results from both shards
      // shard 1 - timestamps 110000 -< 130000;  shard 2 - timestamps 130000 <- 1400000
      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = LogicalPlanQuery(ref, simpleAgg("time_group_avg", Seq("3"), childPlan=
                 PartitionsRange(FilteredPartitionQuery(multiFilter), timeScan.copy(end = Seq(140000L)), Seq("min"))))
      probe.send(coordinatorActor, q2)
      probe.expectMsgPF() {
        case QueryResult(_, VectorResult(schema, PartitionVector(None, readers))) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("result", DoubleColumn)), 0)
          readers should have length (1)
          readers.head.vectors.size shouldEqual 1
          readers.head.vectors(0).toSeq shouldEqual Seq(14.0, 24.0, 4.0)
      }
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
      val q2 = LogicalPlanQuery(ref, PartitionsInstant(FilteredPartitionQuery(multiFilter), Seq("min")))
      probe.send(coordinatorActor, q2)
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

    // For some reason this test yields inconsistent results.  It is also of the old aggregation pipeline
    // histo func, which will be rewritten in the ExecPlan.
    ignore("should aggregate using histogram combiner") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      val q1 = LogicalPlanQuery(ref, simpleAgg("sum", Nil, "histogram", Seq("2000"), childPlan=
                 PartitionsRange.all(FilteredPartitionQuery(Nil), Seq("min"))))
      probe.send(coordinatorActor, q1)
      probe.expectMsgPF() {
        case QueryResult(_, VectorResult(schema, PartitionVector(None, readers))) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("counts", IntColumn), ColumnInfo("bucketMax", DoubleColumn)), 0)
          readers should have length (1)
          readers.head.vectors.size shouldEqual 2
          readers.head.vectors(0).toSeq shouldEqual Seq(0, 0, 0, 0, 4, 6, 0, 0, 0, 0)
      }
    }

    implicit val askTimeout = Timeout(5.seconds)

    it("should return QueryError if physical plan execution errors out") {
      // use an ExecPlanQuery which we know cannot be valid
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      val partMethods = Seq(FilteredPartitionScan(ShardSplit(0), Nil))
      val plan = Engine.DistributeConcat(partMethods, shardMap, 4, 10) { method =>
        new ExecPlan.LocalVectorReader(Seq(-1, 199), method, AllChunkScan) }
      probe.send(coordinatorActor, ExecPlanQuery(ref, plan, 100))
      probe.expectMsgPF() {
        case QueryError(ref, e) =>
          e shouldBe a[ArrayIndexOutOfBoundsException]
      }
    }

    it("should return PartitionInfo and Tuples correctly when querying PartitionsInstant") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      val series2 = (2 to 4).map(n => s"Series $n")
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2.toSet.asInstanceOf[Set[Any]])))
      val q2 = LogicalPlanQuery(ref, PartitionsInstant(FilteredPartitionQuery(multiFilter), Seq("min")))
      probe.send(coordinatorActor, q2)
      val info1 = probe.expectMsgPF(3.seconds.dilated) {
        case QueryResult(_, TupleListResult(schema, tuples)) =>
          schema shouldEqual ResultSchema(Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("min", DoubleColumn)), 1)
          tuples should have length (3)
          tuples.map(_.info.get.partKey) shouldEqual series2.map { s => dataset1.partKey(s: Any) }
          tuples.map(_.data.getDouble(1)) shouldEqual Seq(23.0, 24.0, 25.0)
      }
    }

    it("should respond to GetIndexNames and GetIndexValues") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      probe.send(coordinatorActor, GetIndexNames(ref))
      probe.expectMsg(Seq("series"))

      probe.send(coordinatorActor, GetIndexValues(ref, "series", limit=4))
      probe.expectMsg(Seq("Series 0", "Series 1", "Series 2", "Series 3"))
    }
  }

  it("should be able to start ingestion, send rows, and get an ack back") {
    val ref = dataset6.ref

    probe.send(coordinatorActor, CreateDataset(dataset6))
    probe.expectMsg(DatasetCreated)
    startIngestion(dataset6, 1)
    probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset6)))
    probe.expectMsg(Ack(98L))

    // Flush not needed for MemStores.....
    // probe.send(coordActor, Flush(ref, 0))
    // probe.expectMsg(Flushed)

    probe.send(coordinatorActor, GetIngestionStats(ref))
    probe.expectMsg(IngestionActor.IngestionStatus(99))

    // Now, read stuff back from the column store and check that it's all there
    val split = memStore.getScanSplits(ref, 1).head
    val query = QuerySpec("AvgTone", AggregationFunction.Sum)
    val agg1 = memStore.aggregate(dataset6, query, FilteredPartitionScan(split))
                       .get.runAsync.futureValue
    agg1.result.asInstanceOf[Array[Double]](0) should be (575.24 +- 0.01)
  }

  it("should stop datasetActor if error occurs and prevent further ingestion") {
    probe.send(coordinatorActor, CreateDataset(dataset1))
    probe.expectMsg(DatasetCreated)

    val ref = dataset1.ref
    startIngestion(dataset1, 1)

    EventFilter[NumberFormatException](occurrences = 1) intercept {
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, readers ++ Seq(badLine))))
      // This should trigger an error, and datasetCoordinatorActor will stop.  A stop event will come and cause
      // shard status to be updated
    }

    shardManager.shardMappers(ref).statusForShard(0) shouldEqual ShardStatusStopped

    // Now, if we send more rows, we will get UnknownDataset
    probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1)))
    probe.expectMsg(UnknownDataset)
  }
}


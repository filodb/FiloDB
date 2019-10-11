package filodb.coordinator

import java.net.InetAddress

import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, AddressFromURIString, PoisonPill, Props}
import akka.pattern.gracefulStop
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import filodb.coordinator.queryengine2.UnavailablePromQlQueryParams
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core._
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.metadata.{Column, Dataset}
import filodb.core.query._
import filodb.prometheus.ast.TimeStepParams
import filodb.prometheus.parse.Parser

object NodeCoordinatorActorSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
// TODO disabled since several tests in this class are flaky in Travis.
class NodeCoordinatorActorSpec extends ActorTest(NodeCoordinatorActorSpec.getNewSystem)
  with ScalaFutures with BeforeAndAfterEach {

  import akka.testkit._
  import client.DatasetCommands._
  import client.IngestionCommands._
  import client.QueryCommands._
  import Column.ColumnType._
  import filodb.query._
  import GdeltTestData._
  import NodeClusterActor._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(50, Millis))

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
  private lazy val memStore = cluster.memStore.asInstanceOf[TimeSeriesMemStore]
  private lazy val metaStore = cluster.metaStore

  implicit val ec = cluster.ec

  val strategy = DefaultShardAssignmentStrategy
  protected val shardManager = new ShardManager(cluster.settings, DefaultShardAssignmentStrategy)

  val clusterActor = system.actorOf(Props(new Actor {
    import StatusActor._
    def receive: Receive = {
      case SubscribeShardUpdates(ref) => shardManager.subscribe(sender(), ref)
      case e: ShardEvent              => shardManager.updateFromExternalShardEvent(sender(), e)
      case EventEnvelope(seq, events) => events.foreach(e => shardManager.updateFromExternalShardEvent(sender(), e))
                                         sender() ! StatusAck(seq)
    }
  }))
  var coordinatorActor: ActorRef = _
  var probe: TestProbe = _
  var shardMap = new ShardMapper(1)
  val nodeCoordProps = NodeCoordinatorActor.props(metaStore, memStore, cluster.settings)

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
    val sd = SetupDataset(dataset, resources, NodeClusterActor.noOpSource, TestData.storeConf)
    coordinatorActor ! sd
    shardManager.addDataset(dataset, sd.config, sd.source, Some(self))
    shardManager.subscribe(probe.ref, dataset.ref)
    probe.expectMsgPF() { case CurrentShardSnapshot(ds, mapper) => } // for subscription
    for { i <- 0 until numShards } { // for each shard assignment
      probe.expectMsgPF() { case CurrentShardSnapshot(ds, mapper) =>
        shardMap = mapper
      }
    }
    probe.ignoreMsg { case m: Any => m.isInstanceOf[CurrentShardSnapshot] }
  }

  def filters(keyValue: (String, String)*): Seq[ColumnFilter] =
    keyValue.toSeq.map { case (k, v) => ColumnFilter(k, Filter.Equals(v)) }

  describe("NodeCoordinatorActor DatasetOps commands") {
    it("should be able to create new dataset (really for unit testing only)") {
      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetCreated)
    }
  }

  val timeMinSchema = ResultSchema(Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("min", DoubleColumn)), 1)
  val countSchema = ResultSchema(Seq(ColumnInfo("timestamp", LongColumn), ColumnInfo("count", DoubleColumn)), 1)
  val qOpt = QueryOptions(shardOverrides = Some(Seq(0)))

  describe("QueryActor commands and responses") {
    import MachineMetricsData._

    def setupTimeSeries(numShards: Int = 1): DatasetRef = {
      probe.send(coordinatorActor, CreateDataset(dataset1))
      probe.expectMsg(DatasetCreated)

      startIngestion(MachineMetricsData.dataset1, numShards)
      dataset1.ref
    }

    it("should return UnknownDataset if attempting to query before ingestion set up") {
      val ref = MachineMetricsData.dataset1.ref
      val q1 = LogicalPlan2Query(ref, RawSeries(AllChunksSelector, filters("series" -> "Series 1"),
        Seq("min")), UnavailablePromQlQueryParams)
      probe.send(coordinatorActor, q1)
      probe.expectMsg(UnknownDataset)
    }

    it("should return chunks when querying all samples after ingesting rows") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, multiSeriesData().take(20))))
      probe.expectMsg(Ack(0L))

      memStore.refreshIndexForTesting(dataset1.ref)

      // Query existing partition: Series 1
      val q1 = LogicalPlan2Query(ref, RawSeries(AllChunksSelector, filters("series" -> "Series 1"),
        Seq("min")),UnavailablePromQlQueryParams, qOpt)

      probe.send(coordinatorActor, q1)
      val info1 = probe.expectMsgPF(3.seconds.dilated) {
        case QueryResult(_, schema, srvs) =>
          schema.columns shouldEqual timeMinSchema.columns
          srvs should have length (1)
          srvs(0).rows.toSeq should have length (2)   // 2 samples per series
      }

      // Query nonexisting partition
      val q2 = LogicalPlan2Query(ref, RawSeries(AllChunksSelector, filters("series" -> "NotSeries"),
        Seq("min")), UnavailablePromQlQueryParams, qOpt)
      probe.send(coordinatorActor, q2)
      val info2 = probe.expectMsgPF(3.seconds.dilated) {
        case QueryResult(_, schema, Nil) =>
          schema.columns shouldEqual timeMinSchema.columns
      }
    }

    it("should return QueryError if bad arguments or could not execute") {
      val ref = setupTimeSeries()
      val to = System.currentTimeMillis() / 1000
      val from = to - 50
      val qParams = TimeStepParams(from, 10, to)
      val logPlan = Parser.queryRangeToLogicalPlan("topk(a1b, series_1)", qParams)
      val q1 = LogicalPlan2Query(ref, logPlan, UnavailablePromQlQueryParams, qOpt)
      probe.send(coordinatorActor, q1)
      probe.expectMsgClass(classOf[QueryError])
    }

    it("should return results in QueryResult if valid LogicalPlanQuery") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, linearMultiSeries().take(40))))
      probe.expectMsg(Ack(0L))


      // Try a filtered partition query
      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = LogicalPlan2Query(ref, Aggregate(AggregationOperator.Avg, PeriodicSeries(RawSeries(AllChunksSelector,
        multiFilter, Seq("min")), 120000L, 10000L, 130000L)), UnavailablePromQlQueryParams, qOpt)
      memStore.refreshIndexForTesting(dataset1.ref)
      probe.send(coordinatorActor, q2)
      probe.expectMsgPF() {
        case QueryResult(_, schema, vectors) =>
          schema.columns shouldEqual timeMinSchema.columns
          vectors should have length (1)
          vectors(0).rows.map(_.getDouble(1)).toSeq shouldEqual Seq(14.0, 24.0)
      }

      // Query the "count" long column, validate schema.  Should be able to translate everything
      val q3 = LogicalPlan2Query(ref,
                 Aggregate(AggregationOperator.Avg,
                   PeriodicSeries(
                     RawSeries(AllChunksSelector, multiFilter, Seq("count")), 120000L, 10000L, 130000L)),
        UnavailablePromQlQueryParams, qOpt)
      probe.send(coordinatorActor, q3)
      probe.expectMsgPF() {
        case QueryResult(_, schema, vectors) =>
          schema.columns shouldEqual countSchema.columns
          vectors should have length (1)
          vectors(0).rows.map(_.getDouble(1)).toSeq shouldEqual Seq(98.0, 108.0)
      }

      // What if filter returns no results?
      val q4 = LogicalPlan2Query(ref,
                 Aggregate(AggregationOperator.Avg,
                   PeriodicSeries(
                     RawSeries(AllChunksSelector, filters("series" -> "foobar"), Seq("min")), 120000L,
                     10000L, 130000L)), UnavailablePromQlQueryParams,  qOpt)
      probe.send(coordinatorActor, q4)
      probe.expectMsgPF() {
        case QueryResult(_, schema, vectors) =>
          schema.columns shouldEqual timeMinSchema.columns
          vectors should have length (0)
      }
    }

    it("should parse and execute concurrent LogicalPlan queries") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, linearMultiSeries().take(40))))
      probe.fishForSpecificMessage() {case Ack(0L) => }

      memStore.refreshIndexForTesting(dataset1.ref)

      val numQueries = 6

      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = LogicalPlan2Query(ref,
                 Aggregate(AggregationOperator.Avg,
                   PeriodicSeries(
                     RawSeries(AllChunksSelector, multiFilter, Seq("min")), 120000L, 10000L, 130000L)),
                UnavailablePromQlQueryParams, qOpt)
      (0 until numQueries).foreach { i => probe.send(coordinatorActor, q2) }

      (0 until numQueries).foreach { _ =>
        probe.expectMsgPF() {
          case QueryResult(_, schema, vectors) =>
            schema.columns shouldEqual timeMinSchema.columns
            vectors should have length (1)
            vectors(0).rows.map(_.getDouble(1)).toSeq shouldEqual Seq(14.0, 24.0)
        }
      }
    }

    it("should aggregate from multiple shards") {
      val ref = setupTimeSeries(2)
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, linearMultiSeries().take(30))))
      probe.fishForSpecificMessage() {case Ack(0L) => }
      probe.send(coordinatorActor, IngestRows(ref, 1, records(dataset1, linearMultiSeries(130000L).take(20))))
      probe.fishForSpecificMessage() {case Ack(0L) => }

      memStore.refreshIndexForTesting(dataset1.ref)

      // Should return results from both shards
      // shard 1 - timestamps 110000 -< 130000;  shard 2 - timestamps 130000 <- 1400000
      val queryOpt = QueryOptions(shardOverrides = Some(Seq(0, 1)))
      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = LogicalPlan2Query(ref,
                 Aggregate(AggregationOperator.Avg,
                   PeriodicSeries(
                     RawSeries(AllChunksSelector, multiFilter, Seq("min")), 120000L, 10000L, 140000L)),
                  UnavailablePromQlQueryParams, queryOpt)
      probe.send(coordinatorActor, q2)
      probe.expectMsgPF() {
        case QueryResult(_, schema, vectors) =>
          schema.columns shouldEqual timeMinSchema.columns
          vectors should have length (1)
          vectors(0).rows.map(_.getDouble(1)).toSeq shouldEqual Seq(14.0, 24.0, 14.0)
      }
    }

    it("should concatenate raw series from multiple shards") {
      val ref = setupTimeSeries(2)
      // Same series is ingested into two shards.  I know, this should not happen in real life.
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, linearMultiSeries().take(30))))
      probe.fishForSpecificMessage() {case Ack(0L) => }
      probe.send(coordinatorActor, IngestRows(ref, 1, records(dataset1, linearMultiSeries(130000L).take(20))))
      probe.fishForSpecificMessage() {case Ack(0L) => }

      memStore.refreshIndexForTesting(dataset1.ref)

      val queryOpt = QueryOptions(shardOverrides = Some(Seq(0, 1)))
      val series2 = (2 to 4).map(n => s"Series $n")
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2.toSet.asInstanceOf[Set[Any]])))
      val q2 = LogicalPlan2Query(ref, RawSeries(AllChunksSelector, multiFilter, Seq("min")),
        UnavailablePromQlQueryParams, queryOpt)
      probe.send(coordinatorActor, q2)
      val info1 = probe.expectMsgPF(3.seconds.dilated) {
        case QueryResult(_, schema, srvs) =>
          schema.columns shouldEqual timeMinSchema.columns
          srvs should have length (6)
          val groupedByKey = srvs.groupBy(_.key.labelValues)
          groupedByKey.map(_._2.length) shouldEqual Seq(2, 2, 2)
          val lengths = srvs.map(_.rows.toSeq.length)
          lengths.min shouldEqual 2
          lengths.max shouldEqual 3
      }
    }

    implicit val askTimeout = Timeout(5.seconds)

    it("should respond to GetIndexNames and GetIndexValues") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, linearMultiSeries().take(30))))
      probe.expectMsg(Ack(0L))

      memStore.refreshIndexForTesting(dataset1.ref)

      probe.send(coordinatorActor, GetIndexNames(ref))
      probe.expectMsg(Seq("series"))

      probe.send(coordinatorActor, GetIndexValues(ref, "series", 0, limit=4))
      probe.expectMsg(Seq(("Series 0", 1), ("Series 1", 1), ("Series 2", 1), ("Series 3", 1)))
    }

    it("should restart QueryActor on error") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset1, linearMultiSeries().take(30))))
      probe.expectMsg(Ack(0L))

      memStore.refreshIndexForTesting(dataset1.ref)

      probe.send(coordinatorActor, GetIndexNames(ref))
      probe.expectMsg(Seq("series"))

      //actor should restart and serve queries again
      probe.send(coordinatorActor, GetIndexValues(ref, "series", 0, limit=4))
      probe.expectMsg(Seq(("Series 0", 1), ("Series 1", 1), ("Series 2", 1), ("Series 3", 1)))
    }
  }

  // The test below requires new QueryEngine to be able to query from different columns, which doesn't work yet
  it("should be able to start ingestion, send rows, and get an ack back") {
    val ref = dataset6.ref

    probe.send(coordinatorActor, CreateDataset(dataset6))
    probe.expectMsg(DatasetCreated)
    startIngestion(dataset6, 1)
    probe.send(coordinatorActor, IngestRows(ref, 0, records(dataset6)))
    probe.expectMsg(Ack(0L))

    // Flush not needed for MemStores.....
    // probe.send(coordActor, Flush(ref, 0))
    // probe.expectMsg(Flushed)

    probe.send(coordinatorActor, GetIngestionStats(ref))
    probe.expectMsg(IngestionActor.IngestionStatus(99))

    probe.send(coordinatorActor, StatusActor.GetCurrentEvents)
    probe.expectMsg(Map(ref -> Seq(IngestionStarted(ref, 0, coordinatorActor))))

    memStore.refreshIndexForTesting(dataset6.ref)
    // Also the original aggregator is sum(sum_over_time(....)) which is not quite represented by below plan
    // Below plan is really sum each time bucket
    val q2 = LogicalPlan2Query(ref,
               Aggregate(AggregationOperator.Sum,
                 PeriodicSeries(  // No filters, operate on all rows.  Yes this is not a possible PromQL query. So what
                   RawSeries(AllChunksSelector, Nil, Seq("AvgTone")), 0, 10, 99)), UnavailablePromQlQueryParams , qOpt)
    probe.send(coordinatorActor, q2)
    probe.expectMsgPF() {
      case QueryResult(_, schema, vectors) =>
        schema.columns shouldEqual Seq(ColumnInfo("GLOBALEVENTID", LongColumn),
                                       ColumnInfo("AvgTone", DoubleColumn))
        vectors should have length (1)
        // vectors(0).rows.map(_.getDouble(1)).toSeq shouldEqual Seq(575.24)
        // TODO:  verify if the expected results are right.  They are something....
        vectors(0).rows.map(_.getDouble(1).toInt).toSeq shouldEqual Seq(5, 47, 81, 122, 158, 185, 229, 249, 275, 323)
    }
  }

  // TODO: need to find a new way to incur this error.   The problem is that when we create the BinaryRecords
  // the error occurs before we even send the IngestRows over.
  ignore("should stop datasetActor if error occurs and prevent further ingestion") {
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


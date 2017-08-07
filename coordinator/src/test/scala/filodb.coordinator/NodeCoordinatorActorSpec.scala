package filodb.coordinator

import java.net.InetAddress

import scala.concurrent.duration._
import scala.io.Source
import scala.util.Try

import akka.actor.{ActorRef, AddressFromURIString, PoisonPill}
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import org.velvia.filo.ZeroCopyUTF8String
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import scalax.file.Path

import filodb.coordinator.NodeCoordinatorActor.ReloadDCA
import filodb.core._
import filodb.core.metadata.{DataColumn, Dataset, RichProjection}
import filodb.core.query.{AggregationFunction, ColumnFilter, Filter, HistogramBucket}
import filodb.core.store._

object NodeCoordinatorActorSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class NodeCoordinatorActorSpec extends ActorTest(NodeCoordinatorActorSpec.getNewSystem)
  with ScalaFutures with BeforeAndAfterEach {
  import akka.testkit._
  import DatasetCommands._
  import IngestionCommands._
  import GdeltTestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

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
  implicit val context = scala.concurrent.ExecutionContext.Implicits.global


  override def beforeAll() : Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  var coordinatorActor: ActorRef = _
  var probe: TestProbe = _

  import NodeClusterActor.ShardMapUpdate
  val shardMap = new ShardMapper(1)

  override def beforeEach(): Unit = {
    metaStore.clearAllData().futureValue
    memStore.reset()
    coordinatorActor = system.actorOf(NodeCoordinatorActor.props(metaStore, memStore, cluster.columnStore, selfAddress, config))
    probe = TestProbe()
    shardMap.clear()
    shardMap.registerNode(Seq(0), coordinatorActor).isSuccess should equal (true)
  }

  override def afterEach(): Unit = {
    gracefulStop(coordinatorActor, 3.seconds.dilated, PoisonPill).futureValue
    val walDir = config.getString("write-ahead-log.memtable-wal-dir")
    val path = Path.fromString (walDir)
    Try(path.deleteRecursively(continueOnFailure = false))
  }

  def createTable(dataset: Dataset, columns: Seq[DataColumn]): Unit = {
    metaStore.newDataset(dataset).futureValue should equal (Success)
    val ref = DatasetRef(dataset.name)
    columns.foreach { col => metaStore.newColumn(col, ref).futureValue should equal (Success) }
  }

  val colNames = schema.map(_.name)

  describe("NodeCoordinatorActor DatasetOps commands") {
    it("should be able to create new dataset") {
      probe.send(coordinatorActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetCreated)
    }

    it("should return DatasetAlreadyExists creating dataset that already exists") {
      probe.send(coordinatorActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetCreated)

      probe.send(coordinatorActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetAlreadyExists)
    }

    it("should be able to drop a dataset") {
      probe.send(coordinatorActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetCreated)

      val ref = DatasetRef(dataset1.name)
      metaStore.getDataset(ref).futureValue should equal (dataset1)

      probe.send(coordinatorActor, DropDataset(DatasetRef(dataset1.name)))
      probe.expectMsg(DatasetDropped)

      // Now verify that the dataset was indeed dropped
      metaStore.getDataset(ref).failed.futureValue shouldBe a [NotFoundError]
    }
  }

  describe("QueryActor commands and responses") {
    import MachineMetricsData._
    import QueryCommands._

    def setupTimeSeries(map: ShardMapper = shardMap): DatasetRef = {
      probe.send(coordinatorActor, CreateDataset(dataset1, schemaWithSeries))
      probe.expectMsg(DatasetCreated)

      probe.send(coordinatorActor, ShardMapUpdate(dataset1.projections.head.dataset, map))

      probe.send(coordinatorActor, DatasetSetup(dataset1, schemaWithSeries.map(_.toString), 0))
      dataset1.projections.head.dataset
    }

    it("should return UnknownDataset if attempting to query before ingestion set up") {
      probe.send(coordinatorActor, CreateDataset(dataset1, schemaWithSeries))
      probe.expectMsg(DatasetCreated)

      val ref = projection1.datasetRef
      val q1 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq("Series 1")), AllPartitionData)
      probe.send(coordinatorActor, q1)
      probe.expectMsg(UnknownDataset)
    }

    it("should return raw chunks with a RawQuery after ingesting rows") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(multiSeriesData()).take(20)))
      probe.expectMsg(Ack(19L))

      // Query existing partition: Series 1
      val q1 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq("Series 1")), AllPartitionData)
      probe.send(coordinatorActor, q1)
      val info1 = probe.expectMsgPF(3.seconds.dilated) {
        case q @ QueryInfo(_, ref, _) => q
      }
      probe.expectMsgPF(3.seconds.dilated) {
        case QueryRawChunks(info1.id, _, chunks) => chunks.size should equal (1)
      }
      probe.expectMsg(QueryEndRaw(info1.id))

      // Query nonexisting partition
      val q2 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq("NotSeries")), AllPartitionData)
      probe.send(coordinatorActor, q2)
      val info2 = probe.expectMsgPF(3.seconds.dilated) {
        case q @ QueryInfo(_, ref, _) => q
      }
      probe.expectMsg(QueryEndRaw(info2.id))
    }

    it("should return BadArgument/BadQuery if wrong type of partition key passed") {
      val ref = setupTimeSeries()
      val q1 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq(-1)), AllPartitionData)
      probe.send(coordinatorActor, q1)
      probe.expectMsgClass(classOf[BadQuery])
    }

    it("should return BadQuery if aggregation function not defined") {
      val ref = setupTimeSeries()
      val q1 = AggregateQuery(ref, 0, QueryArgs("not-a-func"), SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordinatorActor, q1)
      probe.expectMsg(BadQuery("No such aggregation function not-a-func"))

      val q2 = AggregateQuery(ref, 0, QueryArgs("TimeGroupMin"), SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordinatorActor, q2)
      probe.expectMsg(BadQuery("No such aggregation function TimeGroupMin"))
    }

    it("should return WrongNumberOfArgs if number of arguments wrong") {
      val ref = setupTimeSeries()
      val q1 = AggregateQuery(ref, 0, QueryArgs("time_group_avg", Seq("timestamp", "min")),
                              SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordinatorActor, q1)
      probe.expectMsg(WrongNumberOfArgs(2, 5))
    }

    it("should return BadArgument if arguments could not be parsed successfully") {
      val ref = setupTimeSeries()
      val q1 = AggregateQuery(ref, 0, QueryArgs("time_group_avg", Seq("timestamp", "min", "a", "b", "100")),
                              SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordinatorActor, q1)
      probe.expectMsgClass(classOf[BadArgument])
    }

    it("should return BadArgument if wrong types of columns are passed") {
      val ref = setupTimeSeries()
      val args = QueryArgs("time_group_avg", Seq("p90", "min", "100000", "130000", "100"))
      val q1 = AggregateQuery(ref, 0, args, SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordinatorActor, q1)
      val msg = probe.expectMsgClass(classOf[BadArgument])
      msg.msg should include ("not in allowed set")
    }

    it("should return results in AggregateResponse if valid AggregateQuery") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      val args = QueryArgs("time_group_avg", Seq("timestamp", "min", "110000", "130000", "2"))
      val series = (1 to 3).map(n => Seq(s"Series $n"))
      val q1 = AggregateQuery(ref, 0, args, MultiPartitionQuery(series))
      probe.send(coordinatorActor, q1)
      val answer = probe.expectMsgClass(classOf[AggregateResponse[Double]])
      answer.elementClass should equal (classOf[Double])
      answer.elements should equal (Array(13.0, 23.0))

      // Try a filtered partition query
      import ZeroCopyUTF8String._
      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(multiFilter))
      probe.send(coordinatorActor, q2)
      val answer2 = probe.expectMsgClass(classOf[AggregateResponse[Double]])
      answer2.elementClass should equal (classOf[Double])
      answer2.elements should equal (Array(14.0, 24.0))

      // What if filter returns no results?
      val filter3 = Seq(ColumnFilter("series", Filter.Equals("foobar".utf8)))
      val q3 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(filter3))
      probe.send(coordinatorActor, q3)
      val answer3 = probe.expectMsgClass(classOf[AggregateResponse[Double]])
      answer3.elementClass should equal (classOf[Double])
      answer3.elements.length should equal (2)
    }

    it("should aggregate from multiple shards") {
      val map = new ShardMapper(2)
      map.registerNode(Seq(0, 1), coordinatorActor).isSuccess should equal (true)
      val ref = setupTimeSeries(map)
      probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))
      probe.send(coordinatorActor, IngestRows(ref, 0, 1, records(linearMultiSeries(130000L)).take(20)))
      probe.expectMsg(Ack(19L))

      // Should return results from both shards
      // shard 1 - timestamps 110000 -< 130000;  shard 2 - timestamps 130000 <- 1400000
      val args = QueryArgs("time_group_avg", Seq("timestamp", "min", "110000", "140000", "3"))
      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(multiFilter))
      probe.send(coordinatorActor, q2)
      val answer2 = probe.expectMsgClass(classOf[AggregateResponse[Double]])
      answer2.elementClass should equal (classOf[Double])
      answer2.elements should equal (Array(14.0, 24.0, 4.0))
    }

    it("should aggregate using histogram combiner") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      val args = QueryArgs("sum", Seq("min"), "histogram", Seq("2000"))
      val q1 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(Nil))
      probe.send(coordinatorActor, q1)
      val answer = probe.expectMsgClass(classOf[AggregateResponse[HistogramBucket]])
      answer.elementClass should equal (classOf[HistogramBucket])
      val buckets = answer.elements.toSeq
      buckets should have length (10)
      buckets.map(_.count) should equal (Seq(0, 0, 0, 0, 4, 6, 0, 0, 0, 0))
    }

    it("should query partitions in AggregateQuery") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      val args = QueryArgs("partition_keys", Seq("foo"))  // Doesn't matter what the column name is
      val series2 = (2 to 4).map(n => s"Series $n").toSet
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2.asInstanceOf[Set[Any]])))
      val q2 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(multiFilter))
      probe.send(coordinatorActor, q2)
      val answer2 = probe.expectMsgClass(classOf[AggregateResponse[String]])
      answer2.elementClass should equal (classOf[String])
      answer2.elements.toSet should equal (series2.map(s => s"b[$s]"))
    }

    it("should respond to GetIndexNames and GetIndexValues") {
      val ref = setupTimeSeries()
      probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(linearMultiSeries()).take(30)))
      probe.expectMsg(Ack(29L))

      probe.send(coordinatorActor, GetIndexNames(ref))
      probe.expectMsg(Seq("series"))

      probe.send(coordinatorActor, GetIndexValues(ref, "series", limit=4))
      probe.expectMsg(Seq("Series 0", "Series 1", "Series 2", "Series 3"))
    }
  }

  it("should be able to start ingestion, send rows, and get an ack back") {
    probe.send(coordinatorActor, CreateDataset(dataset6, schema))
    probe.expectMsg(DatasetCreated)

    val ref = projection6.datasetRef
    probe.send(coordinatorActor, ShardMapUpdate(ref, shardMap))
    probe.send(coordinatorActor, DatasetSetup(projection6.dataset, schema.map(_.toString), 0))

    probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(projection6)))
    probe.expectMsg(Ack(98L))

    // Flush not needed for MemStores.....
    // probe.send(coordActor, Flush(ref, 0))
    // probe.expectMsg(Flushed)

    probe.send(coordinatorActor, GetIngestionStats(ref, 0))
    probe.expectMsg(MemStoreCoordActor.Status(99, None))

    // Now, read stuff back from the column store and check that it's all there
    val split = memStore.getScanSplits(ref, 1).head
    val query = QuerySpec(AggregationFunction.Sum, Seq("AvgTone"))
    val agg1 = memStore.aggregate(projection6, 0, query, FilteredPartitionScan(split))
                       .get.runAsync.futureValue
    agg1.result.asInstanceOf[Array[Double]](0) should be (575.24 +- 0.01)
  }

  it("should stop datasetActor if error occurs and prevent further ingestion") {
    probe.send(coordinatorActor, CreateDataset(dataset1, schema))
    probe.expectMsg(DatasetCreated)

    val ref = projection1.datasetRef
    probe.send(coordinatorActor, ShardMapUpdate(ref, shardMap))
    probe.send(coordinatorActor, DatasetSetup(projection1.dataset, schema.map(_.toString), 0))

    EventFilter[NumberFormatException](occurrences = 1) intercept {
      probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(projection1, readers ++ Seq(badLine))))
      // This should trigger an error, and datasetCoordinatorActor will stop, and no ack will be forthcoming.
      probe.expectNoMsg
    }

    // Now, if we send more rows, we will get UnknownDataset
    probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(projection1)))
    probe.expectMsg(UnknownDataset)
  }

  ignore("should reload dataset coordinator actors once the nodes are up") {
    probe.send(coordinatorActor, CreateDataset(dataset4, schema))
    probe.expectMsg(DatasetCreated)

    val proj = projection4.withDatabase("unittest2")
    val ref = proj.datasetRef

    generateActorException(proj)

    probe.send(coordinatorActor, ReloadDCA)
    probe.expectMsg(DCAReady)

    probe.send(coordinatorActor, Flush(ref, 0))
    probe.expectMsg(Flushed)

    probe.send(coordinatorActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 0, -1, 0))

  }

  ignore("should be able to create new WAL files once the reload and flush is complete") {
    probe.send(coordinatorActor, CreateDataset(dataset4, schema))
    probe.expectMsg(DatasetCreated)

    val proj = projection4.withDatabase("unittest2")
    val ref = proj.datasetRef

    generateActorException(proj)

    probe.send(coordinatorActor, ReloadDCA)
    probe.expectMsg(DCAReady)

    var numRows = 0
    if(config.getBoolean("write-ahead-log.write-ahead-log-enabled")){
      numRows = 99
    }
    probe.send(coordinatorActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(0, 0, 0, numRows, -1, 0))

    // Ingest more rows to create new WAL file
    probe.send(coordinatorActor, CheckCanIngest(ref, 0))
    probe.expectMsg(CanIngest(true))

    probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(projection4)))
    probe.expectMsg(Ack(98L))

    Thread sleep 2000

    probe.send(coordinatorActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 0, -1, 99L))

  }

  def generateActorException(proj: RichProjection): Unit = {
    val ref = proj.datasetRef
    probe.send(coordinatorActor, DatasetSetup(proj.dataset, schema.map(_.toString), 0))

    probe.send(coordinatorActor, CheckCanIngest(ref, 0))
    probe.expectMsg(CanIngest(true))

    probe.send(coordinatorActor, IngestRows(ref, 0, 0, records(proj)))
    probe.expectMsg(Ack(98L))

    probe.send(coordinatorActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(0, 0, 0, 99, -1, 99L))

    // val gdeltLines = Source.fromURL(getClass.getResource("/GDELT-sample-test2.csv"))
    //   .getLines.toSeq.drop(1) // drop the header line

    // val readers2 = gdeltLines.map { line => ArrayStringRowReader(line.split(",")) }

    // EventFilter[NumberFormatException](occurrences = 1) intercept {
    //   probe.send(coordActor, IngestRows(ref, 0, 0, readers2))
    //   // This should trigger an error, and datasetCoordinatorActor will stop, and no ack will be forthcoming.
    //   probe.expectNoMsg
    // }
    // Now, if we send more rows, we will get UnknownDataset
    // probe.send(coordActor, IngestRows(ref, 0, 0, readers))
    // probe.expectMsg(UnknownDataset)
  }
}


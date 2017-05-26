package filodb.coordinator

import akka.actor.{ActorRef, PoisonPill}
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import org.velvia.filo.{ArrayStringRowReader, ZeroCopyUTF8String}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Try
import scalax.file.Path

import filodb.coordinator.NodeCoordinatorActor.ReloadDCA
import filodb.core._
import filodb.core.metadata.{DataColumn, Dataset, RichProjection}
import filodb.core.query.{ColumnFilter, Filter, HistogramBucket}
import filodb.core.store._

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}


object NodeCoordinatorActorSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class NodeCoordinatorActorSpec extends ActorTest(NodeCoordinatorActorSpec.getNewSystem)
with CoordinatorSetup with ScalaFutures {
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

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global
  lazy val columnStore = new InMemoryColumnStore(context)
  lazy val metaStore = new InMemoryMetaStore

  override def beforeAll() : Unit = {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  var coordActor: ActorRef = _
  var probe: TestProbe = _

  before {
    metaStore.clearAllData().futureValue
    memStore.reset()
    coordActor = system.actorOf(NodeCoordinatorActor.props(metaStore, memStore, columnStore, config))
    probe = TestProbe()
  }

  after {
    gracefulStop(coordActor, 3.seconds.dilated, PoisonPill).futureValue
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
      probe.send(coordActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetCreated)
    }

    it("should return DatasetAlreadyExists creating dataset that already exists") {
      probe.send(coordActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetCreated)

      probe.send(coordActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetAlreadyExists)
    }

    it("should be able to drop a dataset") {
      probe.send(coordActor, CreateDataset(dataset1, schema))
      probe.expectMsg(DatasetCreated)

      val ref = DatasetRef(dataset1.name)
      metaStore.getDataset(ref).futureValue should equal (dataset1)

      probe.send(coordActor, DropDataset(DatasetRef(dataset1.name)))
      probe.expectMsg(DatasetDropped)

      // Now verify that the dataset was indeed dropped
      metaStore.getDataset(ref).failed.futureValue shouldBe a [NotFoundError]
    }
  }

  import NodeClusterActor.ShardMapUpdate
  val shardMap = new ShardMapper(1)
  shardMap.registerNode(Seq(0), coordActor)

  describe("QueryActor commands and responses") {
    import MachineMetricsData._
    import QueryCommands._

    def setupTimeSeries(): DatasetRef = {
      probe.send(coordActor, CreateDataset(dataset1, schemaWithSeries))
      probe.expectMsg(DatasetCreated)

      probe.send(coordActor, ShardMapUpdate(dataset1.projections.head.dataset, shardMap))

      probe.send(coordActor, DatasetSetup(dataset1, schemaWithSeries.map(_.toString), 0))
      dataset1.projections.head.dataset
    }

    it("should return UnknownDataset if attempting to query before ingestion set up") {
      probe.send(coordActor, CreateDataset(dataset1, schemaWithSeries))
      probe.expectMsg(DatasetCreated)

      val ref = projection1.datasetRef
      val q1 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq("Series 1")), AllPartitionData)
      probe.send(coordActor, q1)
      probe.expectMsg(UnknownDataset)
    }

    it("should return raw chunks with a RawQuery after ingesting rows") {
      val ref = setupTimeSeries()
      probe.send(coordActor, IngestRows(ref, 0, mapper(multiSeriesData()).take(20), 1L))
      probe.expectMsg(Ack(1L))

      // Query existing partition: Series 1
      val q1 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq("Series 1")), AllPartitionData)
      probe.send(coordActor, q1)
      val info1 = probe.expectMsgPF(3.seconds.dilated) {
        case q @ QueryInfo(_, ref, _) => q
      }
      probe.expectMsgPF(3.seconds.dilated) {
        case QueryRawChunks(info1.id, _, chunks) => chunks.size should equal (1)
      }
      probe.expectMsg(QueryEndRaw(info1.id))

      // Query nonexisting partition
      val q2 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq("NotSeries")), AllPartitionData)
      probe.send(coordActor, q2)
      val info2 = probe.expectMsgPF(3.seconds.dilated) {
        case q @ QueryInfo(_, ref, _) => q
      }
      probe.expectMsg(QueryEndRaw(info2.id))
    }

    it("should return BadArgument/BadQuery if wrong type of partition key passed") {
      val ref = setupTimeSeries()
      val q1 = RawQuery(ref, 0, Seq("min"), SinglePartitionQuery(Seq(-1)), AllPartitionData)
      probe.send(coordActor, q1)
      probe.expectMsgClass(classOf[BadQuery])
    }

    it("should return BadQuery if aggregation function not defined") {
      val ref = setupTimeSeries()
      val q1 = AggregateQuery(ref, 0, QueryArgs("not-a-func"), SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordActor, q1)
      probe.expectMsg(BadQuery("No such aggregation function not-a-func"))

      val q2 = AggregateQuery(ref, 0, QueryArgs("TimeGroupMin"), SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordActor, q2)
      probe.expectMsg(BadQuery("No such aggregation function TimeGroupMin"))
    }

    it("should return WrongNumberOfArgs if number of arguments wrong") {
      val ref = setupTimeSeries()
      val q1 = AggregateQuery(ref, 0, QueryArgs("time_group_avg", Seq("timestamp", "min")),
                              SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordActor, q1)
      probe.expectMsg(WrongNumberOfArgs(2, 5))
    }

    it("should return BadArgument if arguments could not be parsed successfully") {
      val ref = setupTimeSeries()
      val q1 = AggregateQuery(ref, 0, QueryArgs("time_group_avg", Seq("timestamp", "min", "a", "b", "100")),
                              SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordActor, q1)
      probe.expectMsgClass(classOf[BadArgument])
    }

    it("should return BadArgument if wrong types of columns are passed") {
      val ref = setupTimeSeries()
      val args = QueryArgs("time_group_avg", Seq("p90", "min", "100000", "130000", "100"))
      val q1 = AggregateQuery(ref, 0, args, SinglePartitionQuery(Seq("Series 1")))
      probe.send(coordActor, q1)
      val msg = probe.expectMsgClass(classOf[BadArgument])
      msg.msg should include ("not in allowed set")
    }

    it("should return results in AggregateResponse if valid AggregateQuery") {
      val ref = setupTimeSeries()
      probe.send(coordActor, IngestRows(ref, 0, mapper(linearMultiSeries()).take(30), 1L))
      probe.expectMsg(Ack(1L))

      val args = QueryArgs("time_group_avg", Seq("timestamp", "min", "110000", "130000", "2"))
      val series = (1 to 3).map(n => Seq(s"Series $n"))
      val q1 = AggregateQuery(ref, 0, args, MultiPartitionQuery(series))
      probe.send(coordActor, q1)
      val answer = probe.expectMsgClass(classOf[AggregateResponse[Double]])
      answer.elementClass should equal (classOf[Double])
      answer.elements should equal (Array(13.0, 23.0))

      // Try a filtered partition query
      import ZeroCopyUTF8String._
      val series2 = (2 to 4).map(n => s"Series $n").toSet.asInstanceOf[Set[Any]]
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2)))
      val q2 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(multiFilter))
      probe.send(coordActor, q2)
      val answer2 = probe.expectMsgClass(classOf[AggregateResponse[Double]])
      answer2.elementClass should equal (classOf[Double])
      answer2.elements should equal (Array(14.0, 24.0))

      // What if filter returns no results?
      val filter3 = Seq(ColumnFilter("series", Filter.Equals("foobar".utf8)))
      val q3 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(filter3))
      probe.send(coordActor, q3)
      val answer3 = probe.expectMsgClass(classOf[AggregateResponse[Double]])
      answer3.elementClass should equal (classOf[Double])
      answer3.elements.length should equal (2)
    }

    it("should aggregate using histogram combiner") {
      val ref = setupTimeSeries()
      probe.send(coordActor, IngestRows(ref, 0, mapper(linearMultiSeries()).take(30), 1L))
      probe.expectMsg(Ack(1L))

      val args = QueryArgs("sum", Seq("min"), "histogram", Seq("2000"))
      val q1 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(Nil))
      probe.send(coordActor, q1)
      val answer = probe.expectMsgClass(classOf[AggregateResponse[HistogramBucket]])
      answer.elementClass should equal (classOf[HistogramBucket])
      val buckets = answer.elements.toSeq
      buckets should have length (10)
      buckets.map(_.count) should equal (Seq(0, 0, 0, 0, 4, 6, 0, 0, 0, 0))
    }

    it("should query partitions in AggregateQuery") {
      val ref = setupTimeSeries()
      probe.send(coordActor, IngestRows(ref, 0, mapper(linearMultiSeries()).take(30), 1L))
      probe.expectMsg(Ack(1L))

      val args = QueryArgs("partition_keys", Seq("foo"))  // Doesn't matter what the column name is
      val series2 = (2 to 4).map(n => s"Series $n").toSet
      val multiFilter = Seq(ColumnFilter("series", Filter.In(series2.asInstanceOf[Set[Any]])))
      val q2 = AggregateQuery(ref, 0, args, FilteredPartitionQuery(multiFilter))
      probe.send(coordActor, q2)
      val answer2 = probe.expectMsgClass(classOf[AggregateResponse[String]])
      answer2.elementClass should equal (classOf[String])
      answer2.elements.toSet should equal (series2.map(s => s"b[$s]"))
    }

    it("should respond to GetIndexNames and GetIndexValues") {
      val ref = setupTimeSeries()
      probe.send(coordActor, IngestRows(ref, 0, mapper(linearMultiSeries()).take(30), 1L))
      probe.expectMsg(Ack(1L))

      probe.send(coordActor, GetIndexNames(ref))
      probe.expectMsg(Seq("series"))

      probe.send(coordActor, GetIndexValues(ref, "series", limit=4))
      probe.expectMsg(Seq("Series 0", "Series 1", "Series 2", "Series 3"))
    }
  }

  it("should be able to start ingestion, send rows, and get an ack back") {
    probe.send(coordActor, CreateDataset(dataset6, schema))
    probe.expectMsg(DatasetCreated)

    val ref = projection6.datasetRef
    probe.send(coordActor, ShardMapUpdate(ref, shardMap))
    probe.send(coordActor, DatasetSetup(projection6.dataset, schema.map(_.toString), 0))

    probe.send(coordActor, CheckCanIngest(ref, 0))
    probe.expectMsg(CanIngest(true))

    probe.send(coordActor, IngestRows(ref, 0, readers, 1L))
    probe.expectMsg(Ack(1L))

    // Now, try to flush and check that stuff was written to columnstore...
    // Note that once we receive the Flushed message back, that means flush cycle was completed.
    probe.send(coordActor, Flush(ref, 0))
    probe.expectMsg(Flushed)

    // TODO: fix this.  Stats are not in the MemStoreCoordActor yet.
    probe.send(coordActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 0, -1, 99L))

    // Now, read stuff back from the column store and check that it's all there
    val scanMethod = SinglePartitionScan(projection6.partKey("GOV", 1979))
    val chunks = memStore.scanChunks(projection6, schema, 0, scanMethod).toSeq
    chunks should have length (1)
    chunks.head.rowIterator().map(_.getInt(6)).sum should equal (80)

    val splits = memStore.getScanSplits(ref, 1)
    splits should have length (1)
    val rowIt = memStore.scanRows(projection6, schema, 0, FilteredPartitionScan(splits.head))
    rowIt.map(_.getInt(6)).sum should equal (492)
  }

  it("should stop datasetActor if error occurs and prevent further ingestion") {
    probe.send(coordActor, CreateDataset(dataset1, schema))
    probe.expectMsg(DatasetCreated)

    val ref = projection1.datasetRef
    probe.send(coordActor, ShardMapUpdate(ref, shardMap))
    probe.send(coordActor, DatasetSetup(projection1.dataset, schema.map(_.toString), 0))

    EventFilter[NumberFormatException](occurrences = 1) intercept {
      probe.send(coordActor, IngestRows(ref, 0, readers ++ Seq(badLine), 1L))
      // This should trigger an error, and datasetCoordinatorActor will stop, and no ack will be forthcoming.
      probe.expectNoMsg
    }

    // Now, if we send more rows, we will get UnknownDataset
    probe.send(coordActor, IngestRows(ref, 0, readers, 1L))
    probe.expectMsg(UnknownDataset)
  }

  ignore("should reload dataset coordinator actors once the nodes are up") {
    probe.send(coordActor, CreateDataset(dataset4, schema))
    probe.expectMsg(DatasetCreated)

    val proj = projection4.withDatabase("unittest2")
    val ref = proj.datasetRef

    generateActorException(proj)

    probe.send(coordActor, ReloadDCA)
    probe.expectMsg(DCAReady)

    probe.send(coordActor, Flush(ref, 0))
    probe.expectMsg(Flushed)

    probe.send(coordActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 0, -1, 0))

  }

  ignore("should be able to create new WAL files once the reload and flush is complete") {
    probe.send(coordActor, CreateDataset(dataset4, schema))
    probe.expectMsg(DatasetCreated)

    val proj = projection4.withDatabase("unittest2")
    val ref = proj.datasetRef

    generateActorException(proj)

    probe.send(coordActor, ReloadDCA)
    probe.expectMsg(DCAReady)

    var numRows = 0
    if(config.getBoolean("write-ahead-log.write-ahead-log-enabled")){
      numRows = 99
    }
    probe.send(coordActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(0, 0, 0, numRows, -1, 0))

    // Ingest more rows to create new WAL file
    probe.send(coordActor, CheckCanIngest(ref, 0))
    probe.expectMsg(CanIngest(true))

    probe.send(coordActor, IngestRows(ref, 0, readers, 1L))
    probe.expectMsg(Ack(1L))

    Thread sleep 2000

    probe.send(coordActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 0, -1, 99L))

  }

  def generateActorException(proj: RichProjection): Unit = {
    val ref = proj.datasetRef
    probe.send(coordActor, DatasetSetup(proj.dataset, schema.map(_.toString), 0))

    probe.send(coordActor, CheckCanIngest(ref, 0))
    probe.expectMsg(CanIngest(true))

    probe.send(coordActor, IngestRows(ref, 0, readers, 1L))
    probe.expectMsg(Ack(1L))

    probe.send(coordActor, GetIngestionStats(ref, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(0, 0, 0, 99, -1, 99L))

    val gdeltLines = Source.fromURL(getClass.getResource("/GDELT-sample-test2.csv"))
      .getLines.toSeq.drop(1) // drop the header line

    val readers2 = gdeltLines.map { line => ArrayStringRowReader(line.split(",")) }

    EventFilter[NumberFormatException](occurrences = 1) intercept {
      probe.send(coordActor, IngestRows(ref, 0, readers2, 1L))
      // This should trigger an error, and datasetCoordinatorActor will stop, and no ack will be forthcoming.
      probe.expectNoMsg
    }
    // Now, if we send more rows, we will get UnknownDataset
    probe.send(coordActor, IngestRows(ref, 0, readers, 1L))
    probe.expectMsg(UnknownDataset)
  }
}


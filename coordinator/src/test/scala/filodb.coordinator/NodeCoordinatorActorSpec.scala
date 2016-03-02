package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.{EventFilter, TestProbe}
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core._
import filodb.core.store._
import filodb.core.metadata.{Column, DataColumn, Dataset}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span, Seconds}

object NodeCoordinatorActorSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class NodeCoordinatorActorSpec extends ActorTest(NodeCoordinatorActorSpec.getNewSystem)
with CoordinatorSetup with ScalaFutures {
  import akka.testkit._
  import NodeCoordinatorActor._
  import GdeltTestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.parseString("filodb.memtable.flush.interval = 500 ms")
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global
  lazy val columnStore = new InMemoryColumnStore(context)
  lazy val metaStore = new InMemoryMetaStore

  override def beforeAll() {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  var coordActor: ActorRef = _
  var probe: TestProbe = _

  before {
    metaStore.clearAllData().futureValue
    coordActor = system.actorOf(NodeCoordinatorActor.props(metaStore, reprojector, columnStore, config))
    probe = TestProbe()
  }

  after {
    gracefulStop(coordActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  def createTable(dataset: Dataset, columns: Seq[DataColumn]): Unit = {
    metaStore.newDataset(dataset).futureValue should equal (Success)
    columns.foreach { col => metaStore.newColumn(col).futureValue should equal (Success) }
  }

  val colNames = schema.map(_.name)

  describe("NodeCoordinatorActor SetupIngestion verification") {
    it("should return UnknownDataset when dataset missing or no columns defined") {
      createTable(Dataset("noColumns", "noSort", "seg"), Nil)

      coordActor ! SetupIngestion("none", colNames, 0)
      expectMsg(UnknownDataset)

      coordActor ! SetupIngestion("noColumns", colNames, 0)
      expectMsg(UndefinedColumns(colNames.toSet))
    }

    it("should return UndefinedColumns if trying to ingest undefined columns") {
      createTable(dataset1, schema)

      probe.send(coordActor, SetupIngestion(dataset1.name, Seq("MonthYear", "last"), 0))
      probe.expectMsg(UndefinedColumns(Set("last")))
    }

    it("should return BadSchema if dataset definition bazooka") {
      createTable(dataset1.copy(partitionColumns = Seq("foo")), schema)
      probe.send(coordActor, SetupIngestion(dataset1.name, colNames, 0))
      probe.expectMsgClass(classOf[BadSchema])
    }

    it("should get IngestionReady if try to set up concurrently for same dataset/version") {
      createTable(dataset1, schema)

      val probes = (1 to 8).map { n => TestProbe() }
      probes.foreach { probe => probe.send(coordActor, SetupIngestion(dataset1.name, colNames, 0)) }
      probes.foreach { probe => probe.expectMsg(IngestionReady) }
    }
  }

  it("should be able to start ingestion, send rows, and get an ack back") {
    probe.send(coordActor, CreateDataset(dataset3, schema))
    probe.expectMsg(DatasetCreated)
    columnStore.clearProjectionData(dataset3.projections.head).futureValue should equal (Success)

    probe.send(coordActor, SetupIngestion(dataset3.name, schema.map(_.name), 0))
    probe.expectMsg(IngestionReady)

    probe.send(coordActor, CheckCanIngest(dataset3.name, 0))
    probe.expectMsg(CanIngest(true))

    probe.send(coordActor, IngestRows(dataset3.name, 0, readers, 1L))
    probe.expectMsg(Ack(1L))

    // Now, try to flush and check that stuff was written to columnstore...
    // Note that once we receive the Flushed message back, that means flush cycle was completed.
    probe.send(coordActor, Flush(dataset3.name, 0))
    probe.expectMsg(Flushed)

    probe.send(coordActor, GetIngestionStats(dataset3.name, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 0, -1))

    // Now, read stuff back from the column store and check that it's all there
    val scanMethod = SinglePartitionScan(Seq("GOV", 1979))
    whenReady(columnStore.scanSegments(projection3, schema, 0, scanMethod)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (1)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment]
      readSeg.segInfo.segment should equal ("0")
      readSeg.rowIterator().map(_.getInt(6)).sum should equal (80)
    }

    val splits = columnStore.getScanSplits(dataset3.name, 1)
    splits should have length (1)
    whenReady(columnStore.scanRows(projection3, schema, 0, FilteredPartitionScan(splits.head))) { rowIter =>
      rowIter.map(_.getInt(6)).sum should equal (492)
    }
  }

  it("should stop datasetActor if error occurs and prevent further ingestion") {
    probe.send(coordActor, CreateDataset(dataset1, schema))
    probe.expectMsg(DatasetCreated)
    columnStore.clearProjectionData(dataset1.projections.head).futureValue should equal (Success)

    probe.send(coordActor, SetupIngestion(dataset1.name, schema.map(_.name), 0))
    probe.expectMsg(IngestionReady)

    EventFilter[NullKeyValue](occurrences = 1) intercept {
      probe.send(coordActor, IngestRows(dataset1.name, 0, readers, 1L))
      // This should trigger an error, and datasetCoordinatorActor will stop, and no ack will be forthcoming.
      probe.expectNoMsg
    }

    // Now, if we send more rows, we will get UnknownDataset
    probe.send(coordActor, IngestRows(dataset1.name, 0, readers, 1L))
    probe.expectMsg(UnknownDataset)
  }
}


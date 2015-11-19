package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.TestProbe
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import org.velvia.filo.TupleRowReader
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core._
import filodb.core.columnstore.{RowReaderSegment, SegmentSpec}
import filodb.core.metadata.{Column, Dataset}
import filodb.cassandra.AllTablesTest

object NodeCoordinatorActorSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class NodeCoordinatorActorSpec extends ActorTest(NodeCoordinatorActorSpec.getNewSystem)
with CoordinatorSetup with AllTablesTest {
  import akka.testkit._
  import NodeCoordinatorActor._
  import SegmentSpec._

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

  describe("NodeCoordinatorActor SetupIngestion verification") {
    it("should return UnknownDataset when dataset missing or no columns defined") {
      createTable(Dataset("noColumns", "noSort"), Nil)

      coordActor ! SetupIngestion("none", GdeltColNames, 0)
      expectMsg(UnknownDataset)

      coordActor ! SetupIngestion("noColumns", GdeltColNames, 0)
      expectMsg(UndefinedColumns(GdeltColNames))
    }

    it("should return UndefinedColumns if trying to ingest undefined columns") {
      createTable(GdeltDataset, GdeltColumns)

      probe.send(coordActor, SetupIngestion(dsName, Seq("monthYear", "last"), 0))
      probe.expectMsg(UndefinedColumns(Seq("last")))
    }

    it("should return BadSchema if dataset definition bazooka") {
      createTable(GdeltDataset.copy(partitionColumn = "foo"), GdeltColumns)
      probe.send(coordActor, SetupIngestion(dsName, GdeltColNames, 0))
      probe.expectMsgClass(classOf[BadSchema])
    }

    it("should get IngestionReady if try to set up concurrently for same dataset/version") {
      createTable(GdeltDataset, GdeltColumns)

      val probes = (1 to 8).map { n => TestProbe() }
      probes.foreach { probe => probe.send(coordActor, SetupIngestion(dsName, GdeltColNames, 0)) }
      probes.foreach { probe => probe.expectMsg(IngestionReady) }
    }
  }

  it("should be able to start ingestion, send rows, and get an ack back") {
    probe.send(coordActor, CreateDataset(largeDataset, schemaWithPartCol))
    probe.expectMsg(DatasetCreated)
    columnStore.clearProjectionData(largeDataset.projections.head).futureValue should equal (Success)

    probe.send(coordActor, SetupIngestion(largeDataset.name, schemaWithPartCol.map(_.name), 0))
    probe.expectMsg(IngestionReady)

    probe.send(coordActor, CheckCanIngest(largeDataset.name, 0))
    probe.expectMsg(CanIngest(true))

    probe.send(coordActor, IngestRows(largeDataset.name, 0, lotLotNames.map(TupleRowReader), 1L))
    probe.expectMsg(Ack(1L))

    // Now, try to flush and check that stuff was written to columnstore...
    // Note that once we receive the Flushed message back, that means flush cycle was completed.
    probe.send(coordActor, Flush(dataset.name, 0))
    probe.expectMsg(Flushed)

    probe.send(coordActor, GetIngestionStats(dataset.name, 0))
    probe.expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 0, -1))

    // Now, read stuff back from the column store and check that it's all there
    val keyRange = KeyRange(largeDataset.name, "nfc", 0L, 30000L)
    whenReady(columnStore.readSegments(schema, keyRange, 0)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (3)
      val readSeg = segments.head.asInstanceOf[RowReaderSegment[Long]]
      readSeg.keyRange should equal (keyRange.copy(end = keyRange.start))
      readSeg.rowIterator().map(_.getLong(2)).toSeq should equal ((0 to 99).map(_.toLong))
    }

    val splits = columnStore.getScanSplits(largeDataset.name)
    splits should have length (1)
    whenReady(columnStore.scanSegments[Long](schema, largeDataset.name, 0,
                                             params = splits.head)) { segIter =>
      val segments = segIter.toSeq
      segments should have length (6)
    }
  }
}


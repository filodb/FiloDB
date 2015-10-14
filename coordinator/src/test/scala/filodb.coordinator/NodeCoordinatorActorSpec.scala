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
import filodb.core.columnstore.RowReaderSegment
import filodb.core.metadata.{Column, Dataset}
import filodb.core.reprojector.{MapDBMemTable, NumRowsFlushPolicy}
import filodb.cassandra.AllTablesTest

object NodeCoordinatorActorSpec extends ActorSpecConfig

class NodeCoordinatorActorSpec extends ActorTest(NodeCoordinatorActorSpec.getNewSystem)
with CoordinatorSetup with AllTablesTest {
  import akka.testkit._
  import NodeCoordinatorActor._

  override def beforeAll() {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  var coordActor: ActorRef = _
  var probe: TestProbe = _
  lazy val memTable = new MapDBMemTable(config)
  lazy val flushPolicy = new NumRowsFlushPolicy(100L)

  before {
    metaStore.clearAllData().futureValue
    coordActor = system.actorOf(NodeCoordinatorActor.props(memTable, metaStore, reprojector, columnStore,
                                config))
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
  }

  val rows = Seq(
    (Some(0L), Some("2015/03/15T15:00Z"), Some(32015), Some(2015)),
    (Some(1L), Some("2015/03/15T16:00Z"), Some(32015), Some(2015))
  )

  it("should be able to start ingestion, send rows, and get an ack back") {
    val dataset = Dataset("gdelt_cas", "id")
    val columns = GdeltColumns.map(_.copy(dataset = "gdelt_cas"))
    probe.send(coordActor, CreateDataset(dataset, columns))
    probe.expectMsg(DatasetCreated)
    columnStore.clearProjectionData(dataset.projections.head).futureValue should equal (Success)

    probe.send(coordActor, SetupIngestion(dataset.name, GdeltColNames, 0))
    probe.expectMsg(IngestionReady)

    probe.send(coordActor, IngestRows(dataset.name, 0, rows.map(TupleRowReader), 1L))
    probe.expectMsg(Ack(1L))

    // Now, try to flush and check that stuff was written to columnstore...
    // Note that once we receive the Flushed message back, that means flush cycle was completed.
    probe.send(coordActor, Flush(dataset.name, 0))
    probe.expectMsg(Flushed)

    implicit val helper = new LongKeyHelper(10000L)
    val keyRange = KeyRange(dataset.name, Dataset.DefaultPartitionKey, 0L, 30000L)
    whenReady(columnStore.readSegments(columns, keyRange, 0)) { segIter =>
      val readSeg = segIter.toSeq.head.asInstanceOf[RowReaderSegment[Long]]
      readSeg.rowIterator().map(_.getInt(2)).toSeq should equal (Seq(32015, 32015))
    }
  }
}


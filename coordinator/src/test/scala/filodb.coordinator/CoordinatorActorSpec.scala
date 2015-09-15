package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.TestProbe
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core._
import filodb.core.columnstore.{RowReaderSegment, TupleRowReader}
import filodb.core.metadata.{Column, Dataset}
import filodb.core.reprojector.{MapDBMemTable, NumRowsFlushPolicy}
import filodb.cassandra.AllTablesTest

object CoordinatorActorSpec extends ActorSpecConfig

class CoordinatorActorSpec extends ActorTest(CoordinatorActorSpec.getNewSystem)
with CoordinatorSetup with AllTablesTest {
  import akka.testkit._
  import CoordinatorActor._

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
    coordActor = system.actorOf(CoordinatorActor.props(memTable, metaStore, scheduler, columnStore,
                                config.getConfig("coordinator")))
    probe = TestProbe()
  }

  after {
    gracefulStop(coordActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  describe("CoordinatorActor SetupIngestion verification") {
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
      probe.expectMsg(BadSchema)
    }
  }

  val rows = Seq(
    (Some(0L), Some("2015/03/15T15:00Z"), Some(32015), Some(2015)),
    (Some(1L), Some("2015/03/15T16:00Z"), Some(32015), Some(2015))
  )

  it("should be able to start ingestion, send rows, and get an ack back") {
    probe.send(coordActor, CreateDataset(GdeltDataset, GdeltColumns))
    probe.expectMsg(DatasetCreated)

    probe.send(coordActor, SetupIngestion(dsName, GdeltColNames, 0))
    probe.expectMsg(IngestionReady)

    probe.send(coordActor, IngestRows(dsName, 0, rows.map(TupleRowReader), 1L))
    probe.expectMsg(Ack(1L))

    // Now, try to flush and check that stuff was written to columnstore...
    probe.send(coordActor, Flush(dsName, 0))
    probe.expectMsg(SchedulerActor.Flushed)

    whenReady(scheduler.tasks.values.head) { responses =>
      memTable.flushingDatasets should equal (Nil)
      implicit val helper = new LongKeyHelper(10000L)
      val keyRange = KeyRange(dsName, Dataset.DefaultPartitionKey, 0L, 30000L)
      whenReady(columnStore.readSegments(GdeltColumns, keyRange, 0)) { segIter =>
        val readSeg = segIter.toSeq.head.asInstanceOf[RowReaderSegment[Long]]
        readSeg.rowIterator().map(_.getInt(2)).toSeq should equal (Seq(32015, 32015))
      }
    }
  }
}


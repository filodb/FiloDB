package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.TestActorRef
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core._
import filodb.core.columnstore.TupleRowReader
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
    createAllTables()
  }

  before { truncateAllTables() }

  lazy val memTable = new MapDBMemTable(config)
  lazy val flushPolicy = new NumRowsFlushPolicy(100L)
  coordinatorActor

  override def afterAll() {
    gracefulStop(coordinatorActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  describe("CoordinatorActor SetupIngestion verification") {
    it("should return UnknownDataset when dataset missing or no columns defined") {
      createTable(GdeltDataset.copy(name = "noColumns"), Nil)

      coordinatorActor ! SetupIngestion("none", GdeltColNames, 0)
      expectMsg(UnknownDataset)

      coordinatorActor ! SetupIngestion("noColumns", GdeltColNames, 0)
      expectMsg(UndefinedColumns(GdeltColNames))
    }

    it("should return UndefinedColumns if trying to ingest undefined columns") {
      createTable(GdeltDataset, GdeltColumns)

      coordinatorActor ! SetupIngestion(dsName, Seq("monthYear", "last"), 0)
      expectMsg(UndefinedColumns(Seq("last")))
    }

    it("should return BadSchema if dataset definition bazooka") {
      createTable(GdeltDataset.copy(partitionColumn = "foo"), GdeltColumns)
      coordinatorActor ! SetupIngestion(dsName, GdeltColNames, 0)
      expectMsg(BadSchema)
    }
  }

  val rows = Seq(
    (Some(0L), Some("2015/03/15T15:00Z"), Some(32015), Some(2015)),
    (Some(1L), Some("2015/03/15T16:00Z"), Some(32015), Some(2015))
  )

  it("should be able to start ingestion, send rows, and get an ack back") {
    createTable(GdeltDataset, GdeltColumns)

    coordinatorActor ! SetupIngestion(dsName, GdeltColNames, 0)
    expectMsg(IngestionReady)

    coordinatorActor ! IngestRows(dsName, 0, rows.map(TupleRowReader), 1L)
    expectMsg(Ack(1L))

    // Now, try to flush and check that stuff was written to columnstore...
  }
}


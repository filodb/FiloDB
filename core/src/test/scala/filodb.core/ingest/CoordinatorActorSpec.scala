package filodb.core.ingest

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.TestActorRef
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import org.velvia.filo.{ColumnParser, TupleRowIngestSupport}
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.cassandra.AllTablesTest
import filodb.core.metadata.{Column, Dataset, Partition, Shard}
import filodb.core.messages._

object CoordinatorActorSpec {
  val config = ConfigFactory.parseString("""
                                           akka.log-dead-letters = 0
                                         """)
  def getNewSystem = ActorSystem("test", config)
}

class CoordinatorActorSpec extends AllTablesTest(CoordinatorActorSpec.getNewSystem) {
  override def beforeAll() {
    super.beforeAll()
    createAllTables()
  }

  before { truncateAllTables() }

  def withCoordinatorActor(dataset: String, partition: String, columns: Seq[String])(f: ActorRef => Unit) {
    val coordinator = system.actorOf(CoordinatorActor.props(metaActor, datastore))
    coordinator ! CoordinatorActor.StartRowIngestion(dataset, partition, columns, 0, TupleRowIngestSupport)
    try {
      f(coordinator)
    } finally {
      // Stop the actor. This isn't strictly necessary, but prevents extraneous messages from spilling over
      // to the next test.  Also, you cannot create two actors with the same name.
      val stopping = gracefulStop(coordinator, 3 seconds, PoisonPill)
      Await.result(stopping, 4 seconds)
    }
  }

  describe("CoordinatorActor StartRowIngestion verification") {
    it("should return NoDatasetColumns when dataset missing or no columns defined") {
      createTable("noColumns", "first", Nil)

      withCoordinatorActor("none", "first", GdeltColNames) { coordinator =>
        expectMsg(CoordinatorActor.NoDatasetColumns)
      }

      withCoordinatorActor("noColumns", "first", GdeltColNames) { coordinator =>
        expectMsg(CoordinatorActor.NoDatasetColumns)
      }
    }

    it("should return error when dataset present but partition not defined") {
      createTable("gdelt", "1979-1984", GdeltColumns)
      withCoordinatorActor("gdelt", "2001", GdeltColNames) { coordinator =>
        expectMsg(NotFound)
      }
    }

    it("should return UndefinedColumns if trying to ingest undefined columns") {
      createTable("gdelt", "1979-1984", GdeltColumns)
      withCoordinatorActor("gdelt", "1979-1984", Seq("monthYear", "last")) { coordinator =>
        expectMsg(CoordinatorActor.UndefinedColumns(Seq("last")))
      }
    }

    it("should return RowIngestionReady if dataset, partition, columns all validate") {
      createTable("gdelt", "1979-1984", GdeltColumns)
      withCoordinatorActor("gdelt", "1979-1984", GdeltColNames) { coordinator =>
        expectMsgType[CoordinatorActor.RowIngestionReady]
      }
    }
  }

  val rows = Seq(
    (Some(0L), Some("2015/03/15T15:00Z"), Some(32015), Some(2015)),
    (Some(1L), Some("2015/03/15T16:00Z"), Some(32015), Some(2015))
  )

  it("should be able to start ingestion, send rows, and get an ack back") {
    createTable("gdelt", "1979-1984", GdeltColumns)
    withCoordinatorActor("gdelt", "1979-1984", GdeltColNames) { coordinator =>
      val ready = expectMsgType[CoordinatorActor.RowIngestionReady]
      val rowIngester = ready.rowIngestActor
      rows.zipWithIndex.foreach { case (row, i) =>
        rowIngester ! RowIngesterActor.Row(i.toLong, i.toLong, 0, row)
      }
      rowIngester ! RowIngesterActor.Flush

      expectMsg(IngesterActor.Ack("gdelt", "1979-1984", 1L))
    }
  }

  it("should be able to stop ingestion stream") (pending)
}


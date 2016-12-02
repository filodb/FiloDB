package filodb.coordinator

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.{EventFilter, TestProbe}
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import filodb.core._
import filodb.core.store._
import filodb.core.metadata.{Column, DataColumn, Dataset}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.util.Try
import scalax.file.Path

object RowSourceSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class RowSourceSpec extends ActorTest(RowSourceSpec.getNewSystem)
with CoordinatorSetup with ScalaFutures {
  import akka.testkit._
  import DatasetCommands._
  import IngestionCommands._
  import GdeltTestData._

  import sources.CsvSourceActor

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.parseString("""filodb.memtable.write.interval = 500 ms
                                           |filodb.memtable.filo.chunksize = 70
                                           |filodb.memtable.max-rows-per-table = 70""".stripMargin)
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global
  val columnStore = new InMemoryColumnStore(context)
  val metaStore = new InMemoryMetaStore

  metaStore.newDataset(dataset1).futureValue should equal (Success)
  val ref = DatasetRef(dataset1.name)
  schema.foreach { col => metaStore.newColumn(col, ref).futureValue should equal (Success) }
  val coordActor = system.actorOf(NodeCoordinatorActor.props(metaStore, reprojector, columnStore, config))

  val dataset33 = dataset3.copy(name = "gdelt2")
  metaStore.newDataset(dataset33).futureValue should equal (Success)
  val ref2 = DatasetRef(dataset33.name)
  schema.foreach { col => metaStore.newColumn(col, ref2).futureValue should equal (Success) }

  before {
    columnStore.dropDataset(ref).futureValue
    columnStore.dropDataset(ref2).futureValue
    coordActor ! NodeCoordinatorActor.Reset
    stateCache.clear()     // Important!  Clear out cached segment state
  }

  override def afterAll(): Unit ={
    super.afterAll()
    gracefulStop(coordActor, 3.seconds.dilated, PoisonPill).futureValue
    val walDir = config.getString("write-ahead-log.memtable-wal-dir")
    val path = Path.fromString (walDir)
    Try(path.deleteRecursively(continueOnFailure = false))
  }

  it("should fail fast if NodeCoordinatorActor bombs at end of ingestion") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test-errors.csv"))
    val csvActor = system.actorOf(CsvSourceActor.props(reader, ref, 0, coordActor,
                                                       ackTimeout = 4.seconds,
                                                       waitPeriod = 2.seconds))

    csvActor ! RowSource.Start

    // Expect failure message, like soon.  Don't hang!
    expectMsgPF(10.seconds.dilated) {
      case RowSource.IngestionErr(msg, _) =>
    }
  }

  it("should fail fast if NodeCoordinatorActor bombs in middle of ingestion") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test-errors.csv"))
    val csvActor = system.actorOf(CsvSourceActor.props(reader, ref, 0, coordActor,
                                                       maxUnackedBatches = 2,
                                                       rowsToRead = 5,
                                                       ackTimeout = 4.seconds,
                                                       waitPeriod = 2.seconds))

    csvActor ! RowSource.Start

    // Expect failure message, like soon.  Don't hang!
    expectMsgPF(10.seconds.dilated) {
      case RowSource.IngestionErr(msg, _) =>
    }
  }

  it("should ingest all rows and handle memtable flush cycle properly") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))

    // Note: can only send 20 rows at a time before waiting for acks.  Therefore this tests memtable
    // ack on timer and ability for RowSource to handle waiting for acks repeatedly
    val csvActor = system.actorOf(CsvSourceActor.props(reader, ref2, 0, coordActor,
                                                       maxUnackedBatches = 2,
                                                       rowsToRead = 10))

    csvActor ! RowSource.Start
    expectMsg(10.seconds.dilated, RowSource.AllDone)

    coordActor ! GetIngestionStats(ref2, 0)
    // 20 rows per memtable write = 80 rows flushed, 19 in memtable, 99 total
    expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 19, -1, 99L))
  }

  it("should ingest all rows and handle memtable full properly") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test-200.csv"))

    // Note: this tries to send 80 rows, after 70 memtable will be flushed and 10 rows go to new memtable
    // then it will send another 80 rows, this time non flushing memtable will become full, hopefully
    // while flush still happening, and cause Nack to be returned.
    val csvActor = system.actorOf(CsvSourceActor.props(reader, ref2, 0, coordActor,
                                                       maxUnackedBatches = 8,
                                                       rowsToRead = 10))

    csvActor ! RowSource.Start
    expectMsg(10.seconds.dilated, RowSource.AllDone)

    coordActor ! GetIngestionStats(ref2, 0)
    // 70 rows per memtable write = 140 rows flushed, 59 in memtable, 199 total
    expectMsg(DatasetCoordinatorActor.Stats(2, 2, 0, 59, -1, 199L))
  }
}
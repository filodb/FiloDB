package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._

import filodb.core._
import filodb.core.store._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span, Seconds}

object RowSourceSpec extends ActorSpecConfig

// This is really an end to end ingestion test, it's what a client talking to a FiloDB node would do
class RowSourceSpec extends ActorTest(RowSourceSpec.getNewSystem)
with CoordinatorSetup with ScalaFutures {
  import akka.testkit._
  import DatasetCommands._
  import IngestionCommands._
  import GdeltTestData._

  import sources.CsvSourceActor
  import IngestionCommands.{IngestionReady, SetupIngestion}

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

  val ref = projection1.datasetRef
  val columnNames = schema.map(_.name)
  metaStore.newDataset(dataset1).futureValue should equal (Success)
  schema.foreach { col => metaStore.newColumn(col, ref).futureValue should equal (Success) }
  val coordActor = system.actorOf(NodeCoordinatorActor.props(metaStore, reprojector, columnStore, config))
  val clusterActor = system.actorOf(NodeClusterActor.singleNodeProps(coordActor))

  val dataset33 = dataset3.withName("gdelt2")
  metaStore.newDataset(dataset33).futureValue should equal (Success)
  val proj2 = RichProjection(dataset33, schema)
  val ref2 = proj2.datasetRef
  schema.foreach { col => metaStore.newColumn(col, ref2).futureValue should equal (Success) }

  before {
    columnStore.dropDataset(ref).futureValue
    columnStore.dropDataset(ref2).futureValue
    coordActor ! NodeCoordinatorActor.Reset
    stateCache.clear()     // Important!  Clear out cached segment state
  }

  override def afterAll(): Unit = {
    gracefulStop(clusterActor, 3.seconds.dilated, PoisonPill).futureValue
    gracefulStop(coordActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  it("should fail fast if NodeCoordinatorActor bombs at end of ingestion") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test-errors.csv"))
    val csvActor = system.actorOf(CsvSourceActor.props(reader, projection1, 0, clusterActor,
                                                       ackTimeout = 4.seconds,
                                                       waitPeriod = 2.seconds))

    coordActor ! SetupIngestion(ref, columnNames, 0)
    expectMsg(IngestionReady)
    csvActor ! RowSource.Start

    // Expect failure message, like soon.  Don't hang!
    expectMsgPF(10.seconds.dilated) {
      case RowSource.IngestionErr(msg, _) =>
    }
  }

  it("should fail fast if NodeCoordinatorActor bombs in middle of ingestion") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test-errors.csv"))
    val csvActor = system.actorOf(CsvSourceActor.props(reader, projection1, 0, clusterActor,
                                                       maxUnackedBatches = 2,
                                                       rowsToRead = 5,
                                                       ackTimeout = 4.seconds,
                                                       waitPeriod = 2.seconds))
    coordActor ! SetupIngestion(ref, columnNames, 0)
    expectMsg(IngestionReady)
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
    val csvActor = system.actorOf(CsvSourceActor.props(reader, proj2, 0, clusterActor,
                                                       maxUnackedBatches = 2,
                                                       rowsToRead = 10))

    coordActor ! SetupIngestion(ref2, columnNames, 0)
    expectMsg(IngestionReady)
    csvActor ! RowSource.Start
    expectMsg(10.seconds.dilated, RowSource.AllDone)

    coordActor ! GetIngestionStats(ref2, 0)
    // 20 rows per memtable write = 80 rows flushed, 19 in memtable, 99 total
    expectMsg(DatasetCoordinatorActor.Stats(1, 1, 0, 19, -1, 99L))
  }

  // NOTE: This tests the replay logic because when memtable is full, it sends back a Nack and
  // then replay has to be triggered
  it("should ingest all rows and handle memtable full properly") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test-200.csv"))

    // Note: this tries to send 80 rows, after 70 memtable will be flushed and 10 rows go to new memtable
    // then it will send another 80 rows, this time non flushing memtable will become full, hopefully
    // while flush still happening, and cause Nack to be returned.
    val csvActor = system.actorOf(CsvSourceActor.props(reader, proj2, 0, clusterActor,
                                                       maxUnackedBatches = 8,
                                                       rowsToRead = 10))

    coordActor ! SetupIngestion(ref2, columnNames, 0)
    expectMsg(IngestionReady)
    csvActor ! RowSource.Start
    expectMsg(10.seconds.dilated, RowSource.AllDone)

    coordActor ! GetIngestionStats(ref2, 0)
    // 70 rows per memtable write = 140 rows flushed, 59 in memtable, 199 total
    expectMsg(DatasetCoordinatorActor.Stats(2, 2, 0, 59, -1, 199L))
  }
}
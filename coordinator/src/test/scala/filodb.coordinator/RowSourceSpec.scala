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

  val config = ConfigFactory.parseString("filodb.memtable.write.interval = 500 ms")
                            .withFallback(ConfigFactory.load("application_test.conf"))
                            .getConfig("filodb")

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global
  val columnStore = new InMemoryColumnStore(context)
  val metaStore = new InMemoryMetaStore

  metaStore.newDataset(dataset1).futureValue should equal (Success)
  val ref = DatasetRef(dataset1.name)
  schema.foreach { col => metaStore.newColumn(col, ref).futureValue should equal (Success) }
  val coordActor = system.actorOf(NodeCoordinatorActor.props(metaStore, reprojector,
                                                      columnStore, config, cluster.selfAddress))

  before {
    columnStore.dropDataset(DatasetRef(dataset1.name)).futureValue
    coordActor ! NodeCoordinatorActor.Reset
  }

  override def afterAll(): Unit = {
    gracefulStop(coordActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  it("should fail fast if NodeCoordinatorActor bombs at end of ingestion") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))
    val csvActor = system.actorOf(CsvSourceActor.props(reader, ref, 0, coordActor))

    csvActor ! RowSource.Start

    // Expect failure message, like soon.  Don't hang!
    expectMsgPF(10.seconds.dilated) {
      case RowSource.IngestionErr(msg) =>
    }
  }

  it("should fail fast if NodeCoordinatorActor bombs in middle of ingestion") {
    val reader = new java.io.InputStreamReader(getClass.getResourceAsStream("/GDELT-sample-test.csv"))
    val csvActor = system.actorOf(CsvSourceActor.props(reader, ref, 0, coordActor,
                                                       maxUnackedBatches = 2,
                                                       rowsToRead = 5))

    csvActor ! RowSource.Start

    // Expect failure message, like soon.  Don't hang!
    expectMsgPF(10.seconds.dilated) {
      case RowSource.IngestionErr(msg) =>
    }
  }
}
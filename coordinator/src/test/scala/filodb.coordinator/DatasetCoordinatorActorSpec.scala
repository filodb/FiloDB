package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.TestProbe
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import org.velvia.filo.{RowReader, TupleRowReader}
import scala.concurrent.Future
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.{InMemoryColumnStore, SegmentSpec}
import filodb.core.reprojector.{DefaultReprojector, MemTable, MapDBMemTable, Reprojector}

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Span, Seconds}

object DatasetCoordinatorActorSpec extends ActorSpecConfig

class DatasetCoordinatorActorSpec extends ActorTest(DatasetCoordinatorActorSpec.getNewSystem)
with ScalaFutures {
  import akka.testkit._
  import SegmentSpec._
  import DatasetCoordinatorActor._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  import scala.concurrent.ExecutionContext.Implicits.global

  val config = ConfigFactory.parseString("memtable.flush-trigger-rows = 100")
                 .withFallback(ConfigFactory.load("application_test.conf"))
  val keyRange = KeyRange("dataset", Dataset.DefaultPartitionKey, 0L, 10000L)
  val myDataset = dataset.copy(partitionColumn = "league")
  // TODO: remove once we get no longer need flushpolicy and scheduler, and move memtable inside DSCoordActor
  val memTable = new MapDBMemTable(config)
  val columnStore = new InMemoryColumnStore

  var dsActor: ActorRef = _
  var probe: TestProbe = _
  var reprojections: Seq[(Types.TableName, Int)] = Nil

  override def beforeAll() {
    super.beforeAll()
    columnStore.initializeProjection(myDataset.projections.head).futureValue
  }

  before {
    memTable.clearAllData()
    columnStore.clearProjectionData(myDataset.projections.head).futureValue
    reprojections = Nil
    dsActor = system.actorOf(DatasetCoordinatorActor.props(
                                  myDataset, 0, columnStore, testReprojector, config, memTable))
    probe = TestProbe()
  }

  after {
    gracefulStop(dsActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }
  }

  private def ingestRows(numRows: Int) {
    dsActor ! Setup(probe.ref, schemaWithPartCol)
    probe.expectMsg(NodeCoordinatorActor.IngestionReady)

    dsActor ! NewRows(probe.ref, namesWithPartCol.take(numRows).map(TupleRowReader), 0L)
    probe.expectMsg(NodeCoordinatorActor.Ack(0L))
  }

  import RowReader._
  val testReprojector = new Reprojector {
    def reproject[K: TypedFieldExtractor](memTable: MemTable, setup: MemTable.IngestionSetup, version: Int):
        Future[Seq[String]] = {
      reprojections = reprojections :+ (setup.dataset.name -> version)
      Future.successful(Seq("Success"))
    }
  }

  it("should respond to GetStats with no flushes and no rows") {
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, -1L, -1L))
    reprojections should equal (Nil)
  }

  it("should not flush if datasets not reached limit yet") {
    ingestRows(99)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 99L, 0L))
    reprojections should equal (Nil)
  }

  it("should automatically flush after ingesting enough rows") {
    ingestRows(100)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(1, 0, 0, 0L, 100L))   // remember our reprojector doesn't delete rows
    reprojections should equal (Seq(("dataset", 0)))
  }

  it("StartFlush should initiate flush even if # rows not reached trigger yet") {
    ingestRows(99)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 99L, 0L))
    reprojections should equal (Nil)

    dsActor ! StartFlush(Some(probe.ref))
    probe.expectMsg(NodeCoordinatorActor.Flushed)
    probe.send(dsActor, GetStats)
    probe.expectMsgPF(3.seconds.dilated) {
      case Stats(1, 1, 0, 0L, _) =>
    }
    reprojections should equal (Seq(("dataset", 0)))
  }
}

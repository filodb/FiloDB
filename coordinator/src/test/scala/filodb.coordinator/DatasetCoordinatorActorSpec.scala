package filodb.coordinator

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.TestProbe
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import org.velvia.filo.{RowReader, TupleRowReader}
import scala.concurrent.Future
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.columnstore.{InMemoryColumnStore, SegmentSpec}
import filodb.core.reprojector.{DefaultReprojector, MemTable, Reprojector}

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

  import system.dispatcher

  // Need to force smaller flush interval to ensure acks get back in time
  val config = ConfigFactory.parseString(
                 """memtable.flush-trigger-rows = 100
                    memtable.max-rows-per-table = 200
                    memtable.flush.interval = 500 ms""")
                 .withFallback(ConfigFactory.load("application_test.conf"))
  val keyRange = KeyRange("dataset", Dataset.DefaultPartitionKey, 0L, 10000L)
  val myDataset = dataset.copy(partitionColumn = "league")
  val schemaWithPartCol = schema ++ Seq(
    Column("league", "dataset", 0, Column.ColumnType.StringColumn)
  )
  val myProjection = RichProjection(myDataset, schemaWithPartCol)
  val columnStore = new InMemoryColumnStore

  var dsActor: ActorRef = _
  var probe: TestProbe = _
  var reprojections: Seq[(Types.TableName, Int)] = Nil

  override def beforeAll() {
    super.beforeAll()
    columnStore.initializeProjection(myDataset.projections.head).futureValue
  }

  before {
    columnStore.clearProjectionData(myDataset.projections.head).futureValue
    reprojections = Nil
    dsActor = system.actorOf(DatasetCoordinatorActor.props(
                                  myProjection, 0, columnStore, testReprojector, config))
    probe = TestProbe()
  }

  after {
    gracefulStop(dsActor, 3.seconds.dilated, PoisonPill).futureValue
  }

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, Some(partNum.toString)) }
  }


  private def ingestRows(numRows: Int) {
    dsActor ! NewRows(probe.ref, namesWithPartCol.take(numRows).map(TupleRowReader), 0L)
    probe.expectMsg(NodeCoordinatorActor.Ack(0L))
  }

  val testReprojector = new Reprojector {
    import filodb.core.columnstore.Segment

    def reproject[K](memTable: MemTable[K], version: Int): Future[Seq[String]] = {
      reprojections = reprojections :+ (memTable.projection.dataset.name -> version)
      Future.successful(Seq("Success"))

    }

    def toSegments[K](memTable: MemTable[K]): Iterator[Segment[K]] = ???
  }

  it("should respond to GetStats with no flushes and no rows") {
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 0, -1))
    reprojections should equal (Nil)
  }

  it("should not flush if datasets not reached limit yet") {
    ingestRows(99)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 99, -1))
    reprojections should equal (Nil)
  }

  it("should automatically flush after ingesting enough rows") {
    ingestRows(100)
    // Ingest more rows.  These should be ingested into the active table AFTER flush is initiated.
    Thread sleep 250
    ingestRows(20)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(1, 1, 0, 20, -1))
    reprojections should equal (Seq(("dataset", 0)))
  }

  it("should not send Ack if over maximum number of rows") {
    // First one will go through, but make memTable full
    dsActor ! NewRows(probe.ref, namesWithPartCol.take(205).map(TupleRowReader), 0L)
    // Second one will not go through or get an ack, already over limit
    // (Hopefully this gets sent before the table is flushed)
    dsActor ! NewRows(probe.ref, namesWithPartCol.drop(205).take(20).map(TupleRowReader), 1L)

    probe.expectMsg(NodeCoordinatorActor.Ack(0L))
    probe.expectNoMsg
  }

  it("StartFlush should initiate flush even if # rows not reached trigger yet") {
    ingestRows(99)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 99, -1))
    reprojections should equal (Nil)

    dsActor ! StartFlush(Some(probe.ref))
    probe.expectMsg(NodeCoordinatorActor.Flushed)
    probe.send(dsActor, GetStats)
    probe.expectMsgPF(3.seconds.dilated) {
      case Stats(1, 1, 0, 0, _) =>
    }
    reprojections should equal (Seq(("dataset", 0)))
  }
}

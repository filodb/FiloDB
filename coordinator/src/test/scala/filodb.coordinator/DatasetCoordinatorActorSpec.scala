package filodb.coordinator

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.testkit.TestProbe
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import org.velvia.filo.{RowReader, TupleRowReader}

import scala.concurrent.Future
import scala.concurrent.duration._
import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.core.store.{ChunkSetSegment, InMemoryColumnStore, SegmentInfo}
import filodb.core.reprojector.{DefaultReprojector, MemTable, Reprojector}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.util.Try
import scalax.file.Path

object DatasetCoordinatorActorSpec extends ActorSpecConfig

class DatasetCoordinatorActorSpec extends ActorTest(DatasetCoordinatorActorSpec.getNewSystem)
with ScalaFutures {
  import akka.testkit._
  import NamesTestData._
  import DatasetCoordinatorActor._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  import system.dispatcher

  // Need to force smaller flush interval to ensure acks get back in time
  val config = ConfigFactory.parseString(
                 """filodb.memtable.flush-trigger-rows = 100
                    filodb.memtable.max-rows-per-table = 100
                    filodb.memtable.noactivity.flush.interval = 2 s
                    filodb.memtable.write.interval = 300 ms""")
                 .withFallback(ConfigFactory.load("application_test.conf"))
                 .getConfig("filodb")

  val myDataset = largeDataset
  val myProjection = RichProjection(myDataset, schemaWithPartCol)
  val columnStore = new InMemoryColumnStore(dispatcher)

  var dsActor: ActorRef = _
  var probe: TestProbe = _
  var reprojections: Seq[(DatasetRef, Int)] = Nil

  override def beforeAll(): Unit = {
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
    val walDir = config.getString("write-ahead-log.memtable-wal-dir")
    val path = Path.fromString (walDir)
    Try(path.deleteRecursively(continueOnFailure = false))
  }

  val namesWithPartCol = (0 until 50).flatMap { partNum =>
    names.map { t => (t._1, t._2, t._3, t._4, Some(partNum.toString)) }
  }


  private def ingestRows(numRows: Int): Unit = {
    dsActor ! NewRows(probe.ref, namesWithPartCol.take(numRows).map(TupleRowReader), 0L)
    probe.expectMsg(IngestionCommands.Ack(0L))
  }

  val dummySegInfo = SegmentInfo(myProjection.partKey("Success"), 0)

  val testReprojector = new Reprojector {
    import filodb.core.store.Segment

    def reproject(memTable: MemTable, version: Int): Future[Seq[SegmentInfo[_, _]]] = {
      reprojections = reprojections :+ (memTable.projection.datasetRef -> version)
      Future.successful(Seq(dummySegInfo))
    }

    def toSegments(memTable: MemTable,
                   partitions: Seq[Types.PartitionKey],
                   version: Int): Seq[ChunkSetSegment] = ???
  }

  it("should respond to GetStats with no flushes and no rows") {
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 0, -1, 0L))
    reprojections should equal (Nil)
  }

  it("should not flush if datasets not reached limit yet") {
    ingestRows(99)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 99, -1, 99L))
    reprojections should equal (Nil)
  }

  it("should automatically flush after ingesting enough rows") {
    ingestRows(100)
    // Ingest more rows.  These should be ingested into the active table AFTER flush is initiated.
    Thread sleep 500
    ingestRows(20)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(1, 1, 0, 20, -1, 120L))
    reprojections should equal (Seq((DatasetRef("dataset"), 0)))
  }

  it("should send back Nack if over maximum number of rows or Nack sent before with no CheckCanIngest") {
    // First one will go through, but make memTable full.  Flush will happen, new memtable available
    dsActor ! NewRows(probe.ref, namesWithPartCol.take(205).map(TupleRowReader), 0L)
    // Second one will go through but make memtable full again.  Third one will be denied becuase
    // hopefully flushing still happening and active memtable is full again
    dsActor ! NewRows(probe.ref, namesWithPartCol.take(205).map(TupleRowReader), 1L)
    dsActor ! NewRows(probe.ref, namesWithPartCol.drop(205).take(20).map(TupleRowReader), 2L)

    probe.expectMsg(IngestionCommands.Ack(0L))
    probe.expectMsg(IngestionCommands.Ack(1L))
    probe.expectMsg(IngestionCommands.Nack(2L))

    // Now, a flush will be initiated, and DSCoordActor should send us CanIngestAgain
    probe.expectMsg(IngestionCommands.ResumeIngest)

    // Check that trying to ingest without sending CheckCanIngest results in a Nack
    dsActor ! NewRows(probe.ref, namesWithPartCol.take(50).map(TupleRowReader), 4L)
    probe.expectMsg(IngestionCommands.Nack(4L))

    // Send CheckCanIngest, after that will be clear to ingest again
    probe.send(dsActor, CanIngest)
    probe.expectMsg(IngestionCommands.CanIngest(true))
    dsActor ! NewRows(probe.ref, namesWithPartCol.take(50).map(TupleRowReader), 5L)
    probe.expectMsg(IngestionCommands.Ack(5L))
  }

  it("StartFlush should initiate flush even if # rows not reached trigger yet") {
    ingestRows(99)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 99, -1, 99L))
    reprojections should equal (Nil)

    dsActor ! StartFlush(Some(probe.ref))
    probe.expectMsg(IngestionCommands.Flushed)
    probe.send(dsActor, GetStats)
    probe.expectMsgPF(3.seconds.dilated) {
      case Stats(1, 1, 0, 0, _, _) =>
    }
    reprojections should equal (Seq((DatasetRef("dataset"), 0)))
  }

  // Sleeps such that no more than totalMs has elapsed from startMs.  If current time is already
  // past (startMs + totalMs), don't sleep at all.
  def sleepRemaining(startMs: Long, totalMs: Int): Unit = {
    val remaining = (startMs + totalMs) - System.currentTimeMillis
    if (remaining > 0) Thread sleep remaining
  }

  it("StartFlush should initiate flush when there is no write activity after few seconds") {
    ingestRows(50)

    val start1 = System.currentTimeMillis
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 50, -1, 50L))
    sleepRemaining(start1, 1000)

    // This Call will cancel the scheduled memtable flush task.
    // After 1000 more ms, there should still be no flush (assuming the stats check happens
    // within 1 second)
    ingestRows(40)
    val start2 = System.currentTimeMillis
    Thread sleep 1000
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(0, 0, 0, 90, -1, 90L))

    // However, 2.5 secs after the ingestion of new data, the flush should have kicked off
    sleepRemaining(start2, 2500)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(1, 1, 0, 0, -1, 90L))

  }

  it("should automatically delete memtable wal files once flush is complete successfully") {
    ingestRows(100)
    // Ingest more rows.  These should be ingested into the active table AFTER flush is initiated.
    Thread sleep 500
    ingestRows(20)
    probe.send(dsActor, GetStats)
    probe.expectMsg(Stats(1, 1, 0, 20, -1, 120L))
    reprojections should equal (Seq((DatasetRef("dataset"), 0)))

  }
}

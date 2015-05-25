package filodb.core.ingest

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.TestProbe
import org.velvia.filo.{ColumnParser, TupleRowIngestSupport}
import scala.concurrent.duration._

import filodb.core.ActorSpecConfig
import filodb.core.cassandra.ActorTest
import filodb.core.metadata.{Column, Partition}

object RowIngesterActorSpec extends ActorSpecConfig

class RowIngesterActorSpec extends ActorTest(RowIngesterActorSpec.getNewSystem) {
  import akka.testkit._

  val testPartition = Partition("dataset1", "part0", chunkSize = 10)
  val schema = Seq(Column("first", "dataset1", 0, Column.ColumnType.StringColumn),
                   Column("last", "dataset1", 0, Column.ColumnType.StringColumn),
                   Column("age", "dataset1", 0, Column.ColumnType.IntColumn))

  val support = TupleRowIngestSupport

  val names = Seq((Some("Khalil"), Some("Mack"), Some(24)),
                  (Some("Ndamukong"), Some("Suh"), Some(28)),
                  (Some("Rodney"), Some("Hudson"), Some(25)),
                  (Some("Jerry"), Some("Rice"), None),
                  (Some("Peyton"), Some("Manning"), None),
                  (Some("Terrance"), Some("Knighton"), Some(28)))

  import RowIngesterActor.Row
  import ColumnParser._

  it("should flush when chunk gets full and one more line added") {
    val probe = TestProbe()
    lazy val rowIngestActor = system.actorOf(
                                RowIngesterActor.props(probe.ref, schema, testPartition, support))
    (0 until 5).foreach { i => rowIngestActor ! Row(i, i, 0, names(i)) }
    (0 until 5).foreach { i => rowIngestActor ! Row(i + 5, i + 5, 0, names(i)) }

    // At this point nothing would be sent yet, but hard to test for this unless you wait 3 secs :(
    rowIngestActor ! Row(10L, 10L, 0, names.last)

    val chunk = probe.expectMsgClass(classOf[IngesterActor.ChunkedColumns])
    chunk.version should equal (0)
    chunk.rowIdRange should equal ((0L, 9L))
    chunk.lastSequenceNo should equal (9L)
    chunk.columnsBytes.keys should equal (Set("first", "last", "age"))

    val ageBinSeq = ColumnParser.parse[Int](chunk.columnsBytes("age"))
    ageBinSeq should have length (10)
    ageBinSeq(0) should equal (24)
    ageBinSeq.toList should equal (List(24, 28, 25, 24, 28, 25))

    probe.expectNoMsg(1.second.dilated)
  }

  it("should flush when changing versions, or rowId goes backwards") {
    val probe = TestProbe()
    lazy val rowIngestActor = system.actorOf(
                                RowIngesterActor.props(probe.ref, schema, testPartition, support))

    // Ingest 3 rows at version 0
    (0 until 3).foreach { i => rowIngestActor ! Row(i, i, 0, names(i)) }

    // Ingest a row at version 1, higher rowId, should flush prev chunk
    rowIngestActor ! Row(3L, 3L, 1, names(3))
    val chunk = probe.expectMsgClass(classOf[IngesterActor.ChunkedColumns])
    chunk.version should equal (0)
    chunk.rowIdRange should equal ((0L, 2L))
    chunk.lastSequenceNo should equal (2L)

    // Ingest 3 rows again at version 0, should flush 1 row at higher version
    (0 until 3).foreach { i => rowIngestActor ! Row(i, i, 0, names(i)) }
    val chunk2 = probe.expectMsgClass(classOf[IngesterActor.ChunkedColumns])
    chunk2.version should equal (1)
    // TODO: if we start writing non-aligned chunks then change this
    chunk2.rowIdRange should equal ((0L, 3L))
    chunk2.lastSequenceNo should equal (3L)
  }

  it("should flush when Flush command issued, or actor is told to shut down") {
    val probe = TestProbe()
    lazy val rowIngestActor = system.actorOf(
                                RowIngesterActor.props(probe.ref, schema, testPartition, support))

    // Ingest 3 rows at version 0, flush, check
    (0 until 3).foreach { i => rowIngestActor ! Row(i, i, 0, names(i)) }
    rowIngestActor ! RowIngesterActor.Flush
    val chunk = probe.expectMsgClass(classOf[IngesterActor.ChunkedColumns])
    chunk.version should equal (0)
    chunk.rowIdRange should equal ((0L, 2L))
    chunk.lastSequenceNo should equal (2L)

    // Ingest 2 more rows, shut down actor, check for chunk
    (3 to 4).foreach { i => rowIngestActor ! Row(i, i, 0, names(i)) }
    rowIngestActor ! PoisonPill
    val chunk2 = probe.expectMsgClass(classOf[IngesterActor.ChunkedColumns])
    chunk2.version should equal (0)
    // TODO: if we start writing non-aligned chunks then change this
    chunk2.rowIdRange should equal ((0L, 4L))
    chunk2.lastSequenceNo should equal (4L)
  }
}
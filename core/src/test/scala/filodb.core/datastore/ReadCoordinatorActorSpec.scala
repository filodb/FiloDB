package filodb.core.datastore

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import org.velvia.filo.{ColumnParser, TupleRowIngestSupport}
import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.scalalogging.slf4j.StrictLogging

import filodb.core.cassandra.{AllTablesTest, DataTable}
import filodb.core.metadata.{Column, Dataset, Partition, Shard}
import filodb.core.messages._

object ReadCoordinatorActorSpec {
  val config = ConfigFactory.parseString("""
                                           akka.log-dead-letters = 0
                                           akka.loggers = ["akka.testkit.TestEventListener"]
                                         """)
  def getNewSystem = ActorSystem("test", config)
}

class ReadCoordinatorActorSpec extends AllTablesTest(ReadCoordinatorActorSpec.getNewSystem) with StrictLogging {
  import ReadCoordinatorActor._
  import akka.testkit._

  override def beforeAll() {
    super.beforeAll()
    createAllTables()
  }

  before { truncateAllTables() }

  def withCoordinatorActor(partition: Partition, version: Int, columns: Seq[String])(f: ActorRef => Unit) {
    val coordinator = system.actorOf(ReadCoordinatorActor.props(datastore, partition, version, columns))
    try {
      f(coordinator)
    } finally {
      // Stop the actor. This isn't strictly necessary, but prevents extraneous messages from spilling over
      // to the next test.  Also, you cannot create two actors with the same name.
      val stopping = gracefulStop(coordinator, 3.seconds.dilated, PoisonPill)
      Await.result(stopping, 4.seconds.dilated)
    }
  }

  val colABytes = Seq("A-1", "A-2", "A-3").map(_.getBytes).map(ByteBuffer.wrap(_))
  val colBBytes = Seq("B-1", "B-2", "B-3").map(_.getBytes).map(ByteBuffer.wrap(_))

  // Creates two shards: one at 0L, another at 200L, first shard has two chunks
  private def writeDataChunks(): Partition = {
    logger.info("About to writeDataChunks...")
    val (partObj, cols) = createTable("gdelt", "first", GdeltColumns take 2)
    val partition = partObj.copy(shardVersions = Map(0L -> (0 -> 1), 200L -> (0 -> 1)),
                                 chunkSize = 100)
    val shard1 = Shard(partition, 0, 0L)
    whenReady(datastore.insertOneChunk(shard1, 0L, 99L, Map("id" -> colABytes(0),
                                                            "sqlDate" -> colBBytes(0)))) { ack =>
      ack should equal (Datastore.Ack(99L))
    }
    whenReady(datastore.insertOneChunk(shard1, 100L, 199L, Map("id" -> colABytes(1),
                             "sqlDate" -> colBBytes(1)))) { ack =>
      ack should equal (Datastore.Ack(199L))
    }

    val shard2 = Shard(partition, 0, 200L)
    whenReady(datastore.insertOneChunk(shard2, 200L, 299L, Map("id" -> colABytes(2),
                             "sqlDate" -> colBBytes(2)))) { ack =>
      ack should equal (Datastore.Ack(299L))
    }

    partition
  }

  describe("error conditions") {
    it("GetNextChunk on empty partition returns InvalidPartition") {
      val fakePartition = Partition("unknown", "unknown")
      withCoordinatorActor(fakePartition, 0, GdeltColNames) { coord =>
        coord ! GetNextChunk
        expectMsg(InvalidPartitionVersion)
      }
    }

    it("returns an error on unknown version") {
      val (partObj, cols) = createTable("gdelt", "first", GdeltColumns take 2)
      val partition = partObj.copy(shardVersions = Map(0L -> (3 -> 5)))
      withCoordinatorActor(partition, 1, GdeltColNames take 2) { coord =>
        coord ! GetNextChunk
        expectMsg(InvalidPartitionVersion)
      }
    }

    it("times out on unknown columns") {
      val partition = writeDataChunks()
      withCoordinatorActor(partition, 0, Seq("id", "bar")) { coord =>
        coord ! GetNextChunk
        expectNoMsg
      }
    }
  }

  describe("normal reads") {
    it("can read all chunks of multiple shards, 2 columns") {
      val partition = writeDataChunks()
      withCoordinatorActor(partition, 0, GdeltColNames take 2) { coord =>
        coord ! GetNextChunk
        val chunks1 = expectMsgClass(classOf[RowChunk])
        chunks1.startRowId should equal (0L)
        chunks1.chunks should equal (Array(colABytes(0), colBBytes(0)))

        coord ! GetNextChunk
        val chunks2 = expectMsgClass(classOf[RowChunk])
        chunks2.startRowId should equal (100L)
        chunks2.chunks should equal (Array(colABytes(1), colBBytes(1)))

        coord ! GetNextChunk
        val chunks3 = expectMsgClass(classOf[RowChunk])
        chunks3.startRowId should equal (200L)
        chunks3.chunks should equal (Array(colABytes(2), colBBytes(2)))
      }
    }

    it("can read from 1 column, multiple shards") {
      val partition = writeDataChunks()
      withCoordinatorActor(partition, 0, GdeltColNames take 1) { coord =>
        coord ! GetNextChunk
        val chunks1 = expectMsgClass(classOf[RowChunk])
        chunks1.startRowId should equal (0L)
        chunks1.chunks should equal (Array(colABytes(0)))

        coord ! GetNextChunk
        val chunks2 = expectMsgClass(classOf[RowChunk])
        chunks2.startRowId should equal (100L)
        chunks2.chunks should equal (Array(colABytes(1)))

        coord ! GetNextChunk
        val chunks3 = expectMsgClass(classOf[RowChunk])
        chunks3.startRowId should equal (200L)
        chunks3.chunks should equal (Array(colABytes(2)))
      }
    }
  }
}

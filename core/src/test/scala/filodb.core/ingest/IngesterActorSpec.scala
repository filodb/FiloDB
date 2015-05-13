package filodb.core.ingest

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import akka.testkit.TestActorRef
import akka.pattern.gracefulStop
import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.cassandra.AllTablesTest
import filodb.core.datastore.Datastore
import filodb.core.metadata.{Column, Dataset, Partition, Shard}
import filodb.core.messages._

object IngesterActorSpec {
  val config = ConfigFactory.parseString("""
                                           akka.log-dead-letters = 0
                                         """)
  def getNewSystem = ActorSystem("test", config)
}

class IngesterActorSpec extends AllTablesTest(IngesterActorSpec.getNewSystem) {
  import akka.testkit._

  override def beforeAll() {
    super.beforeAll()
    createAllTables()
  }

  before { truncateAllTables() }

  def withIngesterActor(dataset: String, partition: String, columns: Seq[(String, Column.ColumnType)])
                       (f: ActorRef => Unit) {
    val (partObj, columnSeq) = createTable(dataset, partition, columns)
    val ingester = system.actorOf(IngesterActor.props(partObj, columnSeq, datastore, testActor))
    try {
      f(ingester)
    } finally {
      // Stop the actor. This isn't strictly necessary, but prevents extraneous messages from spilling over
      // to the next test.  Also, you cannot create two actors with the same name.
      val stopping = gracefulStop(ingester, 3.seconds.dilated, PoisonPill)
      Await.result(stopping, 4.seconds.dilated)
    }
  }

  val dummyBytes = ByteBuffer.wrap(Array[Byte](0, 1, 2, 3, 4, 5))
  val columnsBytes = Map("id" -> dummyBytes,
                         "sqlDate" -> dummyBytes)
  val columnsToWrite = GdeltColumns.take(2)
  val chunkCmd = IngesterActor.ChunkedColumns(0, 0L -> 5L, 5L, columnsBytes)

  describe("IngesterActor ingestion") {
    it("should return Acks and update partition shards when ingesting column chunks") {
      withIngesterActor("gdelt", "1979-1984", columnsToWrite) { ingester =>
        ingester ! chunkCmd
        expectMsg(IngesterActor.Ack("gdelt", "1979-1984", 5L))

        whenReady(datastore.getPartition("gdelt", "1979-1984")) {
          case Datastore.ThePartition(p) =>
            p.shardVersions.size should equal (1)
        }
      }
    }

    it("should return ShardingError if invalid version or rowId") {
      withIngesterActor("gdelt", "1979-1984", columnsToWrite) { ingester =>
        ingester ! chunkCmd.copy(version = -1)
        expectMsg(IngesterActor.ShardingError("gdelt", "1979-1984", 5L))
      }
    }
  }
}


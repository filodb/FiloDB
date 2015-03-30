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
    val coordinator = system.actorOf(CoordinatorActor.props(metaActor, writerActor))
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

      withCoordinatorActor("none", "first", GdeltColNames) { ingester =>
        expectMsg(CoordinatorActor.NoDatasetColumns)
      }

      withCoordinatorActor("noColumns", "first", GdeltColNames) { ingester =>
        expectMsg(CoordinatorActor.NoDatasetColumns)
      }
    }

    it("should return error when dataset present but partition not defined") {
      createTable("gdelt", "1979-1984", GdeltColumns)
      withCoordinatorActor("gdelt", "2001", GdeltColNames) { ingester =>
        expectMsg(NotFound)
      }
    }

    it("should return UndefinedColumns if trying to ingest undefined columns") {
      createTable("gdelt", "1979-1984", GdeltColumns)
      withCoordinatorActor("gdelt", "1979-1984", Seq("monthYear", "last")) { ingester =>
        expectMsg(CoordinatorActor.UndefinedColumns(Seq("last")))
      }
    }

    it("should return RowIngestionReady if dataset, partition, columns all validate") {
      createTable("gdelt", "1979-1984", GdeltColumns)
      withCoordinatorActor("gdelt", "1979-1984", GdeltColNames) { ingester =>
        expectMsgType[CoordinatorActor.RowIngestionReady]
      }
    }
  }

  // val dummyBytes = ByteBuffer.wrap(Array[Byte](0, 1, 2, 3, 4, 5))
  // val columnsBytes = Map("id" -> dummyBytes,
  //                        "sqlDate" -> dummyBytes)
  // val columnsToWrite = columnsBytes.keys.toSeq
  // val chunkCmd = CoordinatorActor.ChunkedColumns(0, 0L -> 5L, 5L, columnsBytes)

  // describe("IngesterActor ingestion") {
  //   it("should return Acks and update partition shards when ingesting column chunks") {
  //     createTable("gdelt", "1979-1984", GdeltColumns)
  //     withIngesterActor("gdelt", "1979-1984", columnsToWrite) { ingester =>
  //       expectMsgType[IngesterActor.GoodToGo]
  //       ingester ! chunkCmd
  //       expectMsg(IngesterActor.Ack("gdelt", "1979-1984", 5L))

  //       metaActor ! Partition.GetPartition("gdelt", "1979-1984")
  //       val Partition.ThePartition(p) = expectMsgType[Partition.ThePartition]
  //       p.shardVersions.size should equal (1)
  //     }
  //   }

  //   it("should return ShardingError if invalid version or rowId") {
  //     createTable("gdelt", "1979-1984", GdeltColumns)
  //     withIngesterActor("gdelt", "1979-1984", columnsToWrite) { ingester =>
  //       expectMsgType[IngesterActor.GoodToGo]
  //       ingester ! chunkCmd.copy(version = -1)
  //       expectMsg(IngesterActor.ShardingError("gdelt", "1979-1984", 5L))
  //     }
  //   }
  // }
}


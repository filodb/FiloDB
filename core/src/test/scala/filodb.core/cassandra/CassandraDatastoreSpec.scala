package filodb.core.cassandra

import akka.actor.ActorSystem
import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.datastore.Datastore
import filodb.core.metadata.{Column, Dataset, Partition, Shard}
import filodb.core.messages._

object CassandraDatastoreSpec {
  def getNewSystem = ActorSystem("test")
}

class CassandraDatastoreSpec extends AllTablesTest(CassandraDatastoreSpec.getNewSystem) {
  import Datastore._

  describe("column API") {
    it("should return IllegalColumnChange if an invalid column addition submitted") {
      val firstColumn = Column("first", "foo", 1, Column.ColumnType.StringColumn)
      whenReady(datastore.newColumn(firstColumn)) { response =>
        response should equal (Success)
      }

      whenReady(datastore.newColumn(firstColumn.copy(version = 0))) { response =>
        response.getClass should equal (classOf[IllegalColumnChange])
      }
    }

    val monthYearCol = Column("monthYear", "gdelt", 1, Column.ColumnType.LongColumn)
    it("should be able to create a Column and get the Schema") {
      datastore.newColumn(monthYearCol).futureValue should equal (Success)
      datastore.getSchema("gdelt", 10).futureValue should equal (TheSchema(Map("monthYear" -> monthYearCol)))
    }
  }

  describe("partition API") {
    val p = Partition("foo", "first")
    val pp = p.copy(shardVersions = Map(0L -> (0 -> 1)))

    it("should not allow adding an invalid or not empty Partition") {
      whenReady(datastore.newPartition(pp)) { response =>
        response should equal (NotEmpty)
      }

      whenReady(datastore.newPartition(p.copy(chunkSize = 0))) { response =>
        response should equal (NotValid)
      }
    }
  }

  describe("data API and throttling") {
    val bb = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))
    val shard = Shard(Partition("dummy", "0", chunkSize = 100), 0, 0L)
    val columnBytes = Map("a" -> bb, "b" -> bb)

    it("lots of concurrent write requests should get mostly TooManyRequests, some Acks") {
      // Cassandra is pretty fast, but with a default test max futures limit of 3, should not take
      // too many tries to hit the limit.
      val futures = (0 until 20).map(i => datastore.insertOneChunk(shard, 0L, 3L, columnBytes))
      val responses = Future.sequence(futures).futureValue
      val acks = responses.collect { case Datastore.Ack(num) => num }.length
      val tooManyRequests = responses.collect { case TooManyRequests => 1 }.length
      acks should be > 1
      tooManyRequests should be > 10
    }
  }
}
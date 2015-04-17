package filodb.core.cassandra

import akka.actor.ActorSystem

import filodb.core.datastore.Datastore
import filodb.core.metadata.{Column, Dataset, Partition}
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
}
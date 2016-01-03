package filodb.cassandra.metastore

import com.typesafe.config.ConfigFactory
import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.store.MetaStore
import filodb.cassandra.AllTablesTest

import org.scalatest.{FunSpec, BeforeAndAfter}

class CassandraMetaStoreSpec extends FunSpec with BeforeAndAfter with AllTablesTest {
  import MetaStore._

  override def beforeAll() {
    super.beforeAll()
    metaStore.initialize().futureValue
  }

  before { metaStore.clearAllData().futureValue }

  describe("column API") {
    it("should return IllegalColumnChange if an invalid column addition submitted") {
      val firstColumn = Column("first", "foo", 1, Column.ColumnType.StringColumn)
      whenReady(metaStore.newColumn(firstColumn)) { response =>
        response should equal (Success)
      }

      whenReady(metaStore.newColumn(firstColumn.copy(version = 0)).failed) { err =>
        err shouldBe an [IllegalColumnChange]
      }
    }

    val monthYearCol = Column("monthYear", "gdelt", 1, Column.ColumnType.LongColumn)
    it("should be able to create a Column and get the Schema") {
      metaStore.newColumn(monthYearCol).futureValue should equal (Success)
      metaStore.getSchema("gdelt", 10).futureValue should equal (Map("monthYear" -> monthYearCol))
    }
  }

}
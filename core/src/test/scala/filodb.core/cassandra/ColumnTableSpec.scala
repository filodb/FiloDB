package filodb.core.cassandra

import com.websudos.phantom.testing.CassandraFlatSpec
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.metadata.Column
import filodb.core.messages._

class ColumnTableSpec extends CassandraFlatSpec with BeforeAndAfter {
  val keySpace = "test"

  import Column.ColumnType

  val firstColumn = Column("first", "foo", 1, ColumnType.StringColumn)
  val ageColumn = Column("age", "foo", 1, ColumnType.IntColumn)
  val schema = Map("first" -> firstColumn, "age" -> ageColumn)

  // First create the columns table
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(ColumnTable.create.future(), 3 seconds)
  }

  before {
    Await.result(ColumnTable.truncate.future(), 3 seconds)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  "ColumnTable" should "return empty schema if a dataset does not exist in columns table" in {
    whenReady(ColumnTable.getSchema("foo", 1)) { response =>
      response should equal (Column.TheSchema(Map()))
    }
  }

  it should "add the first column and read it back as a schema" in {
    whenReady(ColumnTable.newColumn(firstColumn)) { response =>
      response should equal (Success)
    }

    whenReady(ColumnTable.getSchema("foo", 2)) { response =>
      response should equal (Column.TheSchema(Map("first" -> firstColumn)))
    }

    // Check that limiting the getSchema query to version 0 does not return the version 1 column
    whenReady(ColumnTable.getSchema("foo", 0)) { response =>
      response should equal (Column.TheSchema(Map()))
    }
  }

  it should "return MetadataException if illegal column type encoded in Cassandra" in {
    val f = ColumnTable.insert.value(_.dataset, "bar")
                              .value(_.name, "age")
                              .value(_.version, 5)
                              .value(_.columnType, "_so_not_a_real_type")
                              .future()
    Await.result(f, 3 seconds)

    whenReady(ColumnTable.getSchema("bar", 7)) { response =>
      response.getClass should equal (classOf[MetadataException])
    }
  }

  it should "return IllegalColumnChange if an invalid column addition submitted" in {
    whenReady(ColumnTable.newColumn(firstColumn)) { response =>
      response should equal (Success)
    }

    whenReady(ColumnTable.newColumn(firstColumn.copy(version = 0))) { response =>
      response.getClass should equal (classOf[Column.IllegalColumnChange])
    }
  }
}
package filodb.core.cassandra

import com.websudos.phantom.dsl._
import com.websudos.phantom.testkit._
import org.scalatest.BeforeAndAfter
import scala.concurrent.Await
import scala.concurrent.duration._

import filodb.core.datastore.Datastore
import filodb.core.metadata.Column
import filodb.core.messages._

class ColumnTableSpec extends CassandraFlatSpec with BeforeAndAfter {
  implicit val keySpace = KeySpace("unittest")

  import Column.ColumnType

  val firstColumn = Column("first", "foo", 1, ColumnType.StringColumn)

  // First create the columns table
  override def beforeAll() {
    super.beforeAll()
    // Note: This is a CREATE TABLE IF NOT EXISTS
    Await.result(ColumnTable.create.ifNotExists.future(), 3 seconds)
  }

  before {
    Await.result(ColumnTable.truncate.future(), 3 seconds)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  "ColumnTable" should "return empty schema if a dataset does not exist in columns table" in {
    whenReady(ColumnTable.getSchema("foo", 1)) { response =>
      response should equal (Datastore.TheSchema(Map()))
    }
  }

  it should "add the first column and read it back as a schema" in {
    whenReady(ColumnTable.insertColumn(firstColumn)) { response =>
      response should equal (Success)
    }

    whenReady(ColumnTable.getSchema("foo", 2)) { response =>
      response should equal (Datastore.TheSchema(Map("first" -> firstColumn)))
    }

    // Check that limiting the getSchema query to version 0 does not return the version 1 column
    whenReady(ColumnTable.getSchema("foo", 0)) { response =>
      response should equal (Datastore.TheSchema(Map()))
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
}
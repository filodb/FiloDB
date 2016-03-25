package filodb.cassandra.metastore

import com.typesafe.config.ConfigFactory
import com.websudos.phantom.dsl._
import com.websudos.phantom.testkit._
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core._
import filodb.core.metadata.{Column, DataColumn}

class ColumnTableSpec extends CassandraFlatSpec with BeforeAndAfter {
  import Column.ColumnType

  val firstColumn = DataColumn(0, "first", "foo", 1, ColumnType.StringColumn)
  val fooRef = DatasetRef("foo", Some("unittest2"))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb.cassandra")
  val columnTable = new ColumnTable(config)
  implicit val keySpace = KeySpace("unittest2")
  val timeout = Timeout(30 seconds)
  // First create the columns table
  override def beforeAll() {
    super.beforeAll()
    columnTable.createKeyspace("unittest2")
    // Note: This is a CREATE TABLE IF NOT EXISTS
    columnTable.initialize("unittest2").futureValue(timeout)
  }

  before {
    columnTable.clearAll("unittest2").futureValue(timeout)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  "ColumnTable" should "return empty schema if a dataset does not exist in columns table" in {
    columnTable.getSchema(fooRef, 1).futureValue(timeout) should equal (Map())
  }

  it should "add the first column and read it back as a schema" in {
    columnTable.insertColumn(firstColumn, fooRef).futureValue should equal (Success)
    columnTable.getSchema(fooRef, 2).futureValue(timeout) should equal (Map("first" -> firstColumn))

    // Check that limiting the getSchema query to version 0 does not return the version 1 column
    columnTable.getSchema(fooRef, 0).futureValue(timeout) should equal (Map())
  }

  it should "return MetadataException if illegal column type encoded in Cassandra" in {
    val f = columnTable.insert.value(_.dataset, "bar")
                              .value(_.name, "age")
                              .value(_.version, 5)
                              .value(_.columnType, "_so_not_a_real_type")
                              .value(_.id, 0)
                              .future()
    f.futureValue

    val barRef = DatasetRef("bar", Some("unittest2"))
    columnTable.getSchema(barRef, 7).failed.futureValue(timeout) shouldBe a [MetadataException]
  }
}
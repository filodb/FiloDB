package filodb.cassandra.metastore

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.FlatSpec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.cassandra.{AsyncTest, DefaultFiloSessionProvider}
import filodb.core._
import filodb.core.metadata.{Column, DataColumn}

class ColumnTableSpec extends FlatSpec with AsyncTest {
  import Column.ColumnType
  import scala.concurrent.ExecutionContext.Implicits.global

  val firstColumn = DataColumn(0, "first", "foo", 1, ColumnType.StringColumn)
  val fooRef = DatasetRef("foo", Some("unittest2"))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb.cassandra")
  val columnTable = new ColumnTable(config, new DefaultFiloSessionProvider(config))
  val timeout = Timeout(30 seconds)
  // First create the columns table
  override def beforeAll() {
    super.beforeAll()
    columnTable.createKeyspace(columnTable.keyspace)
    columnTable.initialize().futureValue(timeout)
  }

  before {
    columnTable.clearAll().futureValue(timeout)
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  "ColumnTable" should "return empty schema if a dataset does not exist in columns table" in {
    columnTable.getSchema(fooRef, 1).futureValue(timeout) should equal (Map())
  }

  it should "add the first column and read it back as a schema" in {
    columnTable.insertColumns(Seq(firstColumn), fooRef).futureValue should equal (Success)
    columnTable.getSchema(fooRef, 2).futureValue(timeout) should equal (Map("first" -> firstColumn))

    // Check that limiting the getSchema query to version 0 does not return the version 1 column
    columnTable.getSchema(fooRef, 0).futureValue(timeout) should equal (Map())
  }

  it should "return MetadataException if illegal column type encoded in Cassandra" in {
    columnTable.execCql(s"""INSERT INTO ${columnTable.tableString}
                           |(dataset, database, name, version, columntype, id) VALUES (
                           | 'bar', 'unittest2', 'age', 5, '_so_not_a_real_type', 0)""".stripMargin)
               .futureValue

    val barRef = DatasetRef("bar", Some("unittest2"))
    columnTable.getSchema(barRef, 7).failed.futureValue(timeout) shouldBe a [MetadataException]
  }
}
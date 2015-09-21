package filodb.cassandra

import com.typesafe.config.ConfigFactory
import com.websudos.phantom.dsl._
import com.websudos.phantom.testkit._
import org.scalatest.{FunSpec, BeforeAndAfter}
import org.scalatest.time.{Millis, Span, Seconds}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core.metadata.{Column, Dataset}
import filodb.core._
import filodb.cassandra.columnstore.CassandraColumnStore

trait AllTablesTest extends SimpleCassandraTest {
  import filodb.cassandra.metastore._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  val config = ConfigFactory.load("application_test.conf")
  implicit val keySpace = KeySpace(config.getString("cassandra.keyspace"))

  lazy val columnStore = new CassandraColumnStore(config)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))

  import Column.ColumnType._

  val dsName = "gdelt"
  val GdeltDataset = Dataset(dsName, "id")
  val GdeltColumns = Seq(Column("id",      dsName, 0, LongColumn),
                         Column("sqlDate", dsName, 0, StringColumn),
                         Column("monthYear", dsName, 0, IntColumn),
                         Column("year",    dsName, 0, IntColumn))

  val GdeltColNames = GdeltColumns.map(_.name)

  def createTable(dataset: Dataset, columns: Seq[Column]): Unit = {
    metaStore.newDataset(dataset).futureValue should equal (Success)
    columns.foreach { col => metaStore.newColumn(col).futureValue should equal (Success) }
  }
}
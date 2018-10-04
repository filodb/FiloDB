package filodb.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest._

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core._
import filodb.core.metadata.Dataset

trait AllTablesTest extends FunSpec with AsyncTest {
  import filodb.cassandra.metastore._

  implicit val scheduler = monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")

  lazy val columnStore = new CassandraColumnStore(config, scheduler)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))

  def createTable(dataset: Dataset): Unit = {
    metaStore.newDataset(dataset).futureValue should equal (Success)
  }
}
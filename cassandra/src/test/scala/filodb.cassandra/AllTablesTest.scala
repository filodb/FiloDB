package filodb.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core._
import filodb.core.metadata.Dataset

trait AsyncTest extends Suite with Matchers with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures

trait AllTablesTest extends FunSpecLike with AsyncTest {
  import filodb.cassandra.metastore._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  implicit val scheduler = monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")

  lazy val columnStore = new CassandraColumnStore(config, scheduler)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))

  def createTable(dataset: Dataset): Unit = {
    metaStore.newDataset(dataset).futureValue should equal (Success)
  }
}
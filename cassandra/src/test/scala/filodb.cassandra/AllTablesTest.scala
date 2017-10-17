package filodb.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Suite, FunSpecLike, BeforeAndAfter, BeforeAndAfterAll, Matchers}
import org.scalatest.time.{Millis, Span, Seconds}

import filodb.core.metadata.Dataset
import filodb.core._
import filodb.cassandra.columnstore.CassandraColumnStore

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
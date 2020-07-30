package filodb.cassandra

import com.typesafe.config.ConfigFactory

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core._
import org.scalatest.funspec.AnyFunSpec

trait AllTablesTest extends AnyFunSpec with AsyncTest {
  import filodb.cassandra.metastore._

  implicit val scheduler = monix.execution.Scheduler.Implicits.global

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")

  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session
  lazy val columnStore = new CassandraColumnStore(config, scheduler, session)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"), session)

}

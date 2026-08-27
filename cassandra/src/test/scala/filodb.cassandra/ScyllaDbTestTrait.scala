package filodb.cassandra

import com.typesafe.config.ConfigFactory
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.core._
import org.scalatest.funspec.AnyFunSpec

/**
 * Base trait for ScyllaDB tests.
 * This trait is identical to AllTablesTest but loads ScyllaDB-specific configuration.
 *
 * Purpose: Validate drop-in compatibility by running the same test logic against ScyllaDB.
 *
 * The only difference from Cassandra tests is the configuration file loaded.
 * All other code remains identical, proving drop-in compatibility.
 */
trait ScyllaDbTestTrait extends AnyFunSpec with AsyncTest {
  import filodb.cassandra.metastore._

  implicit val scheduler = monix.execution.Scheduler.Implicits.global

  // Load ScyllaDB-specific configuration
  val config = ConfigFactory.load("application_test_scylladb.conf").getConfig("filodb").resolve()

  lazy val session = new DefaultFiloSessionProvider(config.getConfig("cassandra")).session
  lazy val columnStore = new CassandraColumnStore(config, scheduler, session)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"), session)
}

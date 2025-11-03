package filodb.cassandra.columnstore

import com.typesafe.config.{Config, ConfigFactory}

/**
 * ScyllaDB-specific test suite for CassandraColumnStore.
 *
 * This class runs the exact same tests as CassandraColumnStoreSpec,
 * but connects to ScyllaDB instead of Cassandra.
 *
 * Purpose: Validate drop-in compatibility by proving that:
 * 1. The same FiloDB code works with ScyllaDB
 * 2. The same DataStax driver works with ScyllaDB
 * 3. All operations (read, write, query) work identically
 *
 * This is the proper way to validate ScyllaDB as a drop-in replacement.
 */
class ScyllaDbColumnStoreSpec extends CassandraColumnStoreSpec {
  // Override config to use ScyllaDB configuration
  override val config: Config = ConfigFactory.load("application_test_scylladb.conf")
    .getConfig("filodb")
    .resolve()
}

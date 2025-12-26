package filodb.cassandra.metastore

import filodb.cassandra.ScyllaDbTestTrait
import filodb.core.MetaStoreSpec

/**
 * ScyllaDB-specific test suite for CassandraMetaStore.
 *
 * This class runs the exact same tests as CassandraMetaStoreSpec,
 * but connects to ScyllaDB instead of Cassandra.
 *
 * Purpose: Validate that FiloDB's metadata operations work identically
 * on ScyllaDB, proving drop-in compatibility for:
 * - Dataset metadata storage
 * - Checkpoint management
 * - Schema operations
 *
 * The test logic is inherited from MetaStoreSpec.
 * The only difference is the configuration (ScyllaDbTestTrait vs AllTablesTest).
 */
class ScyllaDbMetaStoreSpec extends MetaStoreSpec with ScyllaDbTestTrait

package filodb.cassandra

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.{StoreFactory, CoordinatorSetup}

class CassandraStoreFactory(setup: CoordinatorSetup) extends StoreFactory {
  import setup.ec
  val columnStore = new CassandraColumnStore(setup.config, setup.readEc)
  val metaStore = new CassandraMetaStore(setup.config.getConfig("cassandra"))
}


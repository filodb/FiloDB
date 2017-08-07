package filodb.cassandra

import scala.concurrent.ExecutionContext

import com.typesafe.config.Config

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.StoreFactory

class CassandraStoreFactory(config: Config,
                            executionContext: ExecutionContext,
                            readExecutionContext: ExecutionContext
                           ) extends StoreFactory {

  implicit val ec = executionContext
  val columnStore = new CassandraColumnStore(config, readExecutionContext)
  val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))
}


package filodb.cassandra

import com.typesafe.config.Config
import monix.execution.Scheduler

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.StoreFactory
import filodb.core.downsample.DownsampledTimeSeriesStore
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.store.NullMetaStore

/**
 * A StoreFactory for a TimeSeriesMemStore backed by a Cassandra ChunkSink for on-demand recovery/persistence
 * and a Cassandra MetaStore
 *
 * @param config a Typesafe Config, not at the root but at the "filodb." level
 * @param ioPool a Monix Scheduler, recommended to be the standard I/O pool, for scheduling asynchronous I/O
 */
class CassandraTSStoreFactory(config: Config, ioPool: Scheduler) extends StoreFactory {
  val cassandraConfig = config.getConfig("cassandra")
  val session = FiloSessionProvider.openSession(cassandraConfig)
  val colStore = new CassandraColumnStore(config, ioPool, session)(ioPool)
  val metaStore = new CassandraMetaStore(cassandraConfig, session)(ioPool)
  val memStore = new TimeSeriesMemStore(config, colStore, metaStore)(ioPool)
}

class DownsampledTSStoreFactory(config: Config, ioPool: Scheduler) extends StoreFactory {
  val cassandraConfig = config.getConfig("cassandra")
  val session = FiloSessionProvider.openSession(cassandraConfig)
  val rawColStore = new CassandraColumnStore(config, ioPool, session, false)(ioPool)
  val downsampleColStore = new CassandraColumnStore(config, ioPool, session, true)(ioPool)
  val metaStore = NullMetaStore
  val memStore = new DownsampledTimeSeriesStore(downsampleColStore, rawColStore, config)(ioPool)
}

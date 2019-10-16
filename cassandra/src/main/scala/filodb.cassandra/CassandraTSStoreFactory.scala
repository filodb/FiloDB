package filodb.cassandra

import com.typesafe.config.Config
import monix.execution.Scheduler

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.StoreFactory
import filodb.core.downsample.DownsampledTimeSeriesStore
import filodb.core.memstore.TimeSeriesMemStore
import filodb.core.store.NullColumnStore

/**
 * A StoreFactory for a TimeSeriesMemStore backed by a Cassandra ChunkSink for on-demand recovery/persistence
 * and a Cassandra MetaStore
 *
 * @param config a Typesafe Config, not at the root but at the "filodb." level
 * @param ioPool a Monix Scheduler, recommended to be the standard I/O pool, for scheduling asynchronous I/O
 */
class CassandraTSStoreFactory(config: Config, ioPool: Scheduler) extends StoreFactory {
  val colStore = new CassandraColumnStore(config, ioPool)(ioPool)
  val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))(ioPool)
  val memStore = new TimeSeriesMemStore(config, colStore, metaStore)(ioPool)
}


class DownsampledTSStoreFactory(config: Config, ioPool: Scheduler) extends StoreFactory {
  val colStore = new CassandraColumnStore(config, ioPool)(ioPool)
  val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))(ioPool)
  val memStore = new DownsampledTimeSeriesStore(colStore, metaStore, config)(ioPool)
}

/**
  * A StoreFactory for a TimeSeriesMemStore with Cassandra for metadata, but NullColumnStore for
  * disabling write of chunks to a persistent column store. This can be used in environments
  * where we would like to test in-memory aspects of the store and ignore persistence and
  * on-demand-paging.
  *
  * @param config a Typesafe Config, not at the root but at the "filodb." level
  * @param ioPool a Monix Scheduler, recommended to be the standard I/O pool, for scheduling asynchronous I/O
  */
class NonPersistentTSStoreFactory(config: Config, ioPool: Scheduler) extends StoreFactory {
  val colStore = new NullColumnStore()(ioPool)
  val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))(ioPool)
  val memStore = new TimeSeriesMemStore(config, colStore, metaStore)(ioPool)
}
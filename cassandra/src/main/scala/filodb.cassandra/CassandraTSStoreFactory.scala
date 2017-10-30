package filodb.cassandra

import com.typesafe.config.Config
import monix.execution.Scheduler

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.StoreFactory
import filodb.core.memstore.TimeSeriesMemStore

/**
 * A StoreFactory for a TimeSeriesMemStore backed by a Cassandra ChunkSink for recovery/persistence
 * and a Cassandra MetaStore
 *
 * @param config a Typesafe Config, not at the root but at the "filodb." level
 * @param sched a Monix Scheduler, recommended to be the standard I/O pool, for scheduling asynchronous I/O
 */
class CassandraTSStoreFactory(config: Config, sched: Scheduler) extends StoreFactory {
  val colStore = new CassandraColumnStore(config, sched)(sched)
  val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))(sched)
  val memStore = new TimeSeriesMemStore(config, colStore, metaStore)(sched)
}
package filodb.cassandra.columnstore

import com.typesafe.config.Config
import filodb.core.store.ColumnStore

class CassandraColumnStore(config: Config) extends ColumnStore with CassandraChunkStore with CassandraSummaryStore {

  override def chunkTable: ChunkTable = new ChunkTable(config)

  override def summaryTable: SummaryTable = new SummaryTable(config)
}

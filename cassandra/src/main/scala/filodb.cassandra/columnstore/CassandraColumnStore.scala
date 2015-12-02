package filodb.cassandra.columnstore

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import filodb.core.Messages.Response
import filodb.core.store.ColumnStore

import scala.concurrent.Future

class CassandraColumnStore(keySpace: KeySpace, session: Session) extends ColumnStore
with CassandraChunkStore with CassandraSummaryStore {

  override def chunkTable: ChunkTable = new ChunkTable(keySpace, session)

  override def summaryTable: SummaryTable = new SummaryTable(keySpace, session)

  import scala.concurrent.ExecutionContext.Implicits.global

  def initialize: Future[List[Response]] = {
    for {
      c <- chunkTable.initialize()
      s <- summaryTable.initialize()
    } yield List(c, s)
  }

  def clearAll: Future[List[Response]] = {
    for {
      c <- chunkTable.clearAll()
      s <- summaryTable.clearAll()
    } yield List(c, s)
  }
}


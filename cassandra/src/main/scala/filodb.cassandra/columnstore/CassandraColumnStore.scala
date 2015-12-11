package filodb.cassandra.columnstore

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import filodb.core.Messages.Response
import filodb.core.store.ColumnStore

import scala.concurrent.{ExecutionContext, Future}

class CassandraColumnStore(val keySpace: KeySpace, val session: Session)(implicit val ec: ExecutionContext)
extends ColumnStore
  with CassandraChunkStore
  with CassandraSummaryStore
  with CassandraQueryApi {

  override def chunkTable: ChunkTable = new ChunkTable(keySpace, session)

  override def summaryTable: SummaryTable = new SummaryTable(keySpace, session)

  def initialize: Future[Seq[Response]] = {
    for {
      c <- chunkTable.initialize()
      s <- summaryTable.initialize()
    } yield Seq(c, s)
  }

  def clearAll: Future[Seq[Response]] = {
    for {
      c <- chunkTable.clearAll()
      s <- summaryTable.clearAll()
    } yield Seq(c, s)
  }

}


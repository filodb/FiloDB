package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.ChunkSinkStats

/**
 * FIXME: To be removed. The table is populated but never queried.
 *
 * Represents the table which holds the incremental chunk metadata or indexes for each segment
 * of a partition.  The chunk index contains info about each chunk including if it overrides
 * any records in previous chunks.  Each new chunkSet that is written results in a tiny bit more metadata.
 * Unlike the previous ChunkRowMap design, the layout is such that new chunks in a segment get written
 * only with its new metadata, previous index bits are not written again.
 *
 * There is an indextype field.
 *  1 = incremental indices
 * In the future there may be other types.  For example, for aggregated indices or extra metadata
 * representing keys to replace.
 */
sealed class IndexTable(val dataset: DatasetRef, val connector: FiloCassandraConnector)
                       (implicit ec: ExecutionContext) extends BaseDatasetTable {

  import filodb.cassandra.Util._

  val suffix = "index"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |    partition blob,
                     |    indextype int,
                     |    chunkid bigint,
                     |    data blob,
                     |    PRIMARY KEY (partition, indextype, chunkid)
                     |) WITH compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeIndexCql = session.prepare(
    s"INSERT INTO $tableString (partition, indextype, chunkid, data) " +
    "VALUES (?, 1, ?, ?) USING TTL ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  /**
   * Writes new indices to the index table
   * @return Success, or an exception as a Future.failure
   */
  def writeIndices(partition: Array[Byte],
                   indices: Seq[(Types.ChunkID, Array[Byte])],
                   stats: ChunkSinkStats,
                   diskTimeToLive: Int): Future[Response] = {
    var indexBytes = 0
    val partitionBuf = toBuffer(partition)
    val statements = indices.map { case (chunkId, indexData) =>
      indexBytes += indexData.size
      writeIndexCql.bind(partitionBuf,
                         chunkId: java.lang.Long,
                         ByteBuffer.wrap(indexData),
                         diskTimeToLive: java.lang.Integer)
    }
    stats.addIndexWriteStats(indexBytes)
    connector.execStmtWithRetries(unloggedBatch(statements).setConsistencyLevel(ConsistencyLevel.ONE))
  }
}

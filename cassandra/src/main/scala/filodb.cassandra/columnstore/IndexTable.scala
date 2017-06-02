package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import java.nio.ByteBuffer
import monix.reactive.Observable
import scala.concurrent.{ExecutionContext, Future}

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.metadata.RichProjection
import filodb.core.store.ColumnStoreStats

// Typical record read from serialized incremental index (ChunkInfo + Skips) entries
case class IndexRecord(binPartition: ByteBuffer, data: ByteBuffer) {
  def partition(proj: RichProjection): Types.PartitionKey = BinaryRecord(proj.partKeyBinSchema, binPartition)
}

/**
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
  import scala.collection.JavaConversions._

  val suffix = "index"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |    partition blob,
                     |    version int,
                     |    indextype int,
                     |    chunkid bigint,
                     |    data blob,
                     |    PRIMARY KEY ((partition, version), indextype, chunkid)
                     |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin


  def fromRow(row: Row): IndexRecord =
    IndexRecord(row.getBytes("partition"), row.getBytes("data"))

  val selectCql = s"SELECT partition, data FROM $tableString WHERE "
  val partVersionFilter = "partition = ? AND version = ? AND indextype = 1"
  lazy val allPartReadCql = session.prepare(selectCql + partVersionFilter)

  /**
   * Retrieves all indices from a single partition.
   */
  def getIndices(binPartition: Types.PartitionKey,
                 version: Int): Observable[IndexRecord] = {
    val it = session.execute(allPartReadCql.bind(toBuffer(binPartition),
                                                 version: java.lang.Integer))
                    .toIterator.map(fromRow)
    Observable.fromIterator(it).handleObservableErrors
  }

  val tokenQ = "TOKEN(partition, version)"

  def scanIndices(version: Int,
                  tokens: Seq[(String, String)]): Observable[IndexRecord] = {
    def cql(start: String, end: String): String =
      s"SELECT * FROM $tableString WHERE $tokenQ >= $start AND $tokenQ < $end AND indextype = 1 " +
      s"ALLOW FILTERING"
    val it = tokens.iterator.flatMap { case (start, end) =>
        session.execute(cql(start, end)).iterator
               .filter(_.getInt("version") == version)
               .map { row => fromRow(row) }
      }
    Observable.fromIterator(it).handleObservableErrors
  }

  lazy val writeIndexCql = session.prepare(
    s"INSERT INTO $tableString (partition, version, indextype, chunkid, data) " +
    "VALUES (?, ?, 1, ?, ?)")

  /**
   * Writes new indices to the index table
   * @return Success, or an exception as a Future.failure
   */
  def writeIndices(partition: Types.PartitionKey,
                   version: Int,
                   indices: Seq[(Types.ChunkID, Array[Byte])],
                   stats: ColumnStoreStats): Future[Response] = {
    var indexBytes = 0
    val partitionBuf = toBuffer(partition)
    val statements = indices.map { case (chunkId, indexData) =>
      indexBytes += indexData.size
      writeIndexCql.bind(partitionBuf,
                         version: java.lang.Integer,
                         chunkId: java.lang.Long,
                         ByteBuffer.wrap(indexData))
    }
    stats.addIndexWriteStats(indexBytes)
    connector.execStmt(unloggedBatch(statements))
  }
}

package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, ResultSet, Row}
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.ChunkSinkStats

/**
 * Mapping to chunk set info records using a full ingestion time, positioned in the cluster key
 * before the chunk start time. This makes it possible to find chunks based on the time they
 * were actually ingested, which is useful for performing cross-DC repairs and downsampling. In
 * addition, queries for all chunk set infos for a given partition are more efficient than
 * using TimeSeriesChunksTable. This is because the chunks are likely to be fetched from
 * Cassandra due to locality when using TimeSeriesChunksTable, and the chunks table is smaller.
 */
sealed class IngestionTimeIndexTable(val dataset: DatasetRef, val connector: FiloCassandraConnector)
                                    (implicit ec: ExecutionContext) extends BaseDatasetTable {
  import scala.collection.JavaConverters._

  import filodb.cassandra.Util._

  val suffix = "ingestion_time_index"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |    partition blob,
                     |    ingestion_time bigint,
                     |    start_time bigint,
                     |    info blob,
                     |    PRIMARY KEY (partition, ingestion_time, start_time)
                     |) WITH compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val allCql = session.prepare(
      s"SELECT ingestion_time, start_time, info FROM $tableString " +
      s" WHERE partition = ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  /**
    * Test method which returns all rows for a partition.
    */
  def readAllRows(partKeyBytes: ByteBuffer): ResultSet = {
    session.execute(allCql.bind().setBytes(0, partKeyBytes))
  }

  /**
    * Returns Rows consisting of:
    *
    * partition:      ByteBuffer
    * ingestion_time: long
    * start_time:     long
    * info:           ByteBuffer
    *
    * The ChunkSetInfo object contains methods for extracting fields from the info column.
    */
  def scanRowsByIngestionTime(tokens: Seq[(String, String)],
                              ingestionTimeStart: Long,
                              ingestionTimeEnd: Long): Iterator[Row] = {
    def cql(start: String, end: String): String =
      s"SELECT partition, ingestion_time, start_time, info FROM $tableString " +
      s"WHERE TOKEN(partition) >= $start AND TOKEN(partition) < $end " +
      s"AND ingestion_time >= $ingestionTimeStart AND ingestion_time <= $ingestionTimeEnd " +
      s"ALLOW FILTERING"

    tokens.iterator.flatMap { case (start, end) =>
      session.execute(cql(start, end)).iterator.asScala
    }
  }

  def scanPartKeysByIngestionTime(tokens: Seq[(String, String)],
                                  ingestionTimeStart: Long,
                                  ingestionTimeEnd: Long): Observable[ByteBuffer] = {
    def cql(start: String, end: String): String =
      s"SELECT partition FROM $tableString WHERE TOKEN(partition) >= $start AND TOKEN(partition) < $end " +
        s"AND ingestion_time >= $ingestionTimeStart AND ingestion_time <= $ingestionTimeEnd " +
        s"ALLOW FILTERING"
    val it = tokens.iterator.flatMap { case (start, end) =>
      session.execute(cql(start, end)).iterator.asScala
        .map { row => row.getBytes("partition") }
    }
    Observable.fromIterator(it).handleObservableErrors
  }

  lazy val writeIndexCql = session.prepare(
    s"INSERT INTO $tableString (partition, ingestion_time, start_time, info) " +
    "VALUES (?, ?, ?, ?) USING TTL ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  /**
   * Writes new records to the ingestion table.
   *
   * @param infos tuples consisting of ingestion time (millis from 1970), chunk start time, and
   * chunk set info bytes
   * @return Success, or an exception as a Future.failure
   */
  def writeIndices(partition: Array[Byte],
                   infos: Seq[(Long, Long, Array[Byte])],
                   stats: ChunkSinkStats,
                   diskTimeToLiveSeconds: Int): Future[Response] = {
    var infoBytes = 0
    val partitionBuf = toBuffer(partition)
    val statements = infos.map { case (ingestionTime, startTime, info) =>
      infoBytes += info.size
      writeIndexCql.bind(partitionBuf,
                         ingestionTime: java.lang.Long,
                         startTime: java.lang.Long,
                         ByteBuffer.wrap(info),
                         diskTimeToLiveSeconds: java.lang.Integer)
    }
    stats.addIndexWriteStats(infoBytes)
    connector.execStmtWithRetries(unloggedBatch(statements).setConsistencyLevel(ConsistencyLevel.ONE))
  }

  /**
    * Writes a single record, exactly as-is from the scanInfosByIngestionTime method. Is
    * used to copy records from one column store to another.
    */
  def writeIndex(row: Row, diskTimeToLiveSeconds: Int): Future[Response] = {
    connector.execStmtWithRetries(writeIndexCql.bind(
      row.getBytes(0),                // partition
      row.getLong(1): java.lang.Long, // ingestion_time
      row.getLong(2): java.lang.Long, // start_time
      row.getBytes(3),                // info
      diskTimeToLiveSeconds: java.lang.Integer)
    )
  }
}

package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Row}
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.ChunkSinkStats
import filodb.memory.format.UnsafeUtils

case class InfoRecord(binPartition: ByteBuffer, data: ByteBuffer) {
  def partBaseOffset: (Any, Long, Int) = UnsafeUtils.BOLfromBuffer(binPartition)
}

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

  val suffix = "ingestion"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |    partition blob,
                     |    ingestion_time bigint,
                     |    start_time bigint,
                     |    info blob,
                     |    PRIMARY KEY (partition, ingestion_time, start_time)
                     |) WITH compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  def fromRow(row: Row): InfoRecord =
    InfoRecord(row.getBytes("partition"), row.getBytes("info"))

  val selectCql = s"SELECT partition, info FROM $tableString WHERE "
  lazy val allPartReadCql = session.prepare(selectCql + "partition = ?")
  lazy val inPartReadCql = session.prepare(selectCql + "partition IN ?")

  /**
   * Retrieves all infos from a single partition.
   */
  def getInfos(binPartition: Array[Byte]): Observable[InfoRecord] = {
    val it = session.execute(allPartReadCql.bind(toBuffer(binPartition)))
                    .asScala.toIterator.map(fromRow)
    Observable.fromIterator(it).handleObservableErrors
  }

  def getMultiInfos(partitions: Seq[Array[Byte]]): Observable[InfoRecord] = {
    val query = inPartReadCql.bind().setList(0, partitions.map(toBuffer).asJava, classOf[ByteBuffer])
    val it = session.execute(query).asScala.toIterator.map(fromRow)
    Observable.fromIterator(it).handleObservableErrors
  }

  def scanInfos(tokens: Seq[(String, String)]): Observable[InfoRecord] = {
    def cql(start: String, end: String): String =
      s"SELECT * FROM $tableString WHERE TOKEN(partition) >= $start AND TOKEN(partition) < $end " +
      s"ALLOW FILTERING"
    val it = tokens.iterator.flatMap { case (start, end) =>
        session.execute(cql(start, end)).iterator.asScala
               .map { row => fromRow(row) }
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
  def writeIndexes(partition: Array[Byte],
                   infos: Seq[(Long, Long, Array[Byte])],
                   stats: ChunkSinkStats,
                   diskTimeToLive: Int): Future[Response] = {
    var infoBytes = 0
    val partitionBuf = toBuffer(partition)
    val statements = infos.map { case (ingestionTime, startTime, info) =>
      infoBytes += info.size
      writeIndexCql.bind(partitionBuf,
                         ingestionTime: java.lang.Long,
                         startTime: java.lang.Long,
                         ByteBuffer.wrap(info),
                         diskTimeToLive: java.lang.Integer)
    }
    stats.addIndexWriteStats(infoBytes)
    connector.execStmtWithRetries(unloggedBatch(statements).setConsistencyLevel(ConsistencyLevel.ONE))
  }
}

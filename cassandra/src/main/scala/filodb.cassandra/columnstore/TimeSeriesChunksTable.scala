package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import scala.concurrent.Future

import com.datastax.driver.core.{ConsistencyLevel, Row}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store._

/**
 * Represents the table which holds the actual columnar chunks, but
 * with a time series rather than OLAP layout.  The chunks for different columns are stored together grouped
 * under the same chunkID.  This reduces read latency when we read on-demand data because for time series
 * we always read all the chunks together.
 */
sealed class TimeSeriesChunksTable(val dataset: DatasetRef,
                                   val connector: FiloCassandraConnector,
                                   writeConsistencyLevel: ConsistencyLevel)
                                  (implicit sched: Scheduler) extends BaseDatasetTable {
  import collection.JavaConverters._
  import filodb.cassandra.Util._
  import filodb.core.Iterators._

  val suffix = "tschunks"

  private val compressChunks = connector.config.getBoolean("lz4-chunk-compress")

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                    |    partition blob,
                    |    chunkid bigint,
                    |    info blob,
                    |    chunks frozen<list<blob>>,
                    |    PRIMARY KEY (partition, chunkid)
                    |) WITH compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, chunkid, info, chunks
      |) VALUES (?, ?, ?, ?) USING TTL ?""".stripMargin
  ).setConsistencyLevel(writeConsistencyLevel)

  def writeChunks(partition: Array[Byte],
                  chunkInfo: ChunkSetInfo,
                  chunks: Seq[ByteBuffer],
                  stats: ChunkSinkStats,
                  diskTimeToLive: Int): Future[Response] = {
    var chunkBytes = 0L
    val chunkList = chunks.map { bytes =>
                      val finalBytes = compressChunk(bytes)
                      chunkBytes += finalBytes.capacity.toLong
                      finalBytes
                    }.asJava
    val insert = writeChunksCql.bind().setBytes(0, toBuffer(partition))
                                      .setLong(1, chunkInfo.id)
                                      .setBytes(2, toBuffer(ChunkSetInfo.toBytes(chunkInfo)))
                                      .setList(3, chunkList, classOf[ByteBuffer])
                                      .setInt(4, diskTimeToLive)
    stats.addChunkWriteStats(chunks.length, chunkBytes, chunkInfo.numRows)
    connector.execStmtWithRetries(insert.setConsistencyLevel(writeConsistencyLevel))
  }

  lazy val readChunkInCql = session.prepare(
                                 s"""SELECT info, chunks FROM $tableString WHERE
                                  | partition = ?
                                  | AND chunkid IN ?""".stripMargin)
                              .setConsistencyLevel(ConsistencyLevel.ONE)

  /**
   * Reads and returns a single RawPartData, raw data for a single partition/time series
   */
  def readRawPartitionData(partKeyBytes: Array[Byte],
                           chunkInfos: Seq[Array[Byte]]): Task[RawPartData] = {
    val query = readChunkInCql.bind().setBytes(0, toBuffer(partKeyBytes))
                                     .setList(1, chunkInfos.map(ChunkSetInfo.getChunkID).asJava)
    val futChunksets = session.executeAsync(query)
                              .toIterator.handleErrors
                              .map(_.map { row => chunkSetFromRow(row) })
    Task.fromFuture(futChunksets).map { chunkSetIt =>
      RawPartData(partKeyBytes, chunkSetIt.toBuffer)
    }
  }

  private def chunkSetFromRow(row: Row, infoIndex: Int = 0): RawChunkSet = {
    val chunks = row.getList(infoIndex + 1, classOf[ByteBuffer]).toArray.map {
      case b: ByteBuffer => decompressChunk(b)
    }
    RawChunkSet(row.getBytes(infoIndex).array, chunks)
  }

  /**
   * Reads and returns a stream of RawPartDatas given a range of chunkIDs from multiple partitions
   */
  lazy val readChunkRangeCql = session.prepare(
                                 s"""SELECT partition, info, chunks FROM $tableString WHERE
                                  | partition IN ?
                                  | AND chunkid >= ? AND chunkid < ?""".stripMargin)
                              .setConsistencyLevel(ConsistencyLevel.ONE)

  def readRawPartitionRange(partitions: Seq[Array[Byte]],
                            startTime: Long,
                            endTimeExclusive: Long): Observable[RawPartData] = {
    readRawPartitionRangeBB(partitions.map(toBuffer), startTime, endTimeExclusive)
  }

  def readRawPartitionRangeBB(partitions: Seq[ByteBuffer],
                              startTime: Long,
                              endTimeExclusive: Long): Observable[RawPartData] = {
    val query = readChunkRangeCql.bind().setList(0, partitions.asJava, classOf[ByteBuffer])
                                        .setLong(1, chunkID(startTime, 0))
                                        .setLong(2, chunkID(endTimeExclusive, 0))
    val futRawParts = session.executeAsync(query)
                             .toIterator.handleErrors
                             .map { rowIt =>
                               rowIt.map { row => (row.getBytes(0), chunkSetFromRow(row, 1)) }
                                 .sortedGroupBy(_._1)
                                 .map { case (partKeyBuffer, chunkSetIt) =>
                                   RawPartData(partKeyBuffer.array, chunkSetIt.map(_._2).toBuffer)
                                 }
                             }
    Observable.fromFuture(futRawParts).flatMap { it: Iterator[RawPartData] => Observable.fromIterator(it) }
  }

  def scanPartitionsBySplit(tokens: Seq[(String, String)]): Observable[RawPartData] = {
    def cql(start: String, end: String): String =
      s"SELECT partition, info, chunks FROM $tableString WHERE TOKEN(partition) >= $start AND TOKEN(partition) < $end "
    val res: Observable[Future[Iterator[RawPartData]]] = Observable.fromIterable(tokens).map { case (start, end) =>
      session.executeAsync(cql(start, end)).toIterator.handleErrors
              .map { rowIt =>
                rowIt.map { row => (row.getBytes(0), chunkSetFromRow(row, 1)) }
                  .sortedGroupBy(_._1)
                  .map { case (partKeyBuffer, chunkSetIt) =>
                    RawPartData(partKeyBuffer.array, chunkSetIt.map(_._2).toBuffer)
                  }
              }
    }
    res.flatMap{ f => Observable.fromFuture(f).flatMap { it: Iterator[RawPartData] => Observable.fromIterator(it) } }
  }


  private def compressChunk(orig: ByteBuffer): ByteBuffer =
    if (compressChunks) compress(orig) else orig
}
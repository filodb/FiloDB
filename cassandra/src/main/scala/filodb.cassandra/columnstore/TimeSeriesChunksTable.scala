package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import scala.concurrent.Future

import com.datastax.driver.core.{ConsistencyLevel, ResultSet, Row}
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

  private lazy val writeChunksCql = session.prepare(
    s"INSERT INTO $tableString (partition, chunkid, info, chunks) " +
    s"VALUES (?, ?, ?, ?) USING TTL ?")
    .setConsistencyLevel(writeConsistencyLevel)

  private lazy val deleteChunksCql = session.prepare(
    s"DELETE FROM $tableString WHERE partition=? AND chunkid IN ?")
    .setConsistencyLevel(writeConsistencyLevel)

  private lazy val readChunkInCql = session.prepare(
    s"SELECT info, chunks FROM $tableString " +
    s"WHERE partition = ? AND chunkid IN ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  private lazy val readChunksCql = session.prepare(
    s"SELECT chunkid, info, chunks FROM $tableString " +
    s"WHERE partition = ? AND chunkid IN ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  private lazy val readAllChunksCql = session.prepare(
    s"SELECT chunkid, info, chunks FROM $tableString " +
    s"WHERE partition = ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  private lazy val scanBySplit = session.prepare(
    s"SELECT partition, info, chunks FROM $tableString " +
    s"WHERE TOKEN(partition) >= ? AND TOKEN(partition) < ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  private lazy val readChunkRangeCql = session.prepare(
    s"SELECT partition, info, chunks FROM $tableString " +
    s"WHERE partition IN ? AND chunkid >= ? AND chunkid < ?")
    .setConsistencyLevel(ConsistencyLevel.ONE)


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

  /**
    * Writes a single record, exactly as-is from the readChunksNoAsync method. Is
    * used to copy records from one column store to another.
    */
  def writeChunks(partKeyBytes: ByteBuffer,
                  row: Row,
                  stats: ChunkSinkStats,
                  diskTimeToLiveSeconds: Int): Future[Response] = {

    val info = row.getBytes(1)
    val chunks = row.getList(2, classOf[ByteBuffer])

    val chunkBytes = chunks.asScala.map(buf => buf.remaining()).reduce(_ + _)

    stats.addChunkWriteStats(chunks.size(), chunkBytes, ChunkSetInfo.getNumRows(info))

    connector.execStmtWithRetries(writeChunksCql.bind(
      partKeyBytes,                        // partition
      row.getLong(0): java.lang.Long,      // chunkid
      info,
      chunks,
      diskTimeToLiveSeconds: java.lang.Integer)
    )
  }

  /**
    * Deletes raw chunk set rows.
    */
  def deleteChunks(partKeyBytes: ByteBuffer,
                   chunkInfos: Seq[ByteBuffer]): Future[Response] = {
    val query = deleteChunksCql.bind().setBytes(0, partKeyBytes)
                                      .setList(1, chunkInfos.map(ChunkSetInfo.getChunkID).asJava)
    connector.execStmtWithRetries(query)
  }

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
    * Reads raw chunk set Rows consisting of:
    *
    * chunkid: Long
    * info:    ByteBuffer
    * chunks:  List<ByteBuffer>
    *
    * Note: This method is intended for use by repair jobs and isn't async-friendly.
    */
  def readChunksNoAsync(partKeyBytes: ByteBuffer,
                        chunkInfos: Seq[ByteBuffer]): ResultSet = {
    val query = readChunksCql.bind().setBytes(0, partKeyBytes)
                                    .setList(1, chunkInfos.map(ChunkSetInfo.getChunkID).asJava)
    session.execute(query)
  }

  /**
    * Test method which returns the same results as the readChunks method. Not async-friendly.
    */
  def readAllChunksNoAsync(partKeyBytes: ByteBuffer): ResultSet = {
    session.execute(readAllChunksCql.bind().setBytes(0, partKeyBytes))
  }

  /**
   * Reads and returns a stream of RawPartDatas given a range of chunkIDs from multiple partitions
   */
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
    val futRawParts: Future[Iterator[RawPartData]] = session.executeAsync(query)
                             .toIterator.handleErrors
                             .map { rowIt =>
                               rowIt.map { row => (row.getBytes(0), chunkSetFromRow(row, 1)) }
                                 .sortedGroupBy(_._1)
                                 .map { case (partKeyBuffer, chunkSetIt) =>
                                   RawPartData(partKeyBuffer.array, chunkSetIt.map(_._2).toBuffer)
                                 }
                             }
    for {
      it <- Observable.fromFuture(futRawParts)
      rpd <- Observable.fromIterator(it)
    } yield rpd
  }

  def scanPartitionsBySplit(tokens: Seq[(String, String)]): Observable[RawPartData] = {

    val res: Observable[Future[Iterator[RawPartData]]] = Observable.fromIterable(tokens).map { case (start, end) =>
      /*
       * FIXME conversion of tokens to Long works only for Murmur3Partitioner because it generates
       * Long based tokens. If other partitioners are used, this can potentially break.
       * Correct way to bind tokens is to do stmt.bind().setPartitionKeyToken(token)
       */
      val stmt = scanBySplit.bind(start.toLong: java.lang.Long, end.toLong: java.lang.Long)
      session.executeAsync(stmt).toIterator.handleErrors
              .map { rowIt =>
                rowIt.map { row => (row.getBytes(0), chunkSetFromRow(row, 1)) }
                  .sortedGroupBy(_._1)
                  .map { case (partKeyBuffer, chunkSetIt) =>
                    RawPartData(partKeyBuffer.array, chunkSetIt.map(_._2).toBuffer)
                  }
              }
    }

    for {
      fut <- res
      it <- Observable.fromFuture(fut)
      rpd <- Observable.fromIterator(it)
    } yield rpd
  }


  private def compressChunk(orig: ByteBuffer): ByteBuffer =
    if (compressChunks) compress(orig) else orig
}

package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.ColumnStoreStats

case class IndexRecord(binPartition: Types.BinaryPartition,
                       segmentId: Types.SegmentId,
                       data: ByteBuffer)

/**
 * Represents the table which holds the incremental chunk metadata or indexes for each segment
 * of a partition.  The chunk index contains info about each chunk including if it overrides
 * any records in previous chunks.  Each new chunkSet that is written results in a tiny bit more metadata.
 * Unlike the previous ChunkRowMap design, the layout is such that new chunks in a segment get written
 * only with its new metadata, previous index bits are not written again.
 */
sealed class IndexTable(val dataset: DatasetRef, val connector: FiloCassandraConnector)
                       (implicit ec: ExecutionContext) extends BaseDatasetTable {
  import filodb.cassandra.Util._
  import scala.collection.JavaConversions._

  val suffix = "index"

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |    partition blob,
                     |    version int,
                     |    segmentid blob,
                     |    chunkid int,
                     |    data blob,
                     |    PRIMARY KEY ((partition, version), segmentid, chunkid)
                     |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin


  def fromRow(row: Row): IndexRecord =
    IndexRecord(ByteVector(row.getBytes("partition")),
                ByteVector(row.getBytes("segmentid")),
                row.getBytes("data"))

  val selectCql = s"SELECT partition, segmentid, data FROM $tableString WHERE "
  val partVersionFilter = "partition = ? AND version = ? "
  lazy val allPartReadCql = session.prepare(selectCql + partVersionFilter)
  lazy val rangeReadCql = session.prepare(selectCql + partVersionFilter +
                                          "AND segmentid >= ? AND segmentid <= ?")
  lazy val rangeReadExclCql = session.prepare(selectCql + partVersionFilter +
                                              "AND segmentid >= ? AND segmentid < ?")

  /**
   * Retrieves all indices from a single partition.
   */
  def getIndices(binPartition: Types.BinaryPartition,
                 version: Int): Future[Iterator[IndexRecord]] =
    session.executeAsync(allPartReadCql.bind(toBuffer(binPartition),
                                             version: java.lang.Integer))
           .toIterator.map(_.map(fromRow))

  /**
   * Retrieves a whole series of indices, in the range [startSegmentId, untilSegmentId)
   * End is exclusive or not depending on keyRange.endExclusive flag
   */
  def getIndices(keyRange: BinaryKeyRange,
                 version: Int): Future[Iterator[IndexRecord]] = {
    val cql = if (keyRange.endExclusive) rangeReadExclCql else rangeReadCql
    session.executeAsync(cql.bind(toBuffer(keyRange.partition),
                                  version: java.lang.Integer,
                                  toBuffer(keyRange.start), toBuffer(keyRange.end)))
           .toIterator.map(_.map(fromRow))
  }

  val tokenQ = "TOKEN(partition, version)"

  def scanIndices(version: Int,
                  tokens: Seq[(String, String)],
                  segmentClause: String = ""): Future[Iterator[IndexRecord]] = {
    def cql(start: String, end: String): String =
      s"SELECT * FROM $tableString WHERE $tokenQ >= $start AND $tokenQ < $end $segmentClause"
    Future {
      tokens.iterator.flatMap { case (start, end) =>
        session.execute(cql(start, end)).iterator
               .filter(_.getInt("version") == version)
               .map { row => fromRow(row) }
      }
    }
  }

  /**
   * Retrieves a series of indices from all partitions in the given token range,
   * filtered by startSegment until endSegment inclusive.
   */
  def scanIndicesRange(version: Int,
                       tokens: Seq[(String, String)],
                       startSegment: Types.SegmentId,
                       endSegment: Types.SegmentId): Future[Iterator[IndexRecord]] = {
    val clause = s"AND segmentid >= 0x${startSegment.toHex} AND segmentid <= 0x${endSegment.toHex} " +
                  "ALLOW FILTERING"
    scanIndices(version, tokens, clause)
  }

  lazy val writeIndexCql = session.prepare(
    s"INSERT INTO $tableString (partition, version, segmentid, chunkid, data) " +
    "VALUES (?, ?, ?, ?, ?)")

  /**
   * Writes new indices to the index table
   * @return Success, or an exception as a Future.failure
   */
  def writeIndices(partition: Types.BinaryPartition,
                   version: Int,
                   segmentId: Types.SegmentId,
                   indices: Seq[(Int, Array[Byte])],
                   stats: ColumnStoreStats): Future[Response] = {
    var indexBytes = 0
    val partitionBuf = toBuffer(partition)
    val segmentBuf = toBuffer(segmentId)
    val statements = indices.map { case (chunkId, indexData) =>
      indexBytes += indexData.size
      writeIndexCql.bind(partitionBuf,
                         version: java.lang.Integer,
                         segmentBuf,
                         chunkId: java.lang.Integer,
                         ByteBuffer.wrap(indexData))
    }
    stats.addIndexWriteStats(indexBytes)
    connector.execStmt(unloggedBatch(statements))
  }
}

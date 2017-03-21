package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.ColumnStoreStats

case class ChunkRowMapRecord(binPartition: Types.BinaryPartition,
                             segmentId: Types.SegmentId,
                             chunkIds: ByteBuffer,
                             rowNums: ByteBuffer,
                             nextChunkId: Int)

/**
 * Represents the table which holds the ChunkRowMap for each segment of a partition.
 * This maps sort keys in sorted order to chunks and row number within each chunk.
 * The ChunkRowMap is written as two Filo binary vectors.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class ChunkRowMapTable(dataset: DatasetRef, connector: FiloCassandraConnector)
                             (implicit ec: ExecutionContext) extends StrictLogging {
  import filodb.cassandra.Util._
  import scala.collection.JavaConversions._

  val keyspace = dataset.database.getOrElse(connector.defaultKeySpace)
  val tableString = s"${keyspace}.${dataset.dataset + "_chunkmap"}"
  val session = connector.session

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                     |    partition blob,
                     |    version int,
                     |    segmentid blob,
                     |    chunkids blob,
                     |    nextchunkid int,
                     |    rownums blob,
                     |    PRIMARY KEY ((partition, version), segmentid)
                     |)""".stripMargin


  def fromRow(row: Row): ChunkRowMapRecord =
    ChunkRowMapRecord(ByteVector(row.getBytes("partition")),
                      ByteVector(row.getBytes("segmentid")),
                      row.getBytes("chunkids"), row.getBytes("rownums"), row.getInt("nextchunkid"))

  def initialize(): Future[Response] = connector.execCql(createCql)

  def clearAll(): Future[Response] = connector.execCql(s"TRUNCATE $tableString")

  def drop(): Future[Response] = connector.execCql(s"DROP TABLE IF EXISTS $tableString")

  val selectCql = s"SELECT * FROM $tableString WHERE "
  val partVersionFilter = "partition = ? AND version = ? "
  lazy val allPartReadCql = session.prepare(selectCql + partVersionFilter)
  lazy val rangeReadCql = session.prepare(selectCql + partVersionFilter +
                                          "AND segmentid >= ? AND segmentid <= ?")
  lazy val rangeReadExclCql = session.prepare(selectCql + partVersionFilter +
                                              "AND segmentid >= ? AND segmentid < ?")

  /**
   * Retrieves all chunk maps from a single partition.
   */
  def getChunkMaps(binPartition: Types.BinaryPartition,
                   version: Int): Future[Iterator[ChunkRowMapRecord]] =
    session.executeAsync(allPartReadCql.bind(toBuffer(binPartition),
                                             version: java.lang.Integer))
           .toIterator.map(_.map(fromRow)).handleErrors

  /**
   * Retrieves a whole series of chunk maps, in the range [startSegmentId, untilSegmentId)
   * End is exclusive or not depending on keyRange.endExclusive flag
   * @return ChunkMaps(...), if nothing found will return ChunkMaps(Nil).
   */
  def getChunkMaps(keyRange: BinaryKeyRange,
                   version: Int): Future[Iterator[ChunkRowMapRecord]] = {
    val cql = if (keyRange.endExclusive) rangeReadExclCql else rangeReadCql
    session.executeAsync(cql.bind(toBuffer(keyRange.partition),
                                  version: java.lang.Integer,
                                  toBuffer(keyRange.start), toBuffer(keyRange.end)))
           .toIterator.map(_.map(fromRow)).handleErrors
  }

  val tokenQ = "TOKEN(partition, version)"

  def scanChunkMaps(version: Int,
                    tokens: Seq[(String, String)],
                    segmentClause: String = ""): Future[Iterator[ChunkRowMapRecord]] = {
    def cql(start: String, end: String): String =
      selectCql + s"$tokenQ >= $start AND $tokenQ < $end $segmentClause"
    Future {
      tokens.iterator.flatMap { case (start, end) =>
        session.execute(cql(start, end)).iterator
               .filter(_.getInt("version") == version)
               .map { row => fromRow(row) }
      }
    }.handleErrors
  }

  /**
   * Retrieves a series of chunk maps from all partitions in the given token range,
   * filtered by startSegment until endSegment inclusive.
   */
  def scanChunkMapsRange(version: Int,
                         tokens: Seq[(String, String)],
                         startSegment: Types.SegmentId,
                         endSegment: Types.SegmentId): Future[Iterator[ChunkRowMapRecord]] = {
    val clause = s"AND segmentid >= 0x${startSegment.toHex} AND segmentid <= 0x${endSegment.toHex} " +
                  "ALLOW FILTERING"
    scanChunkMaps(version, tokens, clause)
  }

  lazy val writeChunkMapCql = session.prepare(
    s"INSERT INTO $tableString (partition, version, segmentid, chunkids, rownums, nextchunkid) " +
    "VALUES (?, ?, ?, ?, ?, ?)")

  /**
   * Writes a new chunk map to the chunkRowTable.
   * @return Success, or an exception as a Future.failure
   */
  def writeChunkMap(partition: Types.BinaryPartition,
                    version: Int,
                    segmentId: Types.SegmentId,
                    chunkIds: ByteBuffer,
                    rowNums: ByteBuffer,
                    nextChunkId: Int,
                    stats: ColumnStoreStats): Future[Response] = {
    stats.addIndexWriteStats(chunkIds.capacity.toLong + rowNums.capacity.toLong + 4L)
    connector.execStmt(writeChunkMapCql.bind(toBuffer(partition),
                                             version: java.lang.Integer,
                                             toBuffer(segmentId),
                                             chunkIds,
                                             rowNums,
                                             nextChunkId: java.lang.Integer))
  }
}

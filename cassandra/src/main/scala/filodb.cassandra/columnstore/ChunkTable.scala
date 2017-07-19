package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.{ColumnStoreStats, ChunkedData}

/**
 * Represents the table which holds the actual columnar chunks for segments
 *
 * Data is stored in a columnar fashion similar to Parquet -- grouped by column.  Each
 * chunk actually stores many many rows grouped together into one binary chunk for efficiency.
 */
sealed class ChunkTable(dataset: DatasetRef, connector: FiloCassandraConnector)
                       (implicit ec: ExecutionContext) extends StrictLogging {
  import filodb.cassandra.Util._
  import collection.JavaConverters._

  val keyspace = dataset.database.getOrElse(connector.defaultKeySpace)
  val tableString = s"${keyspace}.${dataset.dataset + "_chunks"}"
  val session = connector.session

  // WITH COMPACT STORAGE saves 35% on storage costs according to this article:
  // http://blog.librato.com/posts/cassandra-compact-storage
  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                    |    partition blob,
                    |    version int,
                    |    columnname text,
                    |    segmentid blob,
                    |    chunkid int,
                    |    data blob,
                    |    PRIMARY KEY ((partition, version), columnname, segmentid, chunkid)
                    |) WITH COMPACT STORAGE""".stripMargin

  def initialize(): Future[Response] = connector.execCql(createCql)

  def clearAll(): Future[Response] = connector.execCql(s"TRUNCATE $tableString")

  def drop(): Future[Response] = connector.execCql(s"DROP TABLE IF EXISTS $tableString")

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, version, segmentid, chunkid, columnname, data
      |) VALUES (?, ?, ?, ?, ?, ?)""".stripMargin
  )

  def writeChunks(partition: Types.BinaryPartition,
                  version: Int,
                  segmentId: Types.SegmentId,
                  chunks: Iterator[(String, Types.ChunkID, ByteBuffer)],
                  stats: Option[ColumnStoreStats]): Future[Response] = {
    val partBytes = toBuffer(partition)
    val segKeyBytes = toBuffer(segmentId)
    var chunkBytes = 0L
    val statements = chunks.map { case (columnName, id, bytes) =>
      chunkBytes += bytes.capacity.toLong
      writeChunksCql.bind(partBytes, version: java.lang.Integer, segKeyBytes,
                          id: java.lang.Integer, columnName, bytes)
    }.toSeq
    if (stats.nonEmpty) {
      stats.get.addChunkWriteStats(statements.length, chunkBytes)
    }
    connector.execStmt(unloggedBatch(statements))
  }

  val readChunksCql = s"""SELECT segmentid, chunkid, data FROM $tableString WHERE
                         | columnname = ? AND partition = ? AND version = ? AND
                         | segmentid >= ? AND """.stripMargin

  lazy val readChunksCqlExcl = session.prepare(readChunksCql + "segmentid < ?")
  lazy val readChunksCqlIncl = session.prepare(readChunksCql + "segmentid <= ?")

  // Reads back all the chunks from the requested column for the segments falling within
  // the starting and ending segment IDs.  No paging is performed - so be sure to not
  // ask for too large of a range.  Also, beware the starting segment ID must line up with the
  // segment boundary.
  // endExclusive indicates if the end segment ID is exclusive or not.
  def readChunks(partition: Types.BinaryPartition,
                 version: Int,
                 column: String,
                 startSegmentId: Types.SegmentId,
                 untilSegmentId: Types.SegmentId,
                 endExclusive: Boolean = true): Future[ChunkedData] = {
    val query = (if (endExclusive) readChunksCqlExcl else readChunksCqlIncl).bind(
                  column, toBuffer(partition), version: java.lang.Integer,
                  toBuffer(startSegmentId), toBuffer(untilSegmentId))
    session.executeAsync(query).toScalaFuture.map { rs =>
      val rows = rs.all().asScala
      val byteVectorChunks = rows.map { row => (ByteVector(row.getBytes(0)), row.getInt(1), row.getBytes(2)) }
      ChunkedData(column, byteVectorChunks)
    }
  }
}

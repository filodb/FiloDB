package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.{ColumnStoreStats, ChunkedData, compress, decompress}

/**
 * Represents the table which holds the actual columnar chunks for segments
 *
 * Data is stored in a columnar fashion similar to Parquet -- grouped by column.  Each
 * chunk actually stores many many rows grouped together into one binary chunk for efficiency.
 */
sealed class ChunkTable(val dataset: DatasetRef, val connector: FiloCassandraConnector)
                       (implicit ec: ExecutionContext) extends BaseDatasetTable {
  import filodb.cassandra.Util._
  import collection.JavaConverters._

  val suffix = "chunks"

  private val compressChunks = connector.config.getBoolean("lz4-chunk-compress")
  private val compressBytePrefix = 0xff.toByte

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
                    |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, version, segmentid, chunkid, columnname, data
      |) VALUES (?, ?, ?, ?, ?, ?)""".stripMargin
  )

  def writeChunks(partition: Types.BinaryPartition,
                  version: Int,
                  segmentId: Types.SegmentId,
                  chunkId: Types.ChunkID,
                  chunks: Map[String, ByteBuffer],
                  stats: ColumnStoreStats): Future[Response] = {
    val partBytes = toBuffer(partition)
    val segKeyBytes = toBuffer(segmentId)
    var chunkBytes = 0L
    val statements = chunks.map { case (columnName, bytes) =>
      val finalBytes = compressChunk(bytes)
      chunkBytes += finalBytes.capacity.toLong
      writeChunksCql.bind(partBytes, version: java.lang.Integer, segKeyBytes,
                          chunkId: java.lang.Integer, columnName, finalBytes)
    }.toSeq
    stats.addChunkWriteStats(statements.length, chunkBytes)
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
      val byteVectorChunks = rows.map { row => (ByteVector(row.getBytes(0)),
                                                row.getInt(1),
                                                decompressChunk(row.getBytes(2))) }
      ChunkedData(column, byteVectorChunks)
    }
  }

  lazy val readChunkRangeCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnname = ? AND partition = ? AND version = ? AND
                                  | segmentid = ? AND chunkid >= ? AND chunkid <= ?""".stripMargin)

  def readChunks(partition: Types.BinaryPartition,
                 version: Int,
                 column: String,
                 segmentId: Types.SegmentId,
                 chunkRange: (Types.ChunkID, Types.ChunkID)): Future[ChunkedData] = {
    val query = readChunkRangeCql.bind(column, toBuffer(partition), version: java.lang.Integer,
                                       toBuffer(segmentId),
                                       chunkRange._1: java.lang.Integer,
                                       chunkRange._2: java.lang.Integer)
    session.executeAsync(query).toScalaFuture.map { rs =>
      val rows = rs.all().asScala
      val byteVectorChunks = rows.map { row => (segmentId,
                                                row.getInt(0),
                                                decompressChunk(row.getBytes(1))) }
      ChunkedData(column, byteVectorChunks)
    }
  }

  private def compressChunk(orig: ByteBuffer): ByteBuffer = {
    if (compressChunks) {
      val newBuf = compress(orig, offset = 1)
      newBuf.put(compressBytePrefix)
      newBuf.position(0)
      newBuf
    } else {
      // The lowest first byte of a valid Filo vector will not be 0xFF.
      orig
    }
  }

  private def decompressChunk(compressed: ByteBuffer): ByteBuffer = {
    compressed.get(0) match {
      case `compressBytePrefix` => decompress(compressed, offset = 1)
      case other: Any           => compressed
    }
  }
}

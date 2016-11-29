package filodb.cassandra.columnstore

import com.datastax.driver.core.{BoundStatement, Row}
import java.nio.ByteBuffer
import java.lang.{Long => jlLong, Integer => jlInt}
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
                    |    startkey blob,
                    |    chunkid bigint,
                    |    data blob,
                    |    PRIMARY KEY ((partition, version), columnname, segmentid, startkey, chunkid)
                    |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, version, segmentid, startkey, chunkid, columnname, data
      |) VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin
  )

  def writeChunks(partition: Types.BinaryPartition,
                  version: Int,
                  segmentId: Types.SegmentId,
                  startKey: Array[Byte],
                  chunkId: Types.ChunkID,
                  chunks: Map[String, ByteBuffer],
                  stats: ColumnStoreStats): Future[Response] = {
    val partBytes = toBuffer(partition)
    val segKeyBytes = toBuffer(segmentId)
    val startKeyBuf = ByteBuffer.wrap(startKey)
    var chunkBytes = 0L
    val statements = chunks.map { case (columnName, bytes) =>
      val finalBytes = compressChunk(bytes)
      chunkBytes += finalBytes.capacity.toLong
      writeChunksCql.bind(partBytes, version: jlInt, segKeyBytes, startKeyBuf,
                          chunkId: jlLong, columnName, finalBytes)
    }.toSeq
    stats.addChunkWriteStats(statements.length, chunkBytes)
    connector.execStmt(unloggedBatch(statements))
  }

  lazy val readChunkRangeCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnname = ? AND partition = ? AND version = ? AND segmentid = ?
                                  | AND (startkey, chunkid) >= (?, ?)
                                  | AND (startkey, chunkid) <= (?, ?)""".stripMargin)

  def readChunks(partition: Types.BinaryPartition,
                 version: Int,
                 column: String,
                 segmentId: Types.SegmentId,
                 key1: (Array[Byte], Types.ChunkID),
                 key2: (Array[Byte], Types.ChunkID)): Future[ChunkedData] = {
    val query = readChunkRangeCql.bind(column, toBuffer(partition), version: jlInt,
                                       toBuffer(segmentId),
                                       ByteBuffer.wrap(key1._1), key1._2: jlLong,
                                       ByteBuffer.wrap(key2._1), key2._2: jlLong)
    execReadChunk(column, query)
  }

  private def execReadChunk(column: String, query: BoundStatement): Future[ChunkedData] = {
    session.executeAsync(query).toScalaFuture.map { rs =>
      val rows = rs.all().asScala
      val byteVectorChunks = rows.map { row => (row.getLong(0),
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

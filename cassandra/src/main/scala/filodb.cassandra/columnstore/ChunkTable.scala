package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import net.jpountz.lz4.LZ4Factory
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

  private val lz4Factory = LZ4Factory.fastestInstance()
  private val compressor = lz4Factory.fastCompressor()
  private val decompressor = lz4Factory.fastDecompressor()
  private val sstableCompression = connector.config.getString("sstable-compression")
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
                  stats: ColumnStoreStats): Future[Response] = {
    val partBytes = toBuffer(partition)
    val segKeyBytes = toBuffer(segmentId)
    var chunkBytes = 0L
    val statements = chunks.map { case (columnName, id, bytes) =>
      val finalBytes = compress(bytes)
      chunkBytes += finalBytes.capacity.toLong
      writeChunksCql.bind(partBytes, version: java.lang.Integer, segKeyBytes,
                          id: java.lang.Integer, columnName, finalBytes)
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
                                                decompress(row.getBytes(2))) }
      ChunkedData(column, byteVectorChunks)
    }
  }

  private def compress(orig: ByteBuffer): ByteBuffer = {
    if (compressChunks) {
      // Fastest decompression method is when giving size of original bytes, so store that as first 4 bytes
      val compressedBytes = compressor.compress(orig.array)
      val newBuf = ByteBuffer.allocate(5 + compressedBytes.size)
      newBuf.put(compressBytePrefix)
      newBuf.putInt(orig.capacity)
      newBuf.put(compressedBytes)
      newBuf.position(0)
      newBuf
    } else {
      // The lowest first byte of a valid Filo vector will not be 0xFF.
      orig
    }
  }

  private def decompress(compressed: ByteBuffer): ByteBuffer = {
    // Is this compressed?
    if (compressed.get(0) == compressBytePrefix) {
      val origLength = compressed.getInt(1)
      ByteBuffer.wrap(decompressor.decompress(compressed.array, 5, origLength))
    } else {
      compressed
    }
  }
}

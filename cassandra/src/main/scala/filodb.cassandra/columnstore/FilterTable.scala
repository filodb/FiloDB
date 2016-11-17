package filodb.cassandra.columnstore

import bloomfilter.mutable.BloomFilter
import com.datastax.driver.core.Row
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.{ColumnStoreStats, SegmentState, compress, decompress}

/**
 * Represents the table which holds the Bloom filters for ingestion row key detection.
 */
sealed class FilterTable(val dataset: DatasetRef, val connector: FiloCassandraConnector)
                       (implicit ec: ExecutionContext) extends BaseDatasetTable {
  import filodb.cassandra.Util._
  import collection.JavaConverters._

  val suffix = "filters"

  // WITH COMPACT STORAGE saves 35% on storage costs according to this article:
  // http://blog.librato.com/posts/cassandra-compact-storage
  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                    |    partition blob,
                    |    version int,
                    |    segmentid blob,
                    |    chunkid bigint,
                    |    data blob,
                    |    PRIMARY KEY ((partition, version), segmentid, chunkid)
                    |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val readCql = session.prepare(
    s"SELECT chunkid, data FROM $tableString WHERE partition = ? AND version = ? AND segmentid = ? " +
    s"AND chunkid >= ? AND chunkid <= ?")

  def fromRow(row: Row): SegmentState.IDAndFilter = {
    val buffer = decompress(row.getBytes("data"))
    val bais = new ByteArrayInputStream(buffer.array)
    (row.getLong("chunkid"), BloomFilter.readFrom[Long](bais))
  }

  def readFilters(partition: Types.BinaryPartition,
                  version: Int,
                  segmentId: Types.SegmentId,
                  firstChunkId: Types.ChunkID,
                  lastChunkId: Types.ChunkID): Future[Iterator[SegmentState.IDAndFilter]] = {
    session.executeAsync(readCql.bind(toBuffer(partition),
                                      version: java.lang.Integer,
                                      toBuffer(segmentId),
                                      firstChunkId: java.lang.Long,
                                      lastChunkId: java.lang.Long))
           .toIterator.map(_.map(fromRow))
  }

  lazy val writeIndexCql = session.prepare(
    s"INSERT INTO $tableString (partition, version, segmentid, chunkid, data) " +
    "VALUES (?, ?, ?, ?, ?)")

  def writeFilters(partition: Types.BinaryPartition,
                   version: Int,
                   segmentId: Types.SegmentId,
                   filters: Seq[(Types.ChunkID, BloomFilter[Long])],
                   stats: ColumnStoreStats): Future[Response] = {
    var filterBytes = 0L
    val partitionBuf = toBuffer(partition)
    val segmentBuf = toBuffer(segmentId)
    val baos = new ByteArrayOutputStream()
    val statements = try {
      filters.map { case (chunkId, filter) =>
        baos.reset()
        filter.writeTo(baos)
        val filterBuf = compress(ByteBuffer.wrap(baos.toByteArray))
        filterBytes += filterBuf.capacity
        writeIndexCql.bind(partitionBuf,
                           version: java.lang.Integer,
                           segmentBuf,
                           chunkId: java.lang.Long,
                           filterBuf)
      }
    } finally {
      baos.close()
    }
    stats.addFilterWriteStats(filterBytes)
    connector.execStmt(unloggedBatch(statements))
  }
}
package filodb.cassandra.columnstore

import com.datastax.driver.core.{BoundStatement, Row}
import java.nio.ByteBuffer
import java.lang.{Long => jlLong, Integer => jlInt}
import monix.reactive.Observable
import scala.concurrent.{ExecutionContext, Future}
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.{ColumnStoreStats, ChunkSetInfo, SingleChunkInfo, compress, decompress}

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
                    |    chunkid bigint,
                    |    data blob,
                    |    PRIMARY KEY ((partition, version), columnname, chunkid)
                    |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, version, chunkid, columnname, data
      |) VALUES (?, ?, ?, ?, ?)""".stripMargin
  )

  def writeChunks(partition: Types.PartitionKey,
                  version: Int,
                  chunkId: Types.ChunkID,
                  chunks: Map[String, ByteBuffer],
                  stats: ColumnStoreStats): Future[Response] = {
    val partBytes = toBuffer(partition)
    var chunkBytes = 0L
    val statements = chunks.map { case (columnName, bytes) =>
      val finalBytes = compressChunk(bytes)
      chunkBytes += finalBytes.capacity.toLong
      writeChunksCql.bind(partBytes, version: jlInt, chunkId: jlLong, columnName, finalBytes)
    }.toSeq
    stats.addChunkWriteStats(statements.length, chunkBytes)
    connector.execStmt(unloggedBatch(statements))
  }

  lazy val readChunkInCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnname = ? AND partition = ? AND version = ?
                                  | AND chunkid IN ?""".stripMargin)

  lazy val readChunkRangeCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnname = ? AND partition = ? AND version = ?
                                  | AND chunkid >= ? AND chunkid <= ?""".stripMargin)

  def readChunks(partition: Types.PartitionKey,
                 version: Int,
                 column: String,
                 colNo: Int,
                 chunkIds: Seq[Types.ChunkID],
                 rangeQuery: Boolean = false): Observable[SingleChunkInfo] = {
    val query = if (rangeQuery) {
        readChunkRangeCql.bind(column, toBuffer(partition), version: jlInt,
                               chunkIds.head: jlLong, chunkIds.last: jlLong)
      } else {
        readChunkInCql.bind(column, toBuffer(partition), version: jlInt, chunkIds.asJava)
      }
    Observable.fromFuture(execReadChunk(colNo, query))
              .flatMap(it => Observable.fromIterator(it))
  }

  private def execReadChunk(colNo: Int,
                            query: BoundStatement): Future[Iterator[SingleChunkInfo]] = {
    session.executeAsync(query).toIterator.handleErrors.map { rowIt =>
      rowIt.map { row =>
        SingleChunkInfo(row.getLong(0), colNo, decompressChunk(row.getBytes(1)))
      }
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

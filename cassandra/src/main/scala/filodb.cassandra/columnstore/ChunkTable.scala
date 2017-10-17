package filodb.cassandra.columnstore

import com.datastax.driver.core.BoundStatement
import java.nio.ByteBuffer
import java.lang.{Long => jlLong, Integer => jlInt}
import monix.reactive.Observable
import scala.concurrent.{ExecutionContext, Future}

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.{ChunkSinkStats, SingleChunkInfo, compress, decompress}

/**
 * Represents the table which holds the actual columnar chunks
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
                    |    columnid int,
                    |    chunkid bigint,
                    |    data blob,
                    |    PRIMARY KEY (partition, columnid, chunkid)
                    |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, chunkid, columnid, data
      |) VALUES (?, ?, ?, ?)""".stripMargin
  )

  def writeChunks(partition: Types.PartitionKey,
                  chunkId: Types.ChunkID,
                  chunks: Seq[(Int, ByteBuffer)],
                  stats: ChunkSinkStats): Future[Response] = {
    val partBytes = toBuffer(partition)
    var chunkBytes = 0L
    val statements = chunks.map { case (columnId, bytes) =>
      val finalBytes = compressChunk(bytes)
      chunkBytes += finalBytes.capacity.toLong
      writeChunksCql.bind(partBytes, chunkId: jlLong, columnId: jlInt, finalBytes)
    }.toSeq
    stats.addChunkWriteStats(statements.length, chunkBytes)
    connector.execStmt(unloggedBatch(statements))
  }

  lazy val readChunkInCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnid = ? AND partition = ?
                                  | AND chunkid IN ?""".stripMargin)

  lazy val readChunkRangeCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnid = ? AND partition = ?
                                  | AND chunkid >= ? AND chunkid <= ?""".stripMargin)

  def readChunks(partition: Types.PartitionKey,
                 columnId: Int,
                 colPos: Int,
                 chunkIds: Seq[Types.ChunkID],
                 rangeQuery: Boolean = false): Observable[SingleChunkInfo] = {
    val query = if (rangeQuery) {
        readChunkRangeCql.bind(columnId: jlInt, toBuffer(partition),
                               chunkIds.head: jlLong, chunkIds.last: jlLong)
      } else {
        readChunkInCql.bind(columnId: jlInt, toBuffer(partition), chunkIds.asJava)
      }
    Observable.fromFuture(execReadChunk(colPos, query))
              .flatMap(it => Observable.fromIterator(it))
  }

  private def execReadChunk(colPos: Int,
                            query: BoundStatement): Future[Iterator[SingleChunkInfo]] = {
    session.executeAsync(query).toIterator.handleErrors.map { rowIt =>
      rowIt.map { row =>
        SingleChunkInfo(row.getLong(0), colPos, decompressChunk(row.getBytes(1)))
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

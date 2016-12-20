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
                    |    startkey blob,
                    |    chunkid bigint,
                    |    data blob,
                    |    PRIMARY KEY ((partition, version), columnname, startkey, chunkid)
                    |) WITH COMPACT STORAGE AND compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, version, startkey, chunkid, columnname, data
      |) VALUES (?, ?, ?, ?, ?, ?)""".stripMargin
  )

  def writeChunks(partition: Types.BinaryPartition,
                  version: Int,
                  startKey: Array[Byte],
                  chunkId: Types.ChunkID,
                  chunks: Map[String, ByteBuffer],
                  stats: ColumnStoreStats): Future[Response] = {
    val partBytes = toBuffer(partition)
    val startKeyBuf = ByteBuffer.wrap(startKey)
    var chunkBytes = 0L
    val statements = chunks.map { case (columnName, bytes) =>
      val finalBytes = compressChunk(bytes)
      chunkBytes += finalBytes.capacity.toLong
      writeChunksCql.bind(partBytes, version: jlInt, startKeyBuf,
                          chunkId: jlLong, columnName, finalBytes)
    }.toSeq
    stats.addChunkWriteStats(statements.length, chunkBytes)
    connector.execStmt(unloggedBatch(statements))
  }

  lazy val readChunkRangeCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnname = ? AND partition = ? AND version = ?
                                  | AND (startkey, chunkid) >= (?, ?)
                                  | AND (startkey, chunkid) <= (?, ?)""".stripMargin)

  def readChunks(partition: Types.BinaryPartition,
                 version: Int,
                 column: String,
                 colNo: Int,
                 infosAndSkips: Seq[(ChunkSetInfo, Array[Int])]): Observable[SingleChunkInfo] = {
    val firstInfo = infosAndSkips.head._1
    val lastInfo = infosAndSkips.last._1

    val query = readChunkRangeCql.bind(column, toBuffer(partition), version: jlInt,
                                       ByteBuffer.wrap(firstInfo.firstKey.toSortableBytes()), firstInfo.id: jlLong,
                                       ByteBuffer.wrap(lastInfo.firstKey.toSortableBytes()), lastInfo.id: jlLong)
    Observable.fromFuture(execReadChunk(infosAndSkips, colNo, query))
              .flatMap(it => Observable.fromIterator(it))
  }

  private def execReadChunk(infosAndSkips: Seq[(ChunkSetInfo, Array[Int])],
                            colNo: Int,
                            query: BoundStatement): Future[Iterator[SingleChunkInfo]] = {
    session.executeAsync(query).toIterator.map { rowIt =>
      // NOTE: this assumes that infosAndSkips occur in exact same order as chunks read back.
      // TODO: find a way to skip a chunk or something
      rowIt.zip(infosAndSkips.toIterator).map { case (row, (info, skips)) =>
        val chunkID = row.getLong(0)
        assert(chunkID == info.id)
        SingleChunkInfo(info, skips, colNo, decompressChunk(row.getBytes(1)))
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

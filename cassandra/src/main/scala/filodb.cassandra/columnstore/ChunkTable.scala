package filodb.cassandra.columnstore

import java.lang.{Integer => jlInt, Long => jlLong}
import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{BoundStatement, ConsistencyLevel}
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.{compress, decompress, ChunkSetInfo, ChunkSinkStats, SingleChunkInfo}

/**
 * Represents the table which holds the actual columnar chunks
 *
 * Data is stored in a columnar fashion similar to Parquet -- grouped by column.  Each
 * chunk actually stores many many rows grouped together into one binary chunk for efficiency.
 */
sealed class ChunkTable(val dataset: DatasetRef,
                        val connector: FiloCassandraConnector,
                        writeConsistencyLevel: ConsistencyLevel)
                       (implicit ec: ExecutionContext) extends BaseDatasetTable {
  import collection.JavaConverters._

  import filodb.cassandra.Util._

  val suffix = "chunks"

  private val compressChunks = connector.config.getBoolean("lz4-chunk-compress")

  val createCql = s"""CREATE TABLE IF NOT EXISTS $tableString (
                    |    partition blob,
                    |    columnid int,
                    |    chunkid bigint,
                    |    data blob,
                    |    PRIMARY KEY (partition, columnid, chunkid)
                    |) WITH compression = {
                    'sstable_compression': '$sstableCompression'}""".stripMargin

  lazy val writeChunksCql = session.prepare(
    s"""INSERT INTO $tableString (partition, chunkid, columnid, data
      |) VALUES (?, ?, ?, ?) USING TTL ?""".stripMargin
  ).setConsistencyLevel(writeConsistencyLevel)

  def writeChunks(partition: Array[Byte],
                  chunkInfo: ChunkSetInfo,
                  chunks: Seq[ByteBuffer],
                  stats: ChunkSinkStats,
                  diskTimeToLive: Int): Future[Response] = {
    val partBytes = toBuffer(partition)
    var chunkBytes = 0L
    val statements = chunks.zipWithIndex.map { case (bytes, columnId) =>
      val finalBytes = compressChunk(bytes)
      chunkBytes += finalBytes.capacity.toLong
      writeChunksCql.bind(partBytes, chunkInfo.id: jlLong, columnId: jlInt, finalBytes, diskTimeToLive: jlInt)
    }
    stats.addChunkWriteStats(statements.length, chunkBytes, chunkInfo.numRows)
    connector.execStmtWithRetries(unloggedBatch(statements).setConsistencyLevel(writeConsistencyLevel))
  }

  lazy val readChunkInCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnid = ? AND partition = ?
                                  | AND chunkid IN ?""".stripMargin)
                              .setConsistencyLevel(ConsistencyLevel.ONE)

  lazy val readChunkRangeCql = session.prepare(
                                 s"""SELECT chunkid, data FROM $tableString WHERE
                                  | columnid = ? AND partition = ?
                                  | AND chunkid >= ? AND chunkid <= ?""".stripMargin)
                              .setConsistencyLevel(ConsistencyLevel.ONE)

  def readChunks(partition: Array[Byte],
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

  // Compressed format: see filodb.core.store.compress (package.scala), but basically
  // the original 4-byte length header with bit 31 set, then compressed bytes.
  private def compressChunk(orig: ByteBuffer): ByteBuffer =
    if (compressChunks) compress(orig) else orig

  private def decompressChunk(compressed: ByteBuffer): ByteBuffer = {
    compressed.get(3) match {
      case b if b < 0 => decompress(compressed)
      case b          => compressed
    }
  }
}

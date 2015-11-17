package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import filodb.coordinator.Response

import scala.concurrent.Future
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._

case class ChunkRowMapRecord(segmentId: Types.SegmentId,
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
sealed class ChunkRowMapTable(dataset: String, val config: Config)
extends CassandraTable[ChunkRowMapTable, ChunkRowMapRecord]
with FiloCassandraConnector {
  import filodb.cassandra.Util._
  import scala.collection.JavaConversions._

  override val tableName = dataset + "_chunkmap"

  //scalastyle:off
  object partition extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PartitionKey[Int]
  object segmentId extends BlobColumn(this) with PrimaryKey[ByteBuffer]
  object chunkIds extends BlobColumn(this)
  object rowNums extends BlobColumn(this)
  object nextChunkId extends IntColumn(this)
  // Keeping below to remember how to define a set column, but move it elsewhere.
  // object columnsWritten extends SetColumn[ChunkRowMapTable, ChunkRowMapRecord, String](this)
  //scalastyle:on

  override def fromRow(row: Row): ChunkRowMapRecord =
    ChunkRowMapRecord(ByteVector(segmentId(row)), chunkIds(row), rowNums(row), nextChunkId(row))

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  /**
   * Retrieves a whole series of chunk maps, in the range [startSegmentId, untilSegmentId)
   * @return ChunkMaps(...), if nothing found will return ChunkMaps(Nil).
   */
  def getChunkMaps(partition: String,
                   version: Int,
                   startSegmentId: Types.SegmentId,
                   untilSegmentId: Types.SegmentId): Future[Seq[ChunkRowMapRecord]] =
    select.where(_.partition eqs partition)
          .and(_.version eqs version)
          .and(_.segmentId gte startSegmentId.toByteBuffer)
          .and(_.segmentId lt untilSegmentId.toByteBuffer)
          .fetch()

  def scanChunkMaps(version: Int,
                    startToken: String,
                    endToken: String): Future[Iterator[(String, ChunkRowMapRecord)]] = {
    val tokenQ = "TOKEN(partition, version)"
    val cql = s"SELECT * FROM ${keySpace.name}.$tableName WHERE " +
              s"$tokenQ >= $startToken AND $tokenQ < $endToken"
    Future {
      session.execute(cql).iterator
             .filter(this.version(_) == version)
             .map { row => (partition(row), fromRow(row)) }
    }
  }

  /**
   * Writes a new chunk map to the chunkRowTable.
   * @return Success, or an exception as a Future.failure
   */
  def writeChunkMap(partition: String,
                    version: Int,
                    segmentId: Types.SegmentId,
                    chunkIds: ByteBuffer,
                    rowNums: ByteBuffer,
                    nextChunkId: Int): Future[Response] =
    insert.value(_.partition, partition)
          .value(_.version,   version)
          .value(_.segmentId, segmentId.toByteBuffer)
          .value(_.chunkIds,  chunkIds)
          .value(_.rowNums,   rowNums)
          .value(_.nextChunkId, nextChunkId)
          .future().toResponse()
}

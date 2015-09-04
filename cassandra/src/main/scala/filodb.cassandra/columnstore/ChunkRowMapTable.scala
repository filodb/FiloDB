package filodb.cassandra

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.messages._

case class ChunkRowMapRecord(segmentId: ByteBuffer,
                             chunkIds: ByteBuffer,
                             rowNums: ByteBuffer,
                             nextChunkId: Int)

/**
 * Represents the table which holds the ChunkRowMap for each segment of a partition.
 * This maps sort keys in sorted order to chunks and row number within each chunk.
 * The ChunkRowMap is written as two Filo binary vectors.
 */
sealed class ChunkRowMapTable(dataset: String, config: Config)
extends CassandraTable[ChunkRowMapTable, ChunkRowMapRecord]
with SimpleCassandraConnector {
  import Util._

  override val tableName = dataset + "_chunkmap"
  // TODO: keySpace and other things really belong to a trait
  implicit val keySpace = KeySpace(config.getString("keyspace"))

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
    ChunkRowMapRecord(segmentId(row), chunkIds(row), rowNums(row), nextChunkId(row))

  /**
   * Retrieves a whole series of chunk maps, in the range [startSegmentId, untilSegmentId)
   * @returns ChunkMaps(...), if nothing found will return ChunkMaps(Nil).
   */
  def getChunkMaps(partition: String,
                   version: Int,
                   startSegmentId: ByteBuffer,
                   untilSegmentId: ByteBuffer): Future[Seq[ChunkRowMapRecord]] =
    select.where(_.partition eqs partition)
          .and(_.version eqs version)
          .and(_.segmentId gte startSegmentId).and(_.segmentId lt untilSegmentId)
          .fetch()

  /**
   * Writes a new chunk map to the chunkRowTable.
   * @returns Success, or an exception as a Future.failure
   */
  def writeChunkMap(partition: String,
                    version: Int,
                    segmentId: ByteBuffer,
                    chunkIds: ByteBuffer,
                    rowNums: ByteBuffer,
                    nextChunkId: Int): Future[Response] =
    insert.value(_.partition, partition)
          .value(_.version,   version)
          .value(_.segmentId, segmentId)
          .value(_.chunkIds,  chunkIds)
          .value(_.rowNums,   rowNums)
          .value(_.nextChunkId, nextChunkId)
          .future().toResponseOnly()
}

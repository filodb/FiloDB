package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.messages._

case class ChunkRowMapRecord(segmentId: ByteBuffer,
                           chunkIds: ByteBuffer,
                           rowNums: ByteBuffer,
                           columns: Set[String]) extends Response

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
  object columnsWritten extends SetColumn[ChunkRowMapTable, ChunkRowMapRecord, String](this)
  //scalastyle:on

  override def fromRow(row: Row): ChunkRowMapRecord =
    ChunkRowMapRecord(segmentId(row), chunkIds(row), rowNums(row), columnsWritten(row))

  def getChunkMap(partition: String, version: Int, segmentId: ByteBuffer): Future[Response] =
    select.where(_.partition eqs partition)
          .and(_.version eqs version)
          .and(_.segmentId eqs segmentId)
          .one().map { opt => opt.getOrElse(NotFound) }
          .handleErrors

  def writeChunkMap(partition: String,
                    version: Int,
                    segmentId: ByteBuffer,
                    chunkIds: ByteBuffer,
                    rowNums: ByteBuffer): Future[Response] =
    insert.value(_.partition, partition)
          .value(_.version,   version)
          .value(_.segmentId, segmentId)
          .value(_.chunkIds,  strictBytes(chunkIds))
          .value(_.rowNums,   strictBytes(rowNums))
          .future().toResponse()
}

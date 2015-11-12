package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
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
  object uuid extends LongColumn(this) with StaticColumn[Long]
  object segmentId extends BlobColumn(this) with PrimaryKey[ByteBuffer]
  object endKey extends BlobColumn(this)
  object numRows extends IntColumn(this)
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

  def readSegmentInfos(partition: String,
                       version: Int): Future[Seq[(ByteBuffer, ByteBuffer, Int)]] =
    select(_.segmentId, _.endKey, _.numRows)
          .where(_.partition eqs partition)
          .and(_.version eqs version)
          .fetch

  def readUuid(partition: String, version: Int): Future[Option[Long]] =
    select(_.uuid).where(_.partition eqs partition).and(_.version eqs version).one

  // This is an atomic operation which would fail if uuid does not get replaced, so use logged batches.
  // We cannot have partial updates at all.
  // Logged batches are a perf hit, but should be OK since this is updated only at beginning of every flush.
  def updateSegmentInfos(partition: String,
                         version: Int,
                         oldUuid: Long,
                         newUuid: Long,
                         infos: Seq[(ByteBuffer, ByteBuffer, Int)]): Future[Response] = {
    val insertQ = insert.value(_.partition,  partition)
                        .value(_.version,    version)

    def getBatch(): Future[Response] = {
      val batch = Batch.logged.add(update.where(_.partition eqs partition)
                                         .and(_.version eqs version)
                                         .modify(_.uuid setTo newUuid)
                                         .onlyIf(_.uuid is oldUuid))
      val batch2 = infos.foldLeft(batch) {
        case (batch, (segmentId, endKey, numRows)) =>
          batch.add(insertQ.value(_.segmentId, segmentId)
                           .value(_.endKey, endKey)
                           .value(_.numRows, numRows))
      }
      batch2.future().toResponse()
    }

    // First, insert a 0 into the UUID if the partition/version doesn't exist.  The CAS doesn't work without
    // an existing value.  Then, try the CAS.
    for { preInsertResp <- insertQ.value(_.uuid, 0L).ifNotExists.future()
          batchResp     <- getBatch } yield { batchResp }
  }
}

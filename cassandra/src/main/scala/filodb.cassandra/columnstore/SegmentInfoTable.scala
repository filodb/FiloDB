package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import scala.concurrent.Future
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._

/**
 * Represents the table which holds segment information (start, end, numRows).  Basically used to
 * synchronize segment boundaries amongst every writer, and should be very small and fast to read/write.
 * This needed to be a separate table from the ChunkRowMapTable because it has different update semantics
 * (atomic / transactional), is read and written at different time than the index, and needs to be very fast
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class SegmentInfoTable(dataset: String, val config: Config)
extends CassandraTable[SegmentInfoTable, (ByteVector, ByteVector, Int)]
with FiloCassandraConnector {
  import filodb.cassandra.Util._
  import scala.collection.JavaConversions._

  override val tableName = dataset + "_segmentinfo"

  //scalastyle:off
  object partition extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PartitionKey[Int]
  object uuid extends LongColumn(this) with StaticColumn[Long]
  object segmentId extends BlobColumn(this) with PrimaryKey[ByteBuffer]
  object info extends BlobColumn(this)

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  override def fromRow(row: Row): (ByteVector, ByteVector, Int) = rawToVectors(segmentId(row), info(row))

  private def rawToVectors(segmentId: ByteBuffer, info: ByteBuffer): (ByteVector, ByteVector, Int) = {
    val infoVector = ByteVector(info)
    val numRows = IntKeyHelper.fromBytes(infoVector.take(4))
    (ByteVector(segmentId), infoVector.drop(4), numRows)
  }

  private def toInfo(endKey: ByteVector, numRows: Int): ByteBuffer =
    (IntKeyHelper.toBytes(numRows) ++ endKey).toByteBuffer

  def readSegmentInfos(partition: String,
                       version: Int): Future[Seq[(ByteVector, ByteVector, Int)]] =
    select(_.segmentId, _.info)
          .where(_.partition eqs partition)
          .and(_.version eqs version)
          .fetch.map { _.map { case (segId, info) => rawToVectors(segId, info) } }

  def readUuid(partition: String, version: Int): Future[Option[Long]] =
    select(_.uuid).where(_.partition eqs partition).and(_.version eqs version).one

  // This is an atomic operation which would fail if uuid does not get replaced, so use logged batches.
  // We cannot have partial updates at all.
  // Logged batches are a perf hit, but should be OK since this is updated only at beginning of every flush.
  def updateSegmentInfos(partition: String,
                         version: Int,
                         oldUuid: Long,
                         newUuid: Long,
                         infos: Seq[(ByteVector, ByteVector, Int)]): Future[Response] = {
    val insertQ = insert.value(_.partition,  partition)
                        .value(_.version,    version)

    def getBatch(): Future[Response] = {
      val batch = Batch.logged.add(update.where(_.partition eqs partition)
                                         .and(_.version eqs version)
                                         .modify(_.uuid setTo newUuid)
                                         .onlyIf(_.uuid is oldUuid))
      val batch2 = infos.foldLeft(batch) {
        case (batch, (segId, endKey, numRows)) =>
          batch.add(insertQ.value(_.segmentId, segId.toByteBuffer)
                           .value(_.info,      toInfo(endKey, numRows)))
      }
      batch2.future().toResponse()
    }

    // First, insert a 0 into the UUID if the partition/version doesn't exist.  The CAS doesn't work without
    // an existing value.  Then, try the CAS.
    for { preInsertResp <- insertQ.value(_.uuid, 0L).ifNotExists.future()
          batchResp     <- getBatch } yield { batchResp }
  }
}
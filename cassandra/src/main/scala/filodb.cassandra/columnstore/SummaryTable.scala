package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import com.datastax.driver.core.Session
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import filodb.core.Messages.Response
import filodb.core.metadata.{Projection, SegmentSummary}
import filodb.core.util.{FiloLogging, MemoryPool}

import scala.concurrent.Future

sealed class SummaryTable(ks: KeySpace, _session: Session)
  extends CassandraTable[SummaryTable, (String, java.util.UUID, ByteBuffer)]
  with MemoryPool with FiloLogging{


  import filodb.cassandra.Util._

  implicit val keySpace = ks
  implicit val session = _session

  //scalastyle:off

  object dataset extends StringColumn(this) with PartitionKey[String]

  object projectionId extends IntColumn(this) with PartitionKey[Int]

  object partition extends BlobColumn(this) with PartitionKey[ByteBuffer]

  object segmentId extends StringColumn(this) with PrimaryKey[String]

  object segmentVersion extends TimeUUIDColumn(this)

  object chunkSummaries extends BlobColumn(this)

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def readSummary(projection: Projection,
                  partition: ByteBuffer,
                  segmentId: String): Future[Option[(java.util.UUID, SegmentSummary)]] = {
    val segmentType = projection.segmentType
    val query = select(_.segmentVersion, _.chunkSummaries)
      .where(_.dataset eqs projection.dataset)
      .and(_.projectionId eqs projection.id)
      .and(_.partition eqs partition)
      .and(_.segmentId eqs segmentId)
    for {
      result <- query.one()
      summary = result.map { case (version, buf) => (version, SegmentSummary.read(segmentType, buf)) }
    } yield summary
  }

  def compareAndSetSummary(projection: Projection,
                           partition: ByteBuffer,
                           segmentId: String,
                           oldVersion: Option[java.util.UUID],
                           segmentVersion: java.util.UUID,
                           segmentSummary: SegmentSummary): Future[Response] = {
    oldVersion match {
      case None =>
        insertSummary(projection, partition, segmentId, segmentVersion, segmentSummary)
      case Some(v) =>
        updateSummary(projection, partition, segmentId, v, segmentVersion, segmentSummary)
    }
  }

  def updateSummary(projection: Projection,
                    partition: ByteBuffer,
                    segmentId: String,
                    oldVersion: java.util.UUID,
                    newVersion: java.util.UUID,
                    segmentSummary: SegmentSummary): Future[Response] = {
    val segmentSummarySize = segmentSummary.size
    metrics.debug(s"Acquiring buffer of size $segmentSummarySize for SegmentSummary")
    val ssBytes = acquire(segmentSummarySize)
    SegmentSummary.write(ssBytes,segmentSummary)
    val updateQuery = update.where(_.dataset eqs projection.dataset)
      .and(_.projectionId eqs projection.id)
      .and(_.partition eqs partition)
      .and(_.segmentId eqs segmentId)
      .modify(_.segmentVersion setTo newVersion)
      .and(_.chunkSummaries setTo ssBytes)
      .onlyIf(_.segmentVersion is oldVersion)
    updateQuery.future.map { f =>
      release(ssBytes)
      f
    }.toResponse()
  }

  def insertSummary(projection: Projection,
                    partition: ByteBuffer,
                    segmentId: String,
                    segmentVersion: UUID,
                    segmentSummary: SegmentSummary): Future[Response] = {
    val segmentSummarySize = segmentSummary.size
    metrics.debug(s"Acquiring buffer of size $segmentSummarySize for SegmentSummary")
    val ssBytes = acquire(segmentSummarySize)
    SegmentSummary.write(ssBytes,segmentSummary)
    insert.value(_.dataset, projection.dataset)
      .value(_.projectionId, projection.id)
      .value(_.partition, partition)
      .value(_.segmentId, segmentId)
      .value(_.segmentVersion, segmentVersion)
      .value(_.chunkSummaries, ssBytes)
      .ifNotExists().future().map { f =>
      release(ssBytes)
      f
    }.toResponse()
  }


  override def fromRow(r: Row): (String, UUID, ByteBuffer) = {
    (segmentId(r), segmentVersion(r), chunkSummaries(r))
  }
}

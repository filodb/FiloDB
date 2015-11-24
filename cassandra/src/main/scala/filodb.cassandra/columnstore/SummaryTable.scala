package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import com.typesafe.config.Config
import com.websudos.phantom.CassandraTable
import com.websudos.phantom.dsl._
import filodb.cassandra.FiloCassandraConnector
import filodb.core.Messages.Response
import filodb.core.metadata.{Projection, SegmentSummary}

import scala.concurrent.Future

sealed class SummaryTable(val config: Config)
  extends CassandraTable[SummaryTable, (String, java.util.UUID, ByteBuffer)]
  with FiloCassandraConnector {

  import filodb.cassandra.Util._

  //scalastyle:off

  object dataset extends StringColumn(this) with PartitionKey[String]

  object projectionId extends IntColumn(this) with PartitionKey[Int]

  object partition extends BlobColumn(this) with PartitionKey[ByteBuffer]

  object segmentId extends StringColumn(this) with PrimaryKey[String]

  object segmentVersion extends TimeUUIDColumn(this)

  object chunkSummaries extends BlobColumn(this)


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
      summary = result.map { case (version, buf) => (version, SegmentSummary.fromBytes(segmentType, buf)) }
    } yield summary
  }

  def compareAndSetSummary(projection: Projection,
                           partition: ByteBuffer,
                           segmentId: String,
                           oldVersion: java.util.UUID,
                           segmentVersion: java.util.UUID,
                           segmentSummary: SegmentSummary): Future[Response] = {
    if (oldVersion == null) {
      insertSummary(projection, partition, segmentId, segmentVersion, segmentSummary)
    } else {
      updateSummary(projection, partition, segmentId, oldVersion, segmentVersion, segmentSummary)
    }
  }

  def updateSummary(projection: Projection,
                    partition: ByteBuffer,
                    segmentId: String,
                    oldVersion: java.util.UUID,
                    newVersion: java.util.UUID,
                    segmentSummary: SegmentSummary): Future[Response] = {
    val updateQuery = update.where(_.dataset eqs projection.dataset)
      .and(_.projectionId eqs projection.id)
      .and(_.partition eqs partition)
      .and(_.segmentId eqs segmentId)
      .modify(_.segmentVersion setTo newVersion)
      .and(_.chunkSummaries setTo segmentSummary.toBytes)
      .onlyIf(_.segmentVersion is oldVersion)
    updateQuery.future.toResponse()
  }

  def insertSummary(projection: Projection,
                    partition: ByteBuffer,
                    segmentId: String,
                    segmentVersion: UUID,
                    segmentSummary: SegmentSummary): Future[Response] = {
    insert.value(_.dataset, projection.dataset)
      .value(_.projectionId, projection.id)
      .value(_.partition, partition)
      .value(_.segmentId, segmentId)
      .value(_.segmentVersion, segmentVersion)
      .value(_.chunkSummaries, segmentSummary.toBytes)
      .ifNotExists().future().toResponse()
  }


  override def fromRow(r: Row): (String, UUID, ByteBuffer) = {
    (segmentId(r), segmentVersion(r), chunkSummaries(r))
  }
}

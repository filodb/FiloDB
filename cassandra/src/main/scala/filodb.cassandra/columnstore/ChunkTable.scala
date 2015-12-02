package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import com.datastax.driver.core.Session
import com.websudos.phantom.connectors.KeySpace
import com.websudos.phantom.dsl._
import filodb.core.Messages.Response
import filodb.core.Types.ChunkId
import filodb.core.metadata.Projection

import scala.concurrent.Future

/**
 * Represents the table which holds the actual columnar chunks for segments of a projection.
 * Each row stores data for a column (chunk) of a segment.
 *
 */
sealed class ChunkTable(ks: KeySpace, _session: Session)
  extends CassandraTable[ChunkTable, (String, Int, String, ByteBuffer)] {


  import filodb.cassandra.Util._

  implicit val keySpace = ks
  implicit val session = _session

  //scalastyle:off

  object partition extends BlobColumn(this) with PartitionKey[ByteBuffer]

  object dataset extends StringColumn(this) with PrimaryKey[String]

  object projectionId extends IntColumn(this) with PrimaryKey[Int]

  object columnName extends StringColumn(this) with PrimaryKey[String]

  object segmentId extends StringColumn(this) with PrimaryKey[String]

  object chunkId extends IntColumn(this) with PrimaryKey[Int]

  object data extends BlobColumn(this)

  //scalastyle:on

  def initialize():Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll():Future[Response] = truncate.future().toResponse()

  def writeChunks(projection: Projection,
                  partition: ByteBuffer,
                  columnName: String,
                  segmentId: String,
                  chunkId: ChunkId,
                  data: ByteBuffer): Future[Response] = {
    val insertQ = insert.value(_.dataset, projection.dataset)
      .value(_.projectionId, projection.id)
      .value(_.partition, partition)
      .value(_.columnName, columnName)
      .value(_.segmentId, segmentId)
      .value(_.chunkId, chunkId)
      .value(_.data, data)
    insertQ.future().toResponse()
  }

  def getDataBySegmentAndChunk(projection: Projection,
                   partition: ByteBuffer,
                   columnName: String,
                   segmentStart: String,
                   segmentEnd: String): Future[Map[(String, Int), ByteBuffer]] = {
    for {
      result <- getChunkData(projection,partition,columnName,segmentStart,segmentEnd)
      // there would be a unique combination of segmentId and chunkId.
      // so we just pick up the only value
      data = result.groupBy(i => Tuple2(i._1, i._2)).mapValues(l => l.head._3)
    } yield data
  }

  def getChunkData(projection: Projection,
                   partition: ByteBuffer,
                   columnName: String,
                   segmentStart: String,
                   segmentEnd: String): Future[Seq[(String, Int, ByteBuffer)]] = {
      select(_.segmentId, _.chunkId, _.data)
        .where(_.dataset eqs projection.dataset)
        .and(_.projectionId eqs projection.id)
        .and(_.partition eqs partition)
        .and(_.columnName eqs columnName)
        .and(_.segmentId gte segmentStart)
        .and(_.segmentId lte segmentEnd).fetch()
  }

  def getColumnData(projection: Projection,
                    partition: ByteBuffer,
                    columnName: String,
                    segmentId: String,
                    chunkIds: List[Int]): Future[Seq[(ChunkId, ByteBuffer)]] =
    select(_.chunkId, _.data)
      .where(_.dataset eqs projection.dataset)
      .and(_.projectionId eqs projection.id)
      .and(_.partition eqs partition)
      .and(_.columnName eqs columnName)
      .and(_.segmentId eqs segmentId)
      .and(_.chunkId in chunkIds).fetch()


  override def fromRow(r: Row): (String, Int, String, ByteBuffer) = {
    (segmentId(r), chunkId(r), columnName(r), data(r))
  }
}

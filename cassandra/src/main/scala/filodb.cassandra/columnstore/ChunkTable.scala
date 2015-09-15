package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core._
import filodb.core.columnstore.ChunkedData

/**
 * Represents the table which holds the actual columnar chunks for segments
 *
 * Data is stored in a columnar fashion similar to Parquet -- grouped by column.  Each
 * chunk actually stores many many rows grouped together into one binary chunk for efficiency.
 */
sealed class ChunkTable(dataset: String, config: Config)
extends CassandraTable[ChunkTable, (String, ByteBuffer, Int, ByteBuffer)]
with SimpleCassandraConnector {
  import filodb.cassandra.Util._

  override val tableName = dataset + "_chunks"
  // TODO: keySpace and other things really belong to a trait
  implicit val keySpace = KeySpace(config.getString("keyspace"))

  //scalastyle:off
  object partition extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PartitionKey[Int]
  object columnName extends StringColumn(this) with PrimaryKey[String]
  object segmentId extends BlobColumn(this) with PrimaryKey[ByteBuffer]
  object chunkId extends IntColumn(this) with PrimaryKey[Int]
  object data extends BlobColumn(this)
  //scalastyle:on

  override def fromRow(row: Row): (String, ByteBuffer, Int, ByteBuffer) =
    (columnName(row), segmentId(row), chunkId(row), data(row))

  def initialize(): Future[Response] = create.ifNotExists.future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def writeChunks(partition: String,
                  version: Int,
                  segmentId: ByteBuffer,
                  chunks: Iterator[(String, Types.ChunkID, ByteBuffer)]): Future[Response] = {
    val insertQ = insert.value(_.partition,  partition)
                        .value(_.version,    version)
                        .value(_.segmentId,  segmentId)
    // NOTE: This is actually a good use of Unlogged Batch, because all of the inserts
    // are to the same partition key, so they will get collapsed down into one insert
    // for efficiency.
    // NOTE2: the batch add is immutable, so use foldLeft to get the updated batch
    val batch = chunks.foldLeft(Batch.unlogged) {
      case (batch, (columnName, id, bytes)) =>
        batch.add(insertQ.value(_.chunkId, id)
                         .value(_.columnName, columnName)
                         .value(_.data, bytes))
    }
    batch.future().toResponse()
  }

  // Reads back all the chunks from the requested column for the segments falling within
  // the starting and ending segment IDs.  No paging is performed - so be sure to not
  // ask for too large of a range.  Also, beware the starting segment ID must line up with the
  // segment boundary, and the ending segment ID is exclusive!
  def readChunks(partition: String,
                 version: Int,
                 column: String,
                 startSegmentId: ByteBuffer,
                 untilSegmentId: ByteBuffer): Future[ChunkedData] =
    select(_.segmentId, _.chunkId, _.data).where(_.columnName eqs column)
      .and(_.partition eqs partition)
      .and(_.version eqs version)
      .and(_.segmentId gte startSegmentId).and(_.segmentId lt untilSegmentId)
      .fetch().map(chunks => ChunkedData(column, chunks))
}

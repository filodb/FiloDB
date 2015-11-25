package filodb.cassandra.columnstore

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import scala.concurrent.Future
import scodec.bits._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.columnstore.ChunkedData

/**
 * Represents the table which holds the actual columnar chunks for segments
 *
 * Data is stored in a columnar fashion similar to Parquet -- grouped by column.  Each
 * chunk actually stores many many rows grouped together into one binary chunk for efficiency.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class ChunkTable(dataset: String, val config: Config)
extends CassandraTable[ChunkTable, (String, Types.SegmentId, Int, ByteBuffer)]
with FiloCassandraConnector {
  import filodb.cassandra.Util._

  override val tableName = dataset + "_chunks"

  //scalastyle:off
  object partition extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PartitionKey[Int]
  object columnName extends StringColumn(this) with PrimaryKey[String]
  object segmentId extends BlobColumn(this) with PrimaryKey[ByteBuffer]
  object chunkId extends IntColumn(this) with PrimaryKey[Int]
  object data extends BlobColumn(this)
  //scalastyle:on

  override def fromRow(row: Row): (String, Types.SegmentId, Int, ByteBuffer) =
    (columnName(row), ByteVector(segmentId(row)), chunkId(row), data(row))

  // WITH COMPACT STORAGE saves 35% on storage costs according to this article:
  // http://blog.librato.com/posts/cassandra-compact-storage
  def initialize(): Future[Response] = create.ifNotExists.option(Storage.CompactStorage)
                                             .future().toResponse()

  def clearAll(): Future[Response] = truncate.future().toResponse()

  def writeChunks(partition: String,
                  version: Int,
                  segmentId: Types.SegmentId,
                  chunks: Iterator[(String, Types.ChunkID, ByteBuffer)]): Future[Response] = {
    val insertQ = insert.value(_.partition,  partition)
                        .value(_.version,    version)
                        .value(_.segmentId,  segmentId.toByteBuffer)
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
  // segment boundary.
  // endExclusive indicates if the end segment ID is exclusive or not.
  def readChunks(partition: String,
                 version: Int,
                 column: String,
                 startSegmentId: Types.SegmentId,
                 untilSegmentId: Types.SegmentId,
                 endExclusive: Boolean = true): Future[ChunkedData] = {
    val initialQuery =
      select(_.segmentId, _.chunkId, _.data).where(_.columnName eqs column)
        .and(_.partition eqs partition)
        .and(_.version eqs version)
        .and(_.segmentId gte startSegmentId.toByteBuffer)
    val wholeQuery = if (endExclusive) { initialQuery.and(_.segmentId lt untilSegmentId.toByteBuffer) }
                     else              { initialQuery.and(_.segmentId lte untilSegmentId.toByteBuffer) }
    wholeQuery.fetch().map { chunks =>
      val byteVectorChunks = chunks.map { case (seg, chunkId, data) => (ByteVector(seg), chunkId, data) }
      ChunkedData(column, byteVectorChunks)
    }
  }
}

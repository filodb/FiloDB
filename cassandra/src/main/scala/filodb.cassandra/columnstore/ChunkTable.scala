package filodb.cassandra.columnstore

import java.nio.ByteBuffer

import com.typesafe.config.Config
import com.websudos.phantom.builder.MultiColumnInClause
import com.websudos.phantom.dsl._
import filodb.cassandra.FiloCassandraConnector
import filodb.core.Messages.Response
import filodb.core.metadata.{SimpleChunk, ChunkWithMeta, Projection}

import scala.concurrent.Future

/**
 * Represents the table which holds the actual columnar chunks for segments of a projection.
 * Each row stores data for a column (chunk) of a segment.
 *
 * @param config a Typesafe Config with hosts, port, and keyspace parameters for Cassandra connection
 */
sealed class ChunkTable(val config: Config)
  extends CassandraTable[ChunkTable, (String, Int, String, ByteBuffer)]
  with FiloCassandraConnector {

  import filodb.cassandra.Util._

  //scalastyle:off

  object dataset extends StringColumn(this) with PartitionKey[String]

  object projectionId extends IntColumn(this) with PartitionKey[Int]

  object partition extends BlobColumn(this) with PartitionKey[ByteBuffer]

  object segmentId extends StringColumn(this) with PrimaryKey[String]

  object columnName extends StringColumn(this) with PrimaryKey[String]

  object chunkId extends IntColumn(this) with PrimaryKey[Int]

  object numRows extends IntColumn(this)

  object chunkOverrides extends BlobColumn(this)

  object data extends BlobColumn(this)

  //scalastyle:on


  def initialize() = create.ifNotExists.future()

  def clearAll() = truncate.future()

  def writeChunks(projection: Projection, partition: ByteBuffer,
                  segmentId: String,
                  chunk: ChunkWithMeta): Future[Response] = {
    val insertQ = insert.value(_.dataset, projection.dataset)
      .value(_.projectionId, projection.id)
      .value(_.partition, partition)
      .value(_.segmentId, segmentId)
      .value(_.chunkId, chunk.chunkId)
      .value(_.chunkOverrides, SimpleChunk.chunkOverridesAsByteBuffer(chunk.chunkOverrides))
      .value(_.numRows, chunk.numRows)
    // NOTE: This is actually a good use of Unlogged Batch, because all of the inserts
    // are to the same partition key, so they will get collapsed down into one insert
    // for efficiency.
    // NOTE2: the batch add is immutable, so use foldLeft to get the updated batch
    val chunks = chunk.columnVectors.zipWithIndex
      .map { case (col, i) => projection.schema(i).name -> col }
    val batched = chunks.foldLeft(Batch.unlogged) {
      case (batch, (columnName, bytes)) =>
        batch.add(insertQ.value(_.columnName, columnName).value(_.data, bytes))
    }
    batched.future().toResponse()
  }

  // Reads back all the chunks from the requested column for the segments falling within
  // the starting and ending segment IDs.  No paging is performed - so be sure to not
  // ask for too large of a range.  Also, beware the starting segment ID must line up with the
  // segment boundary.
  // endExclusive indicates if the end segment ID is exclusive or not.
  def readChunksForSegmentRange(projection: Projection,
                                partition: ByteBuffer,
                                startSegmentId: String,
                                untilSegmentId: String,
                                columnNames: Seq[String],
                                endExclusive: Boolean = true): Future[Seq[(String, Seq[ChunkWithMeta])]] = {
    val initialQuery =
      select(_.segmentId, _.chunkId, _.columnName, _.data, _.chunkOverrides, _.numRows)
        .where(_.dataset eqs projection.dataset)
        .and(_.projectionId eqs projection.id)
        .and(_.partition eqs partition)
        .and(_.segmentId gte startSegmentId)
    val clusterQuery = if (endExclusive) {
      initialQuery.and(_.segmentId lt untilSegmentId)
    }
    else {
      initialQuery.and(_.segmentId lte untilSegmentId)
    }
    //restrict column names using in restriction.
    //will fetch these columns for all chunk ids in the segment
    val wholeQuery = clusterQuery.and(_.columnName in columnNames.toList)
    for {
      res <- wholeQuery.fetch()
      bySegment = res.groupBy(_._1)
      chunksBySegment = bySegment.map { case (segmentId, seq) =>
        val chunks = seq.groupBy(_._2).map { case (chunkId, chunkSeq) =>
          val (overrides, numRows) = (chunkSeq.head._5, chunkSeq.head._6)
          val namedVectors = chunkSeq.map { case (a, b, colName, data, c, d) => colName -> data }.toMap
          SimpleChunk(projection, chunkId, namedVectors, numRows, overrides)
        }.toSeq
        (segmentId, chunks)
      }.toSeq

    } yield chunksBySegment
  }

  def readChunks(projection: Projection,
                 partition: ByteBuffer,
                 segmentId: String,
                 columnNames: Seq[String],
                 chunkIds: Seq[Int]): Future[Seq[ChunkWithMeta]] = {
    val initialQuery =
      select(_.chunkId, _.columnName, _.data, _.chunkOverrides, _.numRows)
        .where(_.dataset eqs projection.dataset)
        .and(_.projectionId eqs projection.id)
        .and(_.partition eqs partition)
        .and(_.segmentId eqs segmentId)
    val values = columnNames.flatMap { col =>
      chunkIds.map(List(col, _))
    }.toList
    def inClause(table: ChunkTable) = MultiColumnInClause(List(table.columnName.name, table.chunkId.name), values).clause
    val wholeQuery = initialQuery.and(inClause)
    for {
      res <- wholeQuery.fetch()
      byChunk = res.groupBy(_._1)
      chunks = byChunk.map { case (chunkId, chunkSeq) =>
        val (overrides, numRows) = (chunkSeq.head._4, chunkSeq.head._5)
        val namedVectors = chunkSeq.map { case (b, colName, data, c, d) => colName -> data }.toMap
        SimpleChunk(projection, chunkId, namedVectors, numRows, overrides)
      }.toSeq
    } yield chunks
  }

  override def fromRow(r: Row): (String, Int, String, ByteBuffer) = {
    (segmentId(r), chunkId(r), columnName(r), data(r))
  }
}

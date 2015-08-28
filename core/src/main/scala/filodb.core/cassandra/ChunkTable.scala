package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import com.websudos.phantom.dsl._
import java.nio.ByteBuffer
import scala.concurrent.Future

import filodb.core.messages._
import filodb.core.datastore2.Types

/**
 * Represents the table which holds the actual columnar chunks for segments
 *
 * Data is stored in a columnar fashion similar to Parquet -- grouped by column.  Each
 * chunk actually stores many many rows grouped together into one binary chunk for efficiency.
 */
sealed class ChunkTable(dataset: String, config: Config)
extends CassandraTable[ChunkTable, (String, ByteBuffer, Long, ByteBuffer)]
with SimpleCassandraConnector {
  import Util._

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

  override def fromRow(row: Row): (String, ByteBuffer, Long, ByteBuffer) =
    (columnName(row), segmentId(row), chunkId(row), data(row))

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
                         .value(_.data, strictBytes(bytes)))
    }
    batch.future().toResponse()

  }
}

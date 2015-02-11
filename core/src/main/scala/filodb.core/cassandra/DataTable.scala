package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import com.websudos.phantom.query.{InsertQuery, SelectQuery, SelectWhere}
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import java.nio.ByteBuffer
import play.api.libs.iteratee.Iteratee
import scala.concurrent.Future

/**
 * Represents the "data" table which holds the actual columnar data for datasets.
 *
 * Data is stored in a columnar fashion similar to Parquet -- grouped by column, and then rowId.  Each
 * "rowId" actually stores many many rows grouped together into one binary chunk for efficiency.
 *
 * The static columns are needed to quickly discover:
 * 1) What are the actual columns written here?  (may be a subset of the schema) (important for reads)
 */
sealed class DataTable extends CassandraTable[DataTable, (String, Long, ByteBuffer)] {
  //scalastyle:off
  object dataset extends StringColumn(this) with PartitionKey[String]
  object version extends IntColumn(this) with PartitionKey[Int]
  object partition extends StringColumn(this) with PartitionKey[String]
  object firstRowId extends LongColumn(this) with PartitionKey[Long]
  object columnName extends StringColumn(this) with PrimaryKey[String]
  object rowId extends LongColumn(this) with PrimaryKey[Long]
  object columnsWritten extends StringColumn(this) with StaticColumn[String]
  object data extends BlobColumn(this)
  //scalastyle:on

  override def fromRow(row: Row): DataTable.ColRowBytes =
    (columnName(row), rowId(row), data(row))
}

/**
 * Asynchronous methods to operate on data table.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object DataTable extends DataTable with SimpleCassandraConnector {
  override val tableName = "data"

  // The tuple type returned by the low level readColumns API in the Enumerator
  type ColRowBytes = (String, Long, ByteBuffer)

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  val keySpace = "test"

  import Util._
  import filodb.core.messages._
  import filodb.core.metadata.Shard
  import filodb.core.metadata.Shard._

  def insertQuery(shard: Shard): InsertQuery[DataTable, ColRowBytes] =
    insert.value(_.dataset,    shard.partition.dataset)
          .value(_.version,    shard.version)
          .value(_.partition,  shard.partition.partition)
          .value(_.firstRowId, shard.firstRowId)

  def whereShard(s: SelectQuery[DataTable, ColRowBytes], shard: Shard): SelectWhere[DataTable, ColRowBytes] =
    s.where(_.dataset eqs shard.partition.dataset)
     .and(_.version eqs shard.version)
     .and(_.partition eqs shard.partition.partition)
     .and(_.firstRowId eqs shard.firstRowId)

  /**
   * Inserts one chunk of data from different columns.
   * Checks that the rowIdRange is aligned with the chunkSize.
   * @param shard the Shard to write to
   * @param rowId the starting rowId for the chunk. Must be aligned to chunkSize.
   * @param lastSequenceNo the ending sequence # for the chunk, will be reported back in the Ack
   * @param columnsBytes the column name and bytes to be written for each column
   * @returns Ack(), or ChunkMisaligned, or other ErrorResponse
   */
  def insertOneChunk(shard: Shard,
                     rowId: Long,
                     lastSequenceNo: Long,
                     columnsBytes: Map[String, ByteBuffer]): Future[Response] = {
    if (rowId % shard.partition.chunkSize != 0) return(Future(ChunkMisaligned))

    // NOTE: This is actually a good use of Unlogged Batch, because all of the inserts
    // are to the same partition key, so they will get collapsed down into one insert
    // for efficiency.
    // NOTE2: the batch add is immutable, so use foldLeft to get the updated batch
    val batch = columnsBytes.foldLeft(UnloggedBatchStatement()) {
      case (batch, (columnName, bytes)) =>
        // Sucks, it seems that reusing a partially prepared query doesn't work.
        // Issue filed: https://github.com/websudos/phantom/issues/166
        batch.add(insertQuery(shard).value(_.rowId, rowId)
                                    .value(_.columnName, columnName)
                                    .value(_.data, bytes))
    }
    batch.future().toResponse()
      .collect { case Success => Ack(lastSequenceNo) }
  }
}
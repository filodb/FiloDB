package filodb.core.ingest

import akka.actor.{Actor, ActorRef, Props}
import java.nio.ByteBuffer
import org.velvia.filo.{IngestColumn, RowIngestSupport, RowToColumnBuilder}

import filodb.core.BaseActor
import filodb.core.messages._
import filodb.core.metadata.{Column, Partition}

/**
 * The RowIngesterActor provides a high-level row-based API on top of the chunked columnar low level API.
 *
 * - ingest individual rows with a sequence # and Row ID.
 * - groups the rows into chunks aligned with the chunksize and translates into columnar format
 * - Also takes care of replaces by reading older chunks and doing operations on it
 *
 * RowIngesterActor uses a small row store cache, but the current design is really meant strictly for
 * increasing row IDs and is very simplistic.  If it encounters rowIDs which decrease then it is assumed
 * to be a replay of rows and a flush happens. Also, currently any change in version causes a flush.
 *
 * TODO: actually flush all the rows to a persistent row-store, so we don't have to worry about losing data
 * as much.  This would require more thought in terms of making sure all rows synced to the column store.
 * TODO2: or use Apache Kafka as the row-store.
 * TODO: deal effectively with partial chunks
 *
 * See doc/ingestion.md for more detailed information.
 */
object RowIngesterActor {
  /**
   * Appends a new row or replaces an existing row, depending on the rowId.
   * @param sequenceNo input sequence number, used for at least once acking / replays
   * @param rowId the row ID within the partition to append to or replace
   * @param version the integer version number to write row into
   * @param the row of data R
   */
  case class Row[R](sequenceNo: Long, rowId: Long, version: Int, row: R)

  /**
   * Creates a new RowIngesterActor (to be used in system.actorOf(....))
   * Note: partition does not need to include shard info, just chunkSize, dataset and partition name.
   */
  def props[R](ingesterActor: ActorRef,
               schema: Seq[Column],
               partition: Partition,
               rowIngestSupport: RowIngestSupport[R]): Props =
    Props(classOf[RowIngesterActor[R]], ingesterActor, schema, partition, rowIngestSupport)

  import org.velvia.filo._

  def schemaToFiloSchema(schema: Seq[Column]): Seq[IngestColumn] = schema.map {
    case Column(name, _, _, colType, serializer, false, false) =>
      require(serializer == Column.Serializer.FiloSerializer)
      val builder = colType match {
        case Column.ColumnType.IntColumn  => new IntColumnBuilder
        case Column.ColumnType.LongColumn => new LongColumnBuilder
        case Column.ColumnType.DoubleColumn => new DoubleColumnBuilder
        case Column.ColumnType.StringColumn => new StringColumnBuilder
        case x: Column.ColumnType =>
          throw new IllegalArgumentException(s"Sorry, column type $x not supported!")
      }
      IngestColumn(name, builder)
  }
}

class RowIngesterActor[R](ingesterActor: ActorRef,
                          schema: Seq[Column],
                          partition: Partition,
                          rowIngestSupport: RowIngestSupport[R]) extends BaseActor {
  import RowIngesterActor._

  var currentVersion = -1
  var lastRowId: Long = -1L
  var lastChunk: Long = 0L
  val rows = new collection.mutable.ArrayBuffer[Row[R]]()

  val chunkBuilder = new RowToColumnBuilder(schemaToFiloSchema(schema), rowIngestSupport)

  def chunk(rowId: Long): Long = rowId / partition.chunkSize

  def createChunkFromRows(): Map[String, ByteBuffer] = {
    chunkBuilder.reset()
    var rowId: Long = chunk(lastRowId) * partition.chunkSize
    rows.foreach { row =>
      while (rowId < row.rowId) {
        chunkBuilder.addEmptyRow()
        rowId += 1L
      }
      chunkBuilder.addRow(row.row)
      rowId += 1L
    }
    chunkBuilder.convertToBytes()
  }

  def flush(): Unit = if (rows.nonEmpty) {
    val chunkCmd = IngesterActor.ChunkedColumns(currentVersion,
                                                (lastChunk * partition.chunkSize, rows.last.rowId),
                                                rows.last.sequenceNo,
                                                createChunkFromRows())
    ingesterActor ! chunkCmd
    rows.clear()
  }

  def receive: Receive = {
    case row @ Row(seqNo, rowId, version, _) =>
      // Flush?  If we went over chunk boundary, or went backwards, or changed versions
      if (version != currentVersion ||
          rowId < lastRowId ||
          chunk(rowId) != lastChunk) { flush() }
      currentVersion = version
      lastRowId = rowId
      lastChunk = chunk(rowId)
      rows += row.asInstanceOf[Row[R]]
  }
}
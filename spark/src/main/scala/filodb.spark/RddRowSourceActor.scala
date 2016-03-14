package filodb.spark

import akka.actor.{ActorRef, Props}
import filodb.coordinator.{BaseActor, NodeCoordinatorActor, RowSource}
import org.apache.spark.sql.Row
import org.velvia.filo.RowReader

object RddRowSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedBatches = 10
  val DefaultRowsToRead = 1000

  def props(rows: Iterator[Row],
            columns: Seq[String],
            dataset: String,
            version: Int,
            coordinatorActor: ActorRef,
            maxUnackedBatches: Int = RddRowSourceActor.DefaultMaxUnackedBatches,
            rowsToRead: Int = RddRowSourceActor.DefaultRowsToRead): Props =
    Props(classOf[RddRowSourceActor], rows, columns, dataset, version,
          coordinatorActor, maxUnackedBatches, rowsToRead)
}

/**
 * A source actor for ingesting from one partition of a Spark DataFrame/SchemaRDD.
 * Assumes that the columns in the DataFrame have already been verified to be supported
 * by FiloDB and exist in the dataset already.
 *
 * TODO: implement the rewind() function, store unacked rows so we can replay them
 */
class RddRowSourceActor(rows: Iterator[Row],
                        columns: Seq[String],
                        val dataset: String,
                        val version: Int,
                        val coordinatorActor: ActorRef,
                        val maxUnackedBatches: Int = RddRowSourceActor.DefaultMaxUnackedBatches,
                        rowsToRead: Int = RddRowSourceActor.DefaultRowsToRead)
extends BaseActor with RowSource {
  import NodeCoordinatorActor._

  def getStartMessage(): SetupIngestion = SetupIngestion(dataset, columns, version)

  val seqIds = Iterator.from(0).map { id =>
    if (id % 20 == 0) logger.info(s"Ingesting batch starting at row ${id * rowsToRead}")
    id.toLong
  }

  val groupedRows = rows.map(RddRowReader).grouped(rowsToRead)
  val batchIterator = seqIds.zip(groupedRows)
}

case class RddRowReader(row: Row) extends RowReader {
  def notNull(columnNo: Int): Boolean = !row.isNullAt(columnNo)
  def getBoolean(columnNo: Int): Boolean = row.getBoolean(columnNo)
  def getInt(columnNo: Int): Int = row.getInt(columnNo)
  def getLong(columnNo: Int): Long = row.getLong(columnNo)
  def getDouble(columnNo: Int): Double = row.getDouble(columnNo)
  def getFloat(columnNo: Int): Float = row.getFloat(columnNo)
  def getString(columnNo: Int): String = row.getString(columnNo)
  def getAny(columnNo: Int): Any = row.get(columnNo)
}

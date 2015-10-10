package filodb.spark

import akka.actor.{Actor, ActorRef, Props}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types._
import org.velvia.filo.RowReader

import filodb.coordinator.{BaseActor, CoordinatorActor, RowSource}

object RddRowSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedRows = 5000
  val DefaultRowsToRead = 1000

  def props(rows: Iterator[Row],
            columns: Seq[String],
            dataset: String,
            version: Int,
            coordinatorActor: ActorRef,
            maxUnackedRows: Int = RddRowSourceActor.DefaultMaxUnackedRows,
            rowsToRead: Int = RddRowSourceActor.DefaultRowsToRead): Props =
    Props(classOf[RddRowSourceActor], rows, columns, dataset, version,
          coordinatorActor, maxUnackedRows, rowsToRead)
}

/**
 * A source actor for ingesting from one partition of a Spark DataFrame/SchemaRDD.
 * Assumes that the columns in the DataFrame have already been verified to be supported
 * by FiloDB and exist in the dataset already.
 */
class RddRowSourceActor(rows: Iterator[Row],
                        columns: Seq[String],
                        val dataset: String,
                        val version: Int,
                        val coordinatorActor: ActorRef,
                        val maxUnackedRows: Int = RddRowSourceActor.DefaultMaxUnackedRows,
                        val rowsToRead: Int = RddRowSourceActor.DefaultRowsToRead)
extends BaseActor with RowSource {
  import CoordinatorActor._
  import RddRowSourceActor._

  // Assume for now rowIDs start from 0.
  var seqId: Long = 0
  var lastAckedSeqNo = seqId

  def getStartMessage(): SetupIngestion = SetupIngestion(dataset, columns, version)

  // Returns a new row from source => (seqID, rowID, version, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, RowReader)] = {
    if (rows.hasNext) {
      val out = (seqId, RddRowReader(rows.next))
      seqId += 1
      if (seqId % 10000 == 0) logger.info(s"Parsed $seqId rows...")
      Some(out)
    } else {
      None
    }
  }
}

case class RddRowReader(row: Row) extends RowReader {
  def notNull(columnNo: Int): Boolean = !row.isNullAt(columnNo)
  def getBoolean(columnNo: Int): Boolean = row.getBoolean(columnNo)
  def getInt(columnNo: Int): Int = row.getInt(columnNo)
  def getLong(columnNo: Int): Long = row.getLong(columnNo)
  def getDouble(columnNo: Int): Double = row.getDouble(columnNo)
  def getFloat(columnNo: Int): Float = row.getFloat(columnNo)
  def getString(columnNo: Int): String = row.getString(columnNo)
}

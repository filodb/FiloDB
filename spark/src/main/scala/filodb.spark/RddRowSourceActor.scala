package filodb.spark

import akka.actor.{Actor, ActorRef, Props}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types._
import org.velvia.filo.RowIngestSupport

import filodb.core.BaseActor
import filodb.core.ingest.{CoordinatorActor, RowSource}

object RddRowSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedRows = 5000
  val DefaultRowsToRead = 100

  def props(rows: Iterator[Row],
            columns: Seq[String],
            dataset: String,
            partition: String,
            version: Int,
            coordinatorActor: ActorRef,
            maxUnackedRows: Int = RddRowSourceActor.DefaultMaxUnackedRows,
            rowsToRead: Int = RddRowSourceActor.DefaultRowsToRead): Props =
    Props(classOf[RddRowSourceActor], rows, columns, dataset, partition, version,
          coordinatorActor, maxUnackedRows, rowsToRead)
}

/**
 * A source actor for ingesting from one partition of a Spark DataFrame/SchemaRDD.
 * Assumes that the columns in the DataFrame have already been verified to be supported
 * by FiloDB and exist in the dataset already.
 */
class RddRowSourceActor(rows: Iterator[Row],
                        columns: Seq[String],
                        dataset: String,
                        partition: String,
                        version: Int,
                        val coordinatorActor: ActorRef,
                        val maxUnackedRows: Int = RddRowSourceActor.DefaultMaxUnackedRows,
                        val rowsToRead: Int = RddRowSourceActor.DefaultRowsToRead)
extends BaseActor with RowSource[Row] {
  import CoordinatorActor._
  import RddRowSourceActor._

  // Assume for now rowIDs start from 0.
  var seqId: Long = 0
  var lastAckedSeqNo = seqId

  def getStartMessage(): StartRowIngestion[Row] =
    StartRowIngestion(dataset, partition, columns, version, SparkRowIngestSupport)

  // Returns a new row from source => (seqID, rowID, version, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, Long, Int, Row)] = {
    if (rows.hasNext) {
      val out = (seqId, seqId, version, rows.next)
      seqId += 1
      if (seqId % 10000 == 0) logger.debug(s"seqId = $seqId")
      Some(out)
    } else {
      None
    }
  }
}

object SparkRowIngestSupport extends RowIngestSupport[Row] {
  type R = Row
  def getString(row: R, columnNo: Int): Option[String] =
    if (row.isNullAt(columnNo)) None else Some(row.getString(columnNo))
  def getInt(row: R, columnNo: Int): Option[Int] =
    if (row.isNullAt(columnNo)) None else Some(row.getInt(columnNo))
  def getLong(row: R, columnNo: Int): Option[Long] =
    if (row.isNullAt(columnNo)) None else Some(row.getLong(columnNo))
  def getDouble(row: R, columnNo: Int): Option[Double] =
    if (row.isNullAt(columnNo)) None else Some(row.getDouble(columnNo))
}

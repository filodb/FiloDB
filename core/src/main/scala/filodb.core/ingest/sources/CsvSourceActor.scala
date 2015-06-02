package filodb.core.ingest.sources

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import com.opencsv.CSVReader
import org.velvia.filo.RowIngestSupport
import scala.util.Try

import filodb.core.BaseActor
import filodb.core.ingest.{CoordinatorActor, RowSource}

object CsvSourceActor {
  case object AllDone

  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedRows = 3000
  val DefaultRowsToRead = 10

  def props(csvStream: java.io.Reader,
            dataset: String,
            partition: String,
            version: Int,
            coordinatorActor: ActorRef,
            maxUnackedRows: Int = DefaultMaxUnackedRows,
            rowsToRead: Int = DefaultRowsToRead,
            separatorChar: Char = ','): Props =
  Props(classOf[CsvSourceActor], csvStream, dataset, partition, version,
        coordinatorActor, maxUnackedRows, rowsToRead, separatorChar)
}

/**
 * Created for each ingestion of a CSV file.
 * This shows how easy it is to create an ingestion source.
 *
 * The CSV file must have a header row and the reader wound to the beginning of the file.
 * The header row is used to determine the column names to ingest.
 *
 * Non-actors can send RowSource.Start message and wait for the AllDone message.
 */
class CsvSourceActor(csvStream: java.io.Reader,
                     dataset: String,
                     partition: String,
                     version: Int,
                     val coordinatorActor: ActorRef,
                     val maxUnackedRows: Int = CsvSourceActor.DefaultMaxUnackedRows,
                     val rowsToRead: Int = CsvSourceActor.DefaultRowsToRead,
                     separatorChar: Char = ',') extends BaseActor with RowSource[Array[String]] {
  import CsvSourceActor._
  import CoordinatorActor._

  val reader = new CSVReader(csvStream, separatorChar)
  val columns = reader.readNext.toSeq
  logger.info(s"Started CsvSourceActor, ingesting CSV with columns $columns...")

  // Assume for now rowIDs start from 0.
  var seqId: Long = 0
  var lastAckedSeqNo = seqId

  def getStartMessage(): StartRowIngestion[Array[String]] =
    StartRowIngestion(dataset, partition, columns, version, OpenCsvRowSupport)

  // Returns a new row from source => (seqID, rowID, version, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, Long, Int, Array[String])] = {
    Option(reader.readNext()).map { rowValues =>
      val out = (seqId, seqId, version, rowValues)
      seqId += 1
      if (seqId % 10000 == 0) logger.debug(s"seqId = $seqId")
      out
    }
  }

  // What to do when we hit end of data and it's all acked. Typically, return OK and kill oneself.
  def allDoneAndGood(): Unit = {
    logger.info("Finished with CSV ingestion")
    context.parent ! AllDone
    self ! PoisonPill
  }
}

// Define a RowIngestSupport for CSV lines based on OpenCSV
// Throws exceptions if value cannot parse - better to fail fast than silently create an NA value
object OpenCsvRowSupport extends RowIngestSupport[Array[String]] {
  private def maybeString(row: Array[String], index: Int): Option[String] =
    Try(row(index)).toOption.filter(_.nonEmpty)

  def getString(row: Array[String], columnNo: Int): Option[String] =
    maybeString(row, columnNo)
  def getInt(row: Array[String], columnNo: Int): Option[Int] =
    maybeString(row, columnNo).map(_.toInt)
  def getLong(row: Array[String], columnNo: Int): Option[Long] =
    maybeString(row, columnNo).map(_.toLong)
  def getDouble(row: Array[String], columnNo: Int): Option[Double] =
    maybeString(row, columnNo).map(_.toDouble)
}
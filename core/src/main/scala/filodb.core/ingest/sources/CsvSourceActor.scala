package filodb.core.ingest.sources

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.opencsv.CSVReader
import org.velvia.filo.RowIngestSupport
import scala.util.Try

import filodb.core.BaseActor
import filodb.core.ingest.RowSource

object CsvSourceActor {
  case object AllDone

  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedRows = 3000
  val DefaultRowsToRead = 10
}

/**
 * Created for each ingestion of a CSV file.
 * This shows how easy it is to create an ingestion source.
 */
class CsvSourceActor(csvStream: java.io.Reader,
                     version: Int,
                     val rowIngesterActor: ActorRef,
                     val maxUnackedRows: Int = CsvSourceActor.DefaultMaxUnackedRows,
                     val rowsToRead: Int = CsvSourceActor.DefaultRowsToRead,
                     separatorChar: Char = ',') extends BaseActor with RowSource[Array[String]] {
  import CsvSourceActor._

  val reader = new CSVReader(csvStream, separatorChar)

  // Assume for now rowIDs start from 0.
  var seqId: Long = 0
  var lastAckedSeqNo = seqId

  // Returns a new row from source => (seqID, rowID, version, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, Long, Int, Array[String])] = {
    Option(reader.readNext()).map { rowValues =>
      val out = (seqId, seqId, version, rowValues)
      seqId += 1
      out
    }
  }

  // What to do when we hit end of data and it's all acked. Typically, return OK and kill oneself.
  def allDoneAndGood(): Unit = {
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
package filodb.coordinator.sources

import akka.actor.{Actor, ActorRef, Props}
import com.opencsv.CSVReader
import scala.util.Try

import filodb.coordinator.{BaseActor, CoordinatorActor, RowSource}
import filodb.core.columnstore.RowReader

object CsvSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedRows = 5000
  val DefaultRowsToRead = 100

  def props(csvStream: java.io.Reader,
            dataset: String,
            version: Int,
            coordinatorActor: ActorRef,
            maxUnackedRows: Int = DefaultMaxUnackedRows,
            rowsToRead: Int = DefaultRowsToRead,
            separatorChar: Char = ','): Props =
  Props(classOf[CsvSourceActor], csvStream, dataset, version,
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
                     val dataset: String,
                     val version: Int,
                     val coordinatorActor: ActorRef,
                     val maxUnackedRows: Int = CsvSourceActor.DefaultMaxUnackedRows,
                     val rowsToRead: Int = CsvSourceActor.DefaultRowsToRead,
                     separatorChar: Char = ',') extends BaseActor with RowSource {
  import CsvSourceActor._
  import CoordinatorActor._

  val reader = new CSVReader(csvStream, separatorChar)
  val columns = reader.readNext.toSeq
  logger.info(s"Started CsvSourceActor, ingesting CSV with columns $columns...")

  // Assume for now rowIDs start from 0.
  var seqId: Long = 0
  var lastAckedSeqNo = seqId

  def getStartMessage(): SetupIngestion = SetupIngestion(dataset, columns, version)

  // Returns a new row from source => (seqID, rowID, version, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, RowReader)] = {
    Option(reader.readNext()).map { rowValues =>
      val out = (seqId, ArrayStringRowReader(rowValues))
      seqId += 1
      if (seqId % 10000 == 0) logger.debug(s"seqId = $seqId")
      out
    }
  }
}

case class ArrayStringRowReader(strings: Array[String]) extends RowReader {
  //scalastyle:off
  def notNull(columnNo: Int): Boolean = strings(columnNo) != null && strings(columnNo) != ""
  //scalastyle:on
  def getInt(columnNo: Int): Int = strings(columnNo).toInt
  def getLong(columnNo: Int): Long = strings(columnNo).toLong
  def getDouble(columnNo: Int): Double = strings(columnNo).toDouble
  def getString(columnNo: Int): String = strings(columnNo)
}


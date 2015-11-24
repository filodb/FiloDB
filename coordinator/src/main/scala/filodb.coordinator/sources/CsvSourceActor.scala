package filodb.coordinator.sources

import akka.actor.{Actor, ActorRef, Props}
import com.opencsv.CSVReader
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.util.Try

import filodb.coordinator.{BaseActor, NodeCoordinatorActor, RowSource}

object CsvSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedBatches = 50
  val RowsToRead = 100

  def props(csvStream: java.io.Reader,
            dataset: String,
            version: Int,
            coordinatorActor: ActorRef,
            maxUnackedBatches: Int = DefaultMaxUnackedBatches,
            rowsToRead: Int = RowsToRead,
            separatorChar: Char = ','): Props =
  Props(classOf[CsvSourceActor], csvStream, dataset, version,
        coordinatorActor, maxUnackedBatches, rowsToRead, separatorChar)
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
                     val maxUnackedBatches: Int = CsvSourceActor.DefaultMaxUnackedBatches,
                     rowsToRead: Int = CsvSourceActor.RowsToRead,
                     separatorChar: Char = ',') extends BaseActor with RowSource {
  import CsvSourceActor._
  import NodeCoordinatorActor._
  import collection.JavaConverters._

  val reader = new CSVReader(csvStream, separatorChar)
  val columns = reader.readNext.toSeq
  logger.info(s"Started CsvSourceActor, ingesting CSV with columns $columns...")

  def getStartMessage(): SetupIngestion = SetupIngestion(dataset, columns, version)

  val seqIds = Iterator.from(1).map(_.toLong)
  val batchRows: Iterator[Seq[RowReader]] = reader.iterator.asScala
                                                  .map(ArrayStringRowReader)
                                                  .grouped(rowsToRead)
  val batchIterator = seqIds.zip(batchRows)
}

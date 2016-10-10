package filodb.coordinator.sources

import akka.actor.{Actor, ActorRef, Props}
import com.opencsv.CSVReader
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.concurrent.duration._
import scala.util.Try

import filodb.coordinator.{BaseActor, IngestionCommands, RowSource}
import filodb.core.DatasetRef
import filodb.core.metadata.RichProjection

object CsvSourceActor {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedBatches = 50
  val RowsToRead = 100

  def props(csvStream: java.io.Reader,
            projection: RichProjection,
            version: Int,
            clusterActor: ActorRef,
            maxUnackedBatches: Int = DefaultMaxUnackedBatches,
            rowsToRead: Int = RowsToRead,
            separatorChar: Char = ',',
            ackTimeout: FiniteDuration = 10.seconds,
            waitPeriod: FiniteDuration = 5.seconds): Props =
  Props(classOf[CsvSourceActor], csvStream, projection, version,
        clusterActor, maxUnackedBatches, rowsToRead, separatorChar, ackTimeout, waitPeriod)
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
                     val projection: RichProjection,
                     val version: Int,
                     val clusterActor: ActorRef,
                     val maxUnackedBatches: Int,
                     rowsToRead: Int,
                     separatorChar: Char,
                     override val ackTimeout: FiniteDuration,
                     override val waitingPeriod: FiniteDuration) extends BaseActor with RowSource {
  import CsvSourceActor._
  import IngestionCommands._
  import collection.JavaConverters._

  val reader = new CSVReader(csvStream, separatorChar)
  val columns = reader.readNext.toSeq
  logger.info(s"Started CsvSourceActor, ingesting CSV with columns $columns...")

  val batchIterator: Iterator[Seq[RowReader]] = reader.iterator.asScala
                                                  .map(ArrayStringRowReader)
                                                  .grouped(rowsToRead)
}

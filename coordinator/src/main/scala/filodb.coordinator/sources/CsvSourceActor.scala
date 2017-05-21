package filodb.coordinator.sources

import akka.actor.{Actor, ActorRef, Props}
import com.opencsv.CSVReader
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.concurrent.duration._

import filodb.coordinator.{BaseActor, IngestionCommands, RowSource, RowSourceFactory}
import filodb.core.DatasetRef
import filodb.core.memstore.MemStore
import filodb.core.metadata.{Dataset, RichProjection, Column}

object CsvSourceActor extends StrictLogging {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedBatches = 50
  val RowsToRead = 100

  final case class CsvSourceSettings(header: Boolean = true,
                                     maxUnackedBatches: Int = DefaultMaxUnackedBatches,
                                     rowsToRead: Int = RowsToRead,
                                     ackTimeout: FiniteDuration = 10.seconds,
                                     waitPeriod: FiniteDuration = 5.seconds)

  def getHeaderColumns(csvStream: java.io.Reader,
                       separatorChar: Char = ','): Seq[String] = {
    val reader = new CSVReader(csvStream, separatorChar)
    reader.readNext.toSeq
  }

  def getHeaderColumns(csvPath: String): Seq[String] = {
    val fileReader = new java.io.FileReader(csvPath)
    getHeaderColumns(fileReader)
  }

  /**
   * The entry point for CSV files.  The projection must be passed in and match the column
   * order in the file.  Set header to true to skip over the header - columns must match projection
   */
  def props(csvReader: CSVReader,
            projection: RichProjection,
            version: Int,
            settings: CsvSourceSettings = CsvSourceSettings()): Props = {
    Props(classOf[CsvSourceActor], csvReader, projection, version, settings)
  }
}

/**
 * Config for CSV ingestion:
 * {{{
 *   file = "/path/to/file.csv"
 *   header = true
 *   rows-to-read = 100
 *   max-unacked-batches = 2
 * }}}
 * Instead of file one can put "resource"
 */
class CsvSourceFactory extends RowSourceFactory {
  import CsvSourceActor._

  def create(config: Config, projection: RichProjection, memStore: MemStore): Props = {
    val settings = CsvSourceSettings(config.getBoolean("header"),
                     config.as[Option[Int]]("max-unacked-batches").getOrElse(DefaultMaxUnackedBatches),
                     config.as[Option[Int]]("rows-to-read").getOrElse(RowsToRead))
    val reader = config.as[Option[String]]("file").map { filePath =>
                   new java.io.FileReader(filePath)
                 }.getOrElse {
                   new java.io.InputStreamReader(getClass.getResourceAsStream(config.getString("resource")))
                 }
    props(new CSVReader(reader), projection, 0, settings)
  }
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
class CsvSourceActor(reader: CSVReader,
                     val projection: RichProjection,
                     val version: Int,
                     settings: CsvSourceActor.CsvSourceSettings) extends BaseActor with RowSource {
  import CsvSourceActor._
  import collection.JavaConverters._

  val maxUnackedBatches = settings.maxUnackedBatches
  override val waitingPeriod = settings.waitPeriod
  override val ackTimeout = settings.ackTimeout

  if (settings.header) reader.readNext

  val batchIterator: Iterator[Seq[RowReader]] = reader.iterator.asScala
                                                  .map(ArrayStringRowReader)
                                                  .grouped(settings.rowsToRead)
}

package filodb.coordinator.sources

import akka.actor.{Actor, ActorRef, Props}
import com.opencsv.CSVReader
import com.typesafe.scalalogging.StrictLogging
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.concurrent.duration._
import scala.util.Try

import filodb.coordinator.{BaseActor, IngestionCommands, RowSource}
import filodb.core.DatasetRef
import filodb.core.metadata.{Dataset, RichProjection, Column}

object CsvSourceActor extends StrictLogging {
  // Needs to be a multiple of chunkSize. Not sure how to have a good default though.
  val DefaultMaxUnackedBatches = 50
  val RowsToRead = 100

  final case class CsvSourceSettings(maxUnackedBatches: Int = DefaultMaxUnackedBatches,
                                     rowsToRead: Int = RowsToRead,
                                     ackTimeout: FiniteDuration = 10.seconds,
                                     waitPeriod: FiniteDuration = 5.seconds)

  /**
   * The entry point for when your CSV file has a header row.  The projection will be created from the
   * columns in the header row.  Column names not defined in the schema will result in an exception.
   */
  def props(csvStream: java.io.Reader,
            dataset: Dataset,
            schema: Column.Schema,
            version: Int,
            clusterActor: ActorRef,
            separatorChar: Char = ',',
            settings: CsvSourceSettings = CsvSourceSettings()): Props = {
    val (projection, reader, columns) = getProjectionFromHeader(csvStream, dataset, schema, separatorChar)
    logger.info(s"Ingesting CSV with columns $columns...")
    Props(classOf[CsvSourceActor], reader, projection, version, clusterActor, settings)
  }

  def getProjectionFromHeader(csvStream: java.io.Reader,
                              dataset: Dataset,
                              schema: Column.Schema,
                              separatorChar: Char = ','): (RichProjection, CSVReader, Seq[String]) = {
    val reader = new CSVReader(csvStream, separatorChar)
    val columns = reader.readNext.toSeq
    val projection = RichProjection(dataset, columns.map(schema))
    (projection, reader, columns)
  }

  /**
   * The entry point for CSV files with no headers.  The projection must be passed in and match the column
   * order in the file.
   */
  def propsNoHeader(csvReader: CSVReader,
                    projection: RichProjection,
                    version: Int,
                    clusterActor: ActorRef,
                    settings: CsvSourceSettings = CsvSourceSettings()): Props = {
    Props(classOf[CsvSourceActor], csvReader, projection, version, clusterActor, settings)
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
                     val clusterActor: ActorRef,
                     settings: CsvSourceActor.CsvSourceSettings) extends BaseActor with RowSource {
  import CsvSourceActor._
  import collection.JavaConverters._

  val maxUnackedBatches = settings.maxUnackedBatches
  override val waitingPeriod = settings.waitPeriod
  override val ackTimeout = settings.ackTimeout

  val batchIterator: Iterator[Seq[RowReader]] = reader.iterator.asScala
                                                  .map(ArrayStringRowReader)
                                                  .grouped(settings.rowsToRead)
}

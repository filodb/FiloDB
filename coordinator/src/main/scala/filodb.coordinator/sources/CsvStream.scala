package filodb.coordinator.sources

import com.opencsv.CSVReader
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.{IngestionStream, IngestionStreamFactory}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.SomeData
import filodb.core.metadata.Schemas
import filodb.memory.format.{ArrayStringRowReader, RoutingRowReader}
import filodb.memory.MemFactory

object CsvStream extends StrictLogging {
  // Number of lines to read and send at a time
  val BatchSize = 100

  final case class CsvStreamSettings(header: Boolean = true,
                                     batchSize: Int = BatchSize,
                                     separatorChar: Char = ',')

  def getHeaderColumns(csvStream: java.io.Reader,
                       separatorChar: Char = ','): (Seq[String], CSVReader) = {
    val reader = new CSVReader(csvStream, separatorChar)
    (reader.readNext.toSeq, reader)
  }

  def getHeaderColumns(csvPath: String): (Seq[String], CSVReader) = {
    val fileReader = new java.io.FileReader(csvPath)
    getHeaderColumns(fileReader)
  }
}

/**
 * Config for CSV ingestion:
 * {{{
 *   file = "/path/to/file.csv"
 *   header = true
 *   batch-size = 100
 *   # separator-char = ","
 *   column-names = ["time", "value"]
 * }}}
 * Instead of file one can put "resource"
 *
 * If the CSV has a header, you need to set header=true.
 * Then the header will be parsed automatically.
 * Otherwise you must pass in the column names in the config.
 *
 * Offsets created are the line numbers, where the first line after the header is offset 0, next one 1, etc.
 * This Stream Factory is capable of rewinding to a given line number when given a positive offset.
 *
 * NOTE: if this is started with more than one shard, it will read from the same file.
 */
class CsvStreamFactory extends IngestionStreamFactory {
  import CsvStream._

  def create(config: Config, schemas: Schemas, shard: Int, offset: Option[Long]): IngestionStream = {
    require(shard >= 0, s"Shard on creation must be positive but was '$shard'.")
    val settings = CsvStreamSettings(config.getBoolean("header"),
                     config.as[Option[Int]]("batch-size").getOrElse(BatchSize),
                     config.as[Option[String]]("separator-char").getOrElse(",").charAt(0))
    val reader = config.as[Option[String]]("file").map { filePath =>
                   new java.io.FileReader(filePath)
                 }.getOrElse {
                   new java.io.InputStreamReader(getClass.getResourceAsStream(config.getString("resource")))
                 }

    val (columnNames, csvReader) = if (settings.header) {
      getHeaderColumns(reader, settings.separatorChar)
    } else {
      (config.as[Seq[String]]("column-names"), new CSVReader(reader, settings.separatorChar))
    }
    new CsvStream(csvReader, schemas, columnNames, settings, offset)
  }
}

/**
 * CSV post-header reader.
 * Either the CSV file has no headers, in which case the column names must be supplied,
 * or you can read the first line and parse the headers and then invoke this class.
 *
 * @param offset the number of lines to skip; must be >=0 and <= Int.MaxValue or will reset to 0
 */
private[filodb] class CsvStream(csvReader: CSVReader,
                                schemas: Schemas,
                                columnNames: Seq[String],
                                settings: CsvStream.CsvStreamSettings,
                                offset: Option[Long]) extends IngestionStream with StrictLogging {
  import collection.JavaConverters._
  require(schemas.schemas.nonEmpty, s"Schemas cannot be empty")

  val numLinesToSkip = offset.filter(n => n >= 0 && n <= Int.MaxValue).map(_.toInt)
                             .getOrElse {
                               logger.info(s"Possibly resetting offset to 0; supplied offset $offset")
                               0
                             }

  val schema = schemas.schemas.values.find { sch =>
    val allCols = (sch.partition.columns ++ sch.data.columns).map(_.name).toSet
    allCols.intersect(columnNames.toSet).size == columnNames.length
  }.get

  val routing = schema.ingestRouting(columnNames)
  logger.info(s"CsvStream started with schema ${schema.data.name}, columnNames $columnNames, routing $routing")
  if (numLinesToSkip > 0) logger.info(s"Skipping initial $numLinesToSkip lines...")

  val batchIterator = csvReader.iterator.asScala
                        .drop(numLinesToSkip)
                        .map { tokens => RoutingRowReader(ArrayStringRowReader(tokens), routing) }
                        .grouped(settings.batchSize)
                        .zipWithIndex
                        .flatMap { case (readers, idx) =>
                          val builder = new RecordBuilder(MemFactory.onHeapFactory)
                          readers.foreach(builder.addFromReader(_, schema))
                          // Most likely to have only one container.  Just assign same offset.
                          builder.allContainers.map { c => SomeData(c, idx) }
                        }
  val get = Observable.fromIterator(batchIterator)

  def teardown(): Unit = {
    csvReader.close()
  }
}
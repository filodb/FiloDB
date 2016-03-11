package filodb.cli

import java.io.OutputStream
import javax.activation.UnsupportedDataTypeException

import akka.actor.ActorSystem
import com.opencsv.CSVWriter
import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.typesafe.config.ConfigFactory
import org.velvia.filo.{RowReader, FastFiloRowReader}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core._
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.{NodeCoordinatorActor, CoordinatorSetup}
import filodb.core.metadata.Column.{ColumnType, Schema}
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.core.store.{Analyzer, CachedMergingColumnStore, FilteredPartitionScan}

//scalastyle:off
class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
  var columns: Option[Map[String, String]] = None
  var rowKeys: Seq[String] = Nil
  var segmentKey: Option[String] = None
  var partitionKeys: Seq[String] = Nil
  var version: Option[Int] = None
  var select: Option[Seq[String]] = None
  var limit: Int = 1000
  var timeoutMinutes: Int = 99
  var outfile: Option[String] = None
  var delimiter: String = ","

  import Column.ColumnType._

  def toColumns(dataset: String, version: Int): Seq[DataColumn] = {
    columns.map { colStrStr =>
      colStrStr.map { case (name, colType) =>
        colType match {
          case "int"    => DataColumn(0, name, dataset, version, IntColumn)
          case "long"   => DataColumn(0, name, dataset, version, LongColumn)
          case "double" => DataColumn(0, name, dataset, version, DoubleColumn)
          case "string" => DataColumn(0, name, dataset, version, StringColumn)
          case "bool"   => DataColumn(0, name, dataset, version, BitmapColumn)
          case "timestamp" => DataColumn(0, name, dataset, version, TimestampColumn)
        }
      }.toSeq
    }.getOrElse(Nil)
  }
}

object CliMain extends ArgMain[Arguments] with CsvImportExport with CoordinatorSetup {

  val system = ActorSystem("filo-cli")
  val config = ConfigFactory.load.getConfig("filodb")
  lazy val columnStore = new CassandraColumnStore(config, readEc)
  lazy val metaStore = new CassandraMetaStore(config.getConfig("cassandra"))

  def printHelp() {
    println("filo-cli help:")
    println("  commands: init create importcsv list analyze delete")
    println("  columns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string,bool,timestamp")
    println("  OR:  --select col1, col2  [--limit <n>]  [--outfile /tmp/out.csv]")
    println("\nTo change config: pass -Dconfig.file=/path/to/config as first arg or set $FILO_CONFIG_FILE")
    println("  or override any config by passing -Dconfig.path=newvalue as first args")
  }

  def main(args: Arguments) {
    try {
      val version = args.version.getOrElse(0)
      args.command match {
        case Some("init") =>
          println("Initializing FiloDB Cassandra tables...")
          awaitSuccess(metaStore.initialize())
        case Some("list") =>
          args.dataset.map(dumpDataset).getOrElse(dumpAllDatasets())
        case Some("create") =>
          require(args.dataset.isDefined && args.columns.isDefined, "Need to specify dataset and columns")
          require(args.segmentKey.isDefined, "--segmentKey must be defined")
          require(args.rowKeys.nonEmpty, "--rowKeys must be defined")
          val datasetName = args.dataset.get
          createDatasetAndColumns(datasetName, args.toColumns(datasetName, version),
                                  args.rowKeys,
                                  args.segmentKey.get,
                                  if (args.partitionKeys.isEmpty) { Seq(Dataset.DefaultPartitionColumn) }
                                  else { args.partitionKeys })
        case Some("importcsv") =>
          import org.apache.commons.lang3.StringEscapeUtils._
          val delimiter = unescapeJava(args.delimiter)(0)
          ingestCSV(args.dataset.get,
                    version,
                    args.filename.get,
                    delimiter,
                    args.timeoutMinutes.minutes)
        case Some("analyze") =>
          println(Analyzer.analyze(columnStore.asInstanceOf[CachedMergingColumnStore],
                                   metaStore,
                                   args.dataset.get,
                                   version).prettify())
        case Some("delete") =>
          parse(metaStore.deleteDataset(args.dataset.get)) { x => x }

        case x: Any =>
          args.select.map { selectCols =>
            exportCSV(args.dataset.get,
                      version,
                      selectCols,
                      args.limit,
                      args.outfile)
          }.getOrElse(printHelp)
      }
    } catch {
      case e: Throwable =>
        println("Uncaught exception:")
        e.printStackTrace()
        exitCode = 2
    } finally {
      shutdown()
      sys.exit(exitCode)
    }
  }

  def dumpDataset(dataset: String) {
    parse(metaStore.getDataset(dataset)) { datasetObj =>
      println(s"Dataset name: ${datasetObj.name}")
      println(s"Partition keys: ${datasetObj.partitionColumns.mkString(", ")}")
      println(s"Options: ${datasetObj.options}\n")
      datasetObj.projections.foreach(p => println(p.detailedString))
    }
    parse(metaStore.getSchema(dataset, Int.MaxValue)) { schema =>
      println("\nColumns:")
      schema.values.toSeq.sortBy(_.name).foreach { case DataColumn(_, name, _, ver, colType, _) =>
        println("  %-35.35s %5d %s".format(name, ver, colType))
      }
    }
  }

  def dumpAllDatasets() {
    parse(metaStore.getAllDatasets) { datasets =>
      datasets.foreach(println)
    }
  }

  def createDatasetAndColumns(dataset: String,
                              columns: Seq[DataColumn],
                              rowKeys: Seq[String],
                              segmentKey: String,
                              partitionKeys: Seq[String]) {
    println(s"Creating dataset $dataset...")
    val datasetObj = Dataset(dataset, rowKeys, segmentKey, partitionKeys)

    RichProjection.make(datasetObj, columns).recover {
      case err: RichProjection.BadSchema =>
        println(s"Bad dataset schema: $err")
        exitCode = 2
        return
    }

    actorAsk(coordinatorActor, NodeCoordinatorActor.CreateDataset(datasetObj, columns)) {
      case NodeCoordinatorActor.DatasetCreated =>
        println(s"Dataset $dataset created!")
        exitCode = 0
      case NodeCoordinatorActor.DatasetError(errMsg) =>
        println(s"Error creating dataset $dataset: $errMsg")
        exitCode = 2
    }
  }

  import scala.language.existentials
  import filodb.core.metadata.Column.ColumnType._

  private def getRowValues(columns: Seq[Column],
                           columnCount: Int,
                           rowReader: RowReader): Array[String] = {
    var position = 0
    val content = new Array[String](columnCount)
    while (position < columnCount) {
      val value: String = columns(position).columnType match {
        case IntColumn => rowReader.getInt(position).toString
        case LongColumn => rowReader.getLong(position).toString
        case DoubleColumn => rowReader.getDouble(position).toString
        case StringColumn => rowReader.getString(position)
        case _ => throw new UnsupportedDataTypeException
      }
      content.update(position,value)
      position += 1
    }
    content
  }

  private def writeResult(datasetName: String,
                          rows: Iterator[RowReader],
                          columnNames: Seq[String],
                          columns: Seq[Column],
                          outFile: Option[String]) = {
    var outStream: OutputStream = null
    var writer: CSVWriter = null
    val columnCount: Int = columns.size
    try {
      outStream = outFile.map(new java.io.FileOutputStream(_)).getOrElse(System.out)
      writer = new CSVWriter(new java.io.OutputStreamWriter(outStream))
      writer.writeNext(columnNames.toArray, false)
      rows.foreach {
        r =>
          val content: Array[String] = getRowValues(columns,columnCount, r)
          writer.writeNext(content, false)
      }
    } catch {
      case e: Throwable =>
        println(s"Failed to select/export dataset $datasetName")
        throw e
    } finally {
      writer.close()
      outStream.close()
    }
  }

  def exportCSV(dataset: String,
                version: Int,
                columnNames: Seq[String],
                limit: Int,
                outFile: Option[String]): Unit = {
    val schema = Await.result(metaStore.getSchema(dataset, version), 10.second)
    val datasetObj = parse(metaStore.getDataset(dataset)) { ds => ds }
    val richProj = RichProjection(datasetObj, schema.values.toSeq)
    val columns = columnNames.map(schema)

    // NOTE: we will only return data from the first split!
    val splits = columnStore.getScanSplits(dataset)
    val requiredRows = parse(columnStore.scanRows(richProj, columns, version,
                                                  FilteredPartitionScan(splits.head))) {
                         x => x.take(limit) }
    writeResult(dataset, requiredRows, columnNames, columns, outFile)
  }
}
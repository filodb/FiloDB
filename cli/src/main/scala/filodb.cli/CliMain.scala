package filodb.cli

import java.io.OutputStream
import java.sql.Timestamp
import javax.activation.UnsupportedDataTypeException

import akka.actor.ActorSystem
import com.opencsv.CSVWriter
import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.typesafe.config.ConfigFactory
import monix.reactive.Observable
import org.velvia.filo.{RowReader, FastFiloRowReader}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.cassandra.columnstore.CassandraTokenRangeSplit
import filodb.coordinator.client.{Client, LocalClient}
import filodb.coordinator.{DatasetCommands, CoordinatorSetupWithFactory}
import filodb.core._
import filodb.core.metadata.Column.{ColumnType, Schema}
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.core.store.{Analyzer, ChunkInfo, ChunkSetInfo, FilteredPartitionScan, ScanSplit}

//scalastyle:off
class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var database: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
  var columns: Option[Map[String, String]] = None
  var rowKeys: Seq[String] = Nil
  var partitionKeys: Seq[String] = Nil
  var numPartitions: Int = 1000
  var version: Option[Int] = None
  var select: Option[Seq[String]] = None
  var limit: Int = 1000
  var timeoutSeconds: Int = 60
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

object CliMain extends ArgMain[Arguments] with CsvImportExport with CoordinatorSetupWithFactory {

  val system = ActorSystem("filo-cli", systemConfig)
  val config = systemConfig.getConfig("filodb")
  val client = new LocalClient(coordinatorActor)

  import Client.{actorAsk, parse}

  def printHelp() {
    println("filo-cli help:")
    println("  commands: init create importcsv list analyze dumpinfo delete truncate")
    println("  columns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string,bool,timestamp")
    println("  common options:  --dataset --database")
    println("  OR:  --select col1, col2  [--limit <n>]  [--outfile /tmp/out.csv]")
    println("\nTo change config: pass -Dconfig.file=/path/to/config as first arg or set $FILO_CONFIG_FILE")
    println("  or override any config by passing -Dconfig.path=newvalue as first args")
    println("\nFor detailed debugging, uncomment the TRACE/DEBUG loggers in logback.xml and add these ")
    println("  options:  ./filo-cli -Dakka.loglevel=DEBUG -Dakka.actor.debug.receive=on -Dakka.actor.debug.autoreceive=on --command importcsv ...")
  }

  def getRef(args: Arguments): DatasetRef = DatasetRef(args.dataset.get, args.database)

  def combineSplits(splits: Seq[ScanSplit]): ScanSplit = {
    val csplits = splits.asInstanceOf[Seq[CassandraTokenRangeSplit]]

    // Compress all the token ranges down into one split
    csplits.foldLeft(csplits.head.copy(tokens = Nil)) { case (nextSplit, ourSplit) =>
      ourSplit.copy(tokens = ourSplit.tokens ++ nextSplit.tokens)
    }
  }

  def main(args: Arguments) {
    try {
      val version = args.version.getOrElse(0)
      val timeout = args.timeoutSeconds.seconds
      args.command match {
        case Some("init") =>
          println("Initializing FiloDB Admin keyspace and tables...")
          parse(metaStore.initialize(), timeout) {
            case Success =>   println("Succeeded.")
          }

        case Some("list") =>
          args.dataset.map(ds => dumpDataset(ds, args.database)).getOrElse(dumpAllDatasets(args.database))

        case Some("create") =>
          require(args.dataset.isDefined && args.columns.isDefined, "Need to specify dataset and columns")
          require(args.rowKeys.nonEmpty, "--rowKeys must be defined")
          val datasetName = args.dataset.get
          createDatasetAndColumns(getRef(args), args.toColumns(datasetName, version),
                                  args.rowKeys,
                                  if (args.partitionKeys.isEmpty) { Seq(Dataset.DefaultPartitionColumn) }
                                  else { args.partitionKeys },
                                  timeout)

        case Some("importcsv") =>
          import org.apache.commons.lang3.StringEscapeUtils._
          val delimiter = unescapeJava(args.delimiter)(0)
          ingestCSV(getRef(args),
                    version,
                    args.filename.get,
                    delimiter,
                    99.minutes)

        case Some("analyze") =>
          parse(Analyzer.analyze(columnStore,
                                 metaStore,
                                 getRef(args),
                                 version,
                                 combineSplits,
                                 args.numPartitions), 30.minutes)(a => println(a.prettify()))

        case Some("dumpinfo") =>
          printChunkInfos(Analyzer.getChunkInfos(columnStore,
                                   metaStore,
                                   getRef(args),
                                   version,
                                   combineSplits,
                                   args.numPartitions))

        case Some("delete") =>
          client.deleteDataset(getRef(args), timeout)

        case Some("truncate") =>
          client.truncateDataset(getRef(args), version, timeout)

        case x: Any =>
          args.select.map { selectCols =>
            exportCSV(getRef(args),
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

  def dumpDataset(dataset: String, database: Option[String]) {
    val ref = DatasetRef(dataset, database)
    require(ref.dataset == dataset)
    parse(metaStore.getDataset(ref)) { datasetObj =>
      println(s"Dataset name: ${datasetObj.name}")
      println(s"Partition keys: ${datasetObj.partitionColumns.mkString(", ")}")
      println(s"Options: ${datasetObj.options}\n")
      datasetObj.projections.foreach(p => println(p.detailedString))
    }
    parse(metaStore.getSchema(ref, Int.MaxValue)) { schema =>
      println("\nColumns:")
      schema.values.toSeq.sortBy(_.name).foreach { case DataColumn(_, name, _, ver, colType, _) =>
        println("  %-35.35s %5d %s".format(name, ver, colType))
      }
    }
  }

  def dumpAllDatasets(database: Option[String]) {
    parse(metaStore.getAllDatasets(database)) { refs =>
      refs.foreach { ref => println("%25s\t%s".format(ref.database.getOrElse(""), ref.dataset)) }
    }
  }

  def printChunkInfos(infos: Observable[ChunkInfo]): Unit = {
    val fut = infos.foreach { case ChunkInfo(partKey, ChunkSetInfo(id, numRows, firstKey, lastKey)) =>
      println(" %25s\t%10d %5d  %40s %s".format(partKey, id, numRows, firstKey, lastKey))
    }
    parse(fut) { x => x }
  }

  def createDatasetAndColumns(dataset: DatasetRef,
                              columns: Seq[DataColumn],
                              rowKeys: Seq[String],
                              partitionKeys: Seq[String],
                              timeout: FiniteDuration) {
    println(s"Creating dataset $dataset...")
    val datasetObj = Dataset(dataset, rowKeys, partitionKeys)

    RichProjection.make(datasetObj, columns).recover {
      case err: RichProjection.BadSchema =>
        println(s"Bad dataset schema: $err")
        exitCode = 2
        return
    }

    actorAsk(coordinatorActor,
             DatasetCommands.CreateDataset(datasetObj, columns, dataset.database),
             timeout) {
      case DatasetCommands.DatasetCreated =>
        println(s"Dataset $dataset created!")
        exitCode = 0
      case DatasetCommands.DatasetAlreadyExists =>
        println(s"Dataset $dataset already exists!")
        exitCode = 0
      case DatasetCommands.DatasetError(errMsg) =>
        println(s"Error creating dataset $dataset: $errMsg")
        exitCode = 2
      case other: Any =>
        println(s"Error: $other")
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
        case StringColumn => rowReader.filoUTF8String(position).toString
        case BitmapColumn => rowReader.getBoolean(position).toString
        case TimestampColumn => rowReader.as[Timestamp](position).toString
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

  def exportCSV(dataset: DatasetRef,
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
    val requiredRows = columnStore.scanRows(richProj, columns, version, FilteredPartitionScan(splits.head))
                                  .take(limit)
    writeResult(dataset.dataset, requiredRows, columnNames, columns, outFile)
  }
}
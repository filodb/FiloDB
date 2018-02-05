package filodb.cli

import java.io.OutputStream
import java.sql.Timestamp
import javax.activation.UnsupportedDataTypeException

import scala.concurrent.duration._
import scala.util.{Failure, Success => SSuccess, Try}

import com.opencsv.CSVWriter
import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.typesafe.config.{Config, ConfigFactory}
import org.parboiled2.ParseError

import filodb.coordinator._
import filodb.coordinator.client._
import filodb.core._
import filodb.core.metadata.{Column, Dataset, DatasetOptions}
import filodb.core.store._
import filodb.memory.format.RowReader

// scalastyle:off
class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var database: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
  var configPath: Option[String] = None
  var dataColumns: Seq[String] = Nil
  var partitionColumns: Seq[String] = Nil
  var rowKeys: Seq[String] = Seq("timestamp")
  var partitionKeys: Seq[String] = Nil
  var select: Option[Seq[String]] = None
  var limit: Int = 1000
  var timeoutSeconds: Int = 60
  var outfile: Option[String] = None
  var delimiter: String = ","
  var indexName: Option[String] = None
  var host: Option[String] = None
  var port: Int = 2552
  var promql: Option[String] = None
  var metricColumn: String = "__name__"
}

object CliMain extends ArgMain[Arguments] with CsvImportExport with FilodbClusterNode {

  override val role = ClusterRole.Cli

  lazy val client = new LocalClient(coordinatorActor)

  val config = systemConfig.getConfig("filodb")

  import Client.parse

  def printHelp(): Unit = {
    println("filo-cli help:")
    println("  commands: init create importcsv list truncate clearMetadata")
    println("  dataColumns/partitionColumns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string,bitmap,ts,map")
    println("  common options:  --dataset --database")
    println("  OR:  --select col1, col2  [--limit <n>]  [--outfile /tmp/out.csv]")
    println("\n  NOTE: truncate and clearMetadata should NOT be used while FiloDB instances are up\n")
    println("\nStandalone client commands:")
    println("  --host <hostname/IP> [--port ...] --command indexnames --dataset <dataset>")
    println("  --host <hostname/IP> [--port ...] --command indexvalues --indexname <index> --dataset <dataset>")
    println("  --host <hostname/IP> [--port ...] [--metricColumn <col>] --dataset <dataset> --promql <query>")
    println("  --host <hostname/IP> [--port ...] --command setup --filename <configFile> | --configPath <path>")
    println("  --host <hostname/IP> [--port ...] --command list")
    println("  --host <hostname/IP> [--port ...] --command status --dataset <dataset>")
    println("\nTo change config: pass -Dconfig.file=/path/to/config as first arg or set $FILO_CONFIG_FILE")
    println("  or override any config by passing -Dconfig.path=newvalue as first args")
    println("\nFor detailed debugging, uncomment the TRACE/DEBUG loggers in logback.xml and add these ")
    println("  options:  ./filo-cli -Dakka.loglevel=DEBUG -Dakka.actor.debug.receive=on -Dakka.actor.debug.autoreceive=on --command importcsv ...")
  }

  def getRef(args: Arguments): DatasetRef = DatasetRef(args.dataset.get, args.database)

  def getClientAndRef(args: Arguments): (LocalClient, DatasetRef) = {
    require(args.host.nonEmpty && args.dataset.nonEmpty, "--host and --dataset must be defined")
    val remote = Client.standaloneClient(system, args.host.get, args.port)
    (remote, DatasetRef(args.dataset.get))
  }

  def main(args: Arguments): Unit = {
    try {
      val timeout = args.timeoutSeconds.seconds
      args.command match {
        case Some("init") =>
          println("Initializing FiloDB Admin keyspace and tables...")
          parse(metaStore.initialize(), timeout) {
            case Success =>   println("Succeeded.")
          }

        case Some("clearMetadata") =>
          println("WARNING!  Clearing all FiloDB metadata, checkpoints, ingestion configs...")
          parse(metaStore.clearAllData(), timeout) {
            case Success =>   println("Succeeded.")
          }

        case Some("list") =>
          args.host.map { server =>
            listRegisteredDatasets(Client.standaloneClient(system, server, args.port))
          }.getOrElse {
            args.dataset.map(ds => dumpDataset(ds, args.database)).getOrElse(dumpAllDatasets(args.database))
          }

        case Some("create") =>
          require(args.dataset.isDefined &&
                  args.dataColumns.nonEmpty &&
                  args.partitionColumns.nonEmpty, "Need to specify dataset and partition/dataColumns")
          val datasetName = args.dataset.get
          createDataset(getRef(args),
                        args.dataColumns,
                        args.partitionColumns,
                        args.rowKeys,
                        timeout)

        case Some("importcsv") =>
          import org.apache.commons.lang3.StringEscapeUtils._
          val delimiter = unescapeJava(args.delimiter)(0)
          ingestCSV(getRef(args),
                    args.filename.get,
                    delimiter,
                    99.minutes)

        case Some("truncate") =>
          client.truncateDataset(getRef(args), timeout)

        case Some("indexnames") =>
          val (remote, ref) = getClientAndRef(args)
          val names = remote.getIndexNames(ref)
          names.foreach(println)

        case Some("indexvalues") =>
          require(args.indexName.nonEmpty, "--indexName required")
          val (remote, ref) = getClientAndRef(args)
          val values = remote.getIndexValues(ref, args.indexName.get)
          values.foreach(println)

        case Some("status") =>
          val (remote, ref) = getClientAndRef(args)
          dumpShardStatus(remote, ref)

        case Some("setup") =>
          require(args.host.nonEmpty, "--host must be defined")
          val remote = Client.standaloneClient(system, args.host.get, args.port)
          (args.filename, args.configPath) match {
            case (Some(configFile), _) =>
              setupDataset(remote, ConfigFactory.parseFile(new java.io.File(configFile)), timeout)
            case (None, Some(configPath)) =>
              setupDataset(remote, systemConfig.getConfig(configPath), timeout)
            case (None, None) =>
              println("Either --filename or --configPath must be specified for setup")
              exitCode = 1
          }

        case x: Any =>
          args.promql.map { query =>
            require(args.host.nonEmpty && args.dataset.nonEmpty, "--host and --dataset must be defined")
            val remote = Client.standaloneClient(system, args.host.get, args.port)
            parsePromQuery(remote, query, args.dataset.get, args.metricColumn, args.limit)
          }.getOrElse {
            args.select.map { selectCols =>
              exportCSV(getRef(args),
                        selectCols,
                        args.limit,
                        args.outfile)
            }.getOrElse(printHelp)
          }
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

  def dumpDataset(dataset: String, database: Option[String]): Unit = {
    val ref = DatasetRef(dataset, database)
    parse(metaStore.getDataset(ref)) { datasetObj =>
      println(s"Dataset name: ${datasetObj.ref}")
      println(s"Partition columns:\n  ${datasetObj.partitionColumns.mkString("\n  ")}")
      println(s"Data columns:\n  ${datasetObj.dataColumns.mkString("\n  ")}")
      println(s"Row keys:\n  ${datasetObj.rowKeyColumns.mkString("\n  ")}")
      println(s"Options: ${datasetObj.options}\n")
    }
  }

  def dumpAllDatasets(database: Option[String]): Unit = {
    parse(metaStore.getAllDatasets(database)) { refs =>
      refs.foreach { ref => println("%25s\t%s".format(ref.database.getOrElse(""), ref.dataset)) }
    }
  }

  def createDataset(dataset: DatasetRef,
                    dataColumns: Seq[String],
                    partitionColumns: Seq[String],
                    rowKeys: Seq[String],
                    timeout: FiniteDuration): Unit = {
    println(s"Creating dataset $dataset...")

    try {
      val datasetObj = Dataset(dataset.dataset, partitionColumns, dataColumns, rowKeys)
      client.createNewDataset(datasetObj, dataset.database)
      exitCode = 0
    } catch {
      case b: Dataset.BadSchemaError =>
        println(b)
        println(b.getMessage)
        exitCode = 2
      case r: RuntimeException =>
        println(r.getMessage)
        exitCode = 2
    }
  }

  import NodeClusterActor._

  /**
   * setup command configuration example:
   * {{{
   *   dataset = "gdelt"
   *   num-shards = 32   # for Kafka this should match the number of partitions
   *   min-num-nodes = 10     # This many nodes needed to ingest all shards
   *   sourcefactory = "filodb.kafka.KafkaSourceFactory"
   *   sourceconfig {
   *     brokers = ["10.11.12.13", "10.11.12.14"]
   *     topic = "gdelt-events.production"
   *   }
   * }}}
   * sourcefactory and sourceconfig is optional.  If omitted, a NoOpFactory will be used, which means
   * no automatic pull ingestion will be started.  New data can always be pushed into any Filo node.
   */
  def setupDataset(client: LocalClient, config: Config, timeout: FiniteDuration): Unit = {
    val ingestConfig = IngestionConfig(config, backupSourceFactory=noOpSource.streamFactoryClass)
    client.setupDataset(ingestConfig, timeout).foreach {
      case e: ErrorResponse =>
        println(s"Errors setting up dataset ${ingestConfig.ref}: $e")
        exitCode = 2
    }
  }

  def listRegisteredDatasets(client: LocalClient): Unit = {
    client.getDatasets().foreach(println)
  }

  val ShardFormatStr = "%5s\t%20s\t\t%s"

  def dumpShardStatus(client: LocalClient, ref: DatasetRef): Unit = {
    client.getShardMapper(ref) match {
      case Some(map) =>
        println(ShardFormatStr.format("Shard", "Status", "Address"))
        map.shardValues.zipWithIndex.foreach { case ((ref, status), idx) =>
          println(ShardFormatStr.format(idx, status,
                                        Try(ref.path.address).getOrElse("")))
        }
      case _ =>
        println(s"Unable to obtain status for dataset $ref, has it been setup yet?")
    }
  }

  import QueryCommands.QueryResult

  def parsePromQuery(client: LocalClient, query: String, dataset: String,
                     metricCol: String, limit: Int): Unit = {
    val opts = DatasetOptions.DefaultOptions.copy(metricColumn = metricCol)
    val parser = new PromQLParser(query, opts)
    parser.parseToPlan(true) match {
      case SSuccess(plan) =>
        executeQuery(client, dataset, plan, limit)

      case Failure(e: ParseError) =>
        println(s"Failure parsing $query:\n${parser.formatError(e)}")
        exitCode = 2

      case Failure(t: Throwable) => throw t
    }
  }

  def executeQuery(client: LocalClient, dataset: String, plan: LogicalPlan,
                   limit: Int): Unit = {
    val ref = DatasetRef(dataset)
    println(s"Sending query command to server for $ref...")
    println(s"Query Plan:\n$plan")
    try {
      client.logicalPlanQuery(ref, plan) match {
        case QueryResult(_, result) =>
          println(result.schema.columns.map(_.name).mkString("\t"))
          result.prettyPrint(partitionRowLimit=limit).foreach(println)
      }
    } catch {
      case e: ClientException =>
        println(s"ERROR: ${e.getMessage}")
        exitCode = 2
    }
  }

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
          val content: Array[String] = getRowValues(columns, columnCount, r)
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
                columnNames: Seq[String],
                limit: Int,
                outFile: Option[String]): Unit = {
    val datasetObj = parse(metaStore.getDataset(dataset)) { ds => ds }
    val colIDs = datasetObj.colIDs(columnNames: _*).get
    val columns = colIDs.map(id => datasetObj.dataColumns(id))

    // NOTE: we will only return data from the first split!
    val splits = cluster.memStore.getScanSplits(dataset)
    val requiredRows = cluster.memStore.scanRows(datasetObj, colIDs, FilteredPartitionScan(splits.head))
                                  .take(limit)
    writeResult(dataset.dataset, requiredRows, columnNames, columns, outFile)
  }
}
package filodb.cli

import java.io.OutputStream
import java.sql.Timestamp
import javax.activation.UnsupportedDataTypeException

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Try, Failure, Success => SSuccess}

import akka.actor.ActorSystem
import com.opencsv.CSVWriter
import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.typesafe.config.{Config, ConfigFactory}
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._
import org.parboiled2.ParseError
import org.velvia.filo.RowReader

import filodb.coordinator.client._
import filodb.coordinator._
import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, RichProjection}
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store._

// scalastyle:off
class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var database: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
  var configPath: Option[String] = None
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
  var indexName: Option[String] = None
  var host: Option[String] = None
  var port: Int = 2552
  var promql: Option[String] = None
  var metricColumn: String = "__name__"
  var linePerItem: Boolean = false

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
          case "map"    => DataColumn(0, name, dataset, version, MapColumn)
        }
      }.toSeq
    }.getOrElse(Nil)
  }
}

object CliMain extends ArgMain[Arguments] with CsvImportExport with FilodbClusterNode {

  override val role = ClusterRole.Cli

  override lazy val system = ActorSystem(systemName, systemConfig)

  override lazy val cluster = FilodbCluster(system)
  cluster._isInitialized.set(true)

  lazy val client = new LocalClient(coordinatorActor)

  val config = systemConfig.getConfig("filodb")

  import Client.{actorAsk, parse}

  def printHelp() {
    println("filo-cli help:")
    println("  commands: init create importcsv list truncate")
    println("  columns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string,bool,timestamp,map")
    println("  common options:  --dataset --database")
    println("  OR:  --select col1, col2  [--limit <n>]  [--outfile /tmp/out.csv]")
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
          args.host.map { server =>
            listRegisteredDatasets(Client.standaloneClient(system, server, args.port))
          }.getOrElse {
            args.dataset.map(ds => dumpDataset(ds, args.database)).getOrElse(dumpAllDatasets(args.database))
          }

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
              setupDataset(remote, ConfigFactory.parseFile(new java.io.File(configFile)))
            case (None, Some(configPath)) =>
              setupDataset(remote, systemConfig.getConfig(configPath))
            case (None, None) =>
              println("Either --filename or --configPath must be specified for setup")
              exitCode = 1
          }

        case x: Any =>
          args.promql.map { query =>
            require(args.host.nonEmpty && args.dataset.nonEmpty, "--host and --dataset must be defined")
            val remote = Client.standaloneClient(system, args.host.get, args.port)
            parsePromQuery(remote, query, args.dataset.get, args.metricColumn, args.linePerItem)
          }.getOrElse {
            args.select.map { selectCols =>
              exportCSV(getRef(args),
                        version,
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

    try {
      client.createNewDataset(datasetObj, columns, dataset.database)
      exitCode = 0
    } catch {
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
   *   columns = ["GLOBALEVENTID", "MonthYear", "Year", "Actor2Code", "Actor2Name", "Code"]
   *   numshards = 30   # for Kafka this should match the number of partitions
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
  def setupDataset(client: LocalClient, config: Config): Unit = {
    val dataset = DatasetRef(config.getString("dataset"))
    val columns = config.as[Seq[String]]("columns")
    val resourceSpec = DatasetResourceSpec(config.getInt("numshards"),
                                           config.getInt("min-num-nodes"))
    val sourceSpec = config.as[Option[String]]("sourcefactory").map { factory =>
                       IngestionSource(factory, config.getConfig("sourceconfig"))
                     }.getOrElse(noOpSource)
    client.setupDataset(dataset, columns, resourceSpec, sourceSpec).foreach {
      case e: ErrorResponse =>
        println(s"Errors setting up dataset $dataset: $e")
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

  import QueryCommands.{AggregateResponse, QueryArgs}

  def parsePromQuery(client: LocalClient, query: String, dataset: String,
                     metricCol: String, linePerItem: Boolean): Unit = {
    val opts = Dataset.DefaultOptions.copy(metricColumn = metricCol)
    val parser = new PromQLParser(query, opts)
    parser.parseAndGetArgs(true) match {
      // Valid parsed QueryArgs
      case SSuccess(ArgsAndPartSpec(args, partSpec)) =>
        executeQuery(client, dataset, args, partSpec.metricName, partSpec.filters, linePerItem)

      case Failure(e: ParseError) =>
        println(s"Failure parsing $query:\n${parser.formatError(e)}")
        exitCode = 2

      case Failure(t: Throwable) => throw t
    }
  }

  def executeQuery(client: LocalClient, dataset: String, query: QueryArgs,
                   metricName: String, filters: Seq[ColumnFilter], linePerItem: Boolean): Unit = {
    val ref = DatasetRef(dataset)
    println(s"Sending aggregation command to server for $ref...")
    println(s"Query: $query\nFilters: $filters")
    try {
      printAggregate(client.partitionFilterAggregate(ref, query, filters), linePerItem)
    } catch {
      case e: ClientException =>
        println(s"ERROR: ${e.getMessage}")
        exitCode = 2
    }
  }

  def printAggregate(agg: AggregateResponse[_], linePerItem: Boolean): Unit = agg match {
    case AggregateResponse(_, _, array) =>
      if (linePerItem) { array.foreach(println) }
      else             { println(s"[${array.mkString(",")}]") }
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
    val splits = cluster.memStore.getScanSplits(dataset)
    val requiredRows = cluster.memStore.scanRows(richProj, columns, version, FilteredPartitionScan(splits.head))
                                  .take(limit)
    writeResult(dataset.dataset, requiredRows, columnNames, columns, outFile)
  }
}
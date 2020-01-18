package filodb.cli

import java.io.OutputStream
import java.math.BigInteger
import java.sql.Timestamp

import scala.concurrent.duration._
import scala.util.Try

import com.opencsv.CSVWriter
import com.quantifind.sumac.{ArgMain, FieldArgs}
import monix.reactive.Observable
import org.scalactic._

import filodb.coordinator._
import filodb.coordinator.client._
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.coordinator.queryengine2.{PromQlQueryParams, TsdbQueryParams, UnavailablePromQlQueryParams}
import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Column, Schemas}
import filodb.core.store.ChunkSetInfoOnHeap
import filodb.memory.MemFactory
import filodb.memory.format.{BinaryVector, Classes, MemoryReader, RowReader}
import filodb.prometheus.ast.{InMemoryParam, TimeRangeParams, TimeStepParams, WriteBuffersParam}
import filodb.prometheus.parse.Parser
import filodb.query._

// scalastyle:off
class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var database: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
  var configPath: Option[String] = None
  // max # of results returned. Don't make it too high.
  var limit: Int = 200
  var sampleLimit: Int = 1000000
  var timeoutSeconds: Int = 60
  var outfile: Option[String] = None
  var delimiter: String = ","
  var indexName: Option[String] = None
  var host: Option[String] = None
  var port: Int = 2552
  var promql: Option[String] = None
  var schema: Option[String] = None
  var hexPk: Option[String] = None
  var hexVector: Option[String] = None
  var hexChunkInfo: Option[String] = None
  var vectorType: Option[String] = None
  var matcher: Option[String] = None
  var labelNames: Seq[String] = Seq.empty
  var labelFilter: Map[String, String] = Map.empty
  var start: Long = System.currentTimeMillis() / 1000 // promql argument is seconds since epoch
  var end: Long = System.currentTimeMillis() / 1000 // promql argument is seconds since epoch
  var minutes: Option[String] = None
  var step: Long = 10 // in seconds
  var chunks: Option[String] = None   // select either "memory" or "buffers" chunks only
  var everyNSeconds: Option[String] = None
  var shards: Option[Seq[String]] = None
  var spread: Option[Integer] = None
}

object CliMain extends ArgMain[Arguments] with FilodbClusterNode {
  var exitCode = 0

  override val role = ClusterRole.Cli

  lazy val client = new LocalClient(coordinatorActor)

  val config = systemConfig.getConfig("filodb")

  import Client.parse

  def printHelp(): Unit = {
    println("filo-cli help:")
    println("  commands: init create importcsv list clearMetadata")
    println("  dataColumns/partitionColumns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string,bitmap,ts,map")
    println("  common options:  --dataset --database")
    println("  OR:  --select col1, col2  [--limit <n>]  [--outfile /tmp/out.csv]")
    println("\n  NOTE: clearMetadata should NOT be used while FiloDB instances are up\n")
    println("\nStandalone client commands:")
    println("  --host <hostname/IP> [--port ...] --command indexnames --dataset <dataset>")
    println("  --host <hostname/IP> [--port ...] --command indexvalues --indexname <index> --dataset <dataset> --shards SS")
    println("  --host <hostname/IP> [--port ...] [--metricColumn <col>] --dataset <dataset> --promql <query> --start <start> --step <step> --end <end>")
    println("  --host <hostname/IP> [--port ...] --command setup --filename <configFile> | --configPath <path>")
    println("  --host <hostname/IP> [--port ...] --command list")
    println("  --host <hostname/IP> [--port ...] --command status --dataset <dataset>")
    println("  --host <hostname/IP> [--port ...] --command timeseriesMetadata --matcher <matcher-query> --dataset <dataset> --start <start> --end <end>")
    println("  --host <hostname/IP> [--port ...] --command labelValues --labelName <lable-names> --labelFilter <label-filter> --dataset <dataset>")
    println("""  --command promFilterToPartKeyBR --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --schema prom-counter""")
    println("""  --command partKeyBrAsString --hexPk 0x2C0000000F1712000000200000004B8B36940C006D794D65747269634E616D650E00C104006D794E73C004006D795773""")
    println("""  --command decodeChunkInfo --hexChunkInfo 0x12e8253a267ea2db060000005046fc896e0100005046fc896e010000""")
    println("""  --command decodeVector --hexVector 0x1b000000080800000300000000000000010000000700000006080400109836 --vectorType d""")
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

  def getQueryRange(args: Arguments): TimeRangeParams =
    args.chunks.filter { cOpt => cOpt == "memory" || cOpt == "buffers" }
      .map {
        case "memory"  => InMemoryParam(args.step)
        case "buffers" => WriteBuffersParam(args.step)
      }.getOrElse {
      args.minutes.map { minArg =>
        val end = System.currentTimeMillis() / 1000
        TimeStepParams(end - minArg.toInt * 60, args.step, end)
      }.getOrElse(TimeStepParams(args.start, args.step, args.end))
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
          }

        case Some("indexnames") =>
          val (remote, ref) = getClientAndRef(args)
          val names = remote.getIndexNames(ref)
          names.foreach(println)

        case Some("indexvalues") =>
          require(args.indexName.nonEmpty, "--indexName required")
          require(args.shards.nonEmpty, "--shards required")
          val (remote, ref) = getClientAndRef(args)
          val values = remote.getIndexValues(ref, args.indexName.get, args.shards.get.head.toInt, args.limit)
          values.foreach { case (term, freq) => println(f"$term%40s\t$freq") }

        case Some("status") =>
          val (remote, ref) = getClientAndRef(args)
          dumpShardStatus(remote, ref)

        case Some("validateSchemas") => validateSchemas()

        case Some("promFilterToPartKeyBR") =>
          require(args.promql.nonEmpty && args.schema.nonEmpty, "--promql and --schema must be defined")
          promFilterToPartKeyBr(args.promql.get, args.schema.get)

        case Some("partKeyBrAsString") =>
          require(args.hexPk.nonEmpty, "--hexPk must be defined")
          partKeyBrAsString(args.hexPk.get)

        case Some("decodeChunkInfo") =>
          require(args.hexChunkInfo.nonEmpty, "--hexChunkInfo must be defined")
          decodeChunkInfo(args.hexChunkInfo.get)

        case Some("decodeVector") =>
          require(args.hexVector.nonEmpty && args.vectorType.nonEmpty, "--hexVector and --vectorType must be defined")
          decodeVector(args.hexVector.get, args.vectorType.get)

        case Some("timeseriesMetadata") =>
          require(args.host.nonEmpty && args.dataset.nonEmpty && args.matcher.nonEmpty, "--host, --dataset and --matcher must be defined")
          val remote = Client.standaloneClient(system, args.host.get, args.port)
          val options = QOptions(args.limit, args.sampleLimit, args.everyNSeconds.map(_.toInt),
            timeout, args.shards.map(_.map(_.toInt)), args.spread)
          parseTimeSeriesMetadataQuery(remote, args.matcher.get, args.dataset.get,
            getQueryRange(args), options)

        case Some("labelValues") =>
          require(args.host.nonEmpty && args.dataset.nonEmpty && args.labelNames.nonEmpty, "--host, --dataset and --labelName must be defined")
          val remote = Client.standaloneClient(system, args.host.get, args.port)
          val options = QOptions(args.limit, args.sampleLimit, args.everyNSeconds.map(_.toInt),
            timeout, args.shards.map(_.map(_.toInt)), args.spread)
          parseLabelValuesQuery(remote, args.labelNames, args.labelFilter, args.dataset.get,
            getQueryRange(args), options)

        case x: Any =>
          // This will soon be deprecated
          args.promql.map { query =>
            require(args.host.nonEmpty && args.dataset.nonEmpty, "--host and --dataset must be defined")
            val remote = Client.standaloneClient(system, args.host.get, args.port)
            val options = QOptions(args.limit, args.sampleLimit, args.everyNSeconds.map(_.toInt),
              timeout, args.shards.map(_.map(_.toInt)), args.spread)
            parsePromQuery2(remote, query, args.dataset.get, getQueryRange(args), options)
          }
            .getOrElse(printHelp)
      }
    } catch {
      case e: Throwable =>
        println("Uncaught exception:")
        e.printStackTrace()
        exitCode = 2
    } finally {
      // No need to shutdown, just exit.  This ensures things like C* client are not started by accident and
      // ensures a much quicker exit, which is important for CLI.
      sys.exit(exitCode)
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

  def validateSchemas(): Unit = {
    Schemas.fromConfig(config) match {
      case Good(Schemas(partSchema, schemas)) =>
        println("Schema validated.\nPartition schema:")
        partSchema.columns.foreach(c => println(s"\t$c"))
        schemas.foreach { case (schemaName, sch) =>
          println(s"Schema $schemaName (id=${sch.schemaHash}):")
          sch.data.columns.foreach(c => println(s"\t$c"))
          println(s"\n\tDownsamplers: ${sch.data.downsamplers}\n\tDownsample schema: ${sch.data.downsampleSchema}")
        }
      case Bad(errors) =>
        println(s"Schema validation errors:")
        errors.foreach { case (name, err) => println(s"$name\t$err")}
        exitCode = 1
    }
  }

  final case class QOptions(limit: Int, sampleLimit: Int,
                            everyN: Option[Int], timeout: FiniteDuration,
                            shardOverrides: Option[Seq[Int]],
                            spread: Option[Integer])

  def parseTimeSeriesMetadataQuery(client: LocalClient, query: String, dataset: String,
                                   timeParams: TimeRangeParams,
                                   options: QOptions): Unit = {
    val logicalPlan = Parser.metadataQueryToLogicalPlan(query, timeParams)
    executeQuery2(client, dataset, logicalPlan, options, UnavailablePromQlQueryParams)
  }

  def parseLabelValuesQuery(client: LocalClient, labelNames: Seq[String], constraints: Map[String, String], dataset: String,
                            timeParams: TimeRangeParams,
                            options: QOptions): Unit = {
    val logicalPlan = LabelValues(labelNames, constraints, 3.days.toMillis)
    executeQuery2(client, dataset, logicalPlan, options, UnavailablePromQlQueryParams)
  }

  def parsePromQuery2(client: LocalClient, query: String, dataset: String,
                      timeParams: TimeRangeParams,
                      options: QOptions): Unit = {
    val logicalPlan = Parser.queryRangeToLogicalPlan(query, timeParams)
    executeQuery2(client, dataset, logicalPlan, options, PromQlQueryParams(query,timeParams.start, timeParams.step,
      timeParams.end))
  }

  def promFilterToPartKeyBr(query: String, schemaName: String): Unit = {

    import filodb.memory.format.ZeroCopyUTF8String._

    val schema = Schemas.fromConfig(config).get.schemas
                        .getOrElse(schemaName, throw new IllegalArgumentException(s"Invalid schema $schemaName"))
    val exp = Parser.parseFilter(query)
    val rb = new RecordBuilder(MemFactory.onHeapFactory)
    val metricName = exp.metricName.orElse(exp.labelSelection
                                   .find(_.label == "__name__").map(_.value))
                                   .getOrElse(throw new IllegalArgumentException("Metric name not provided"))
    val tags = exp.labelSelection.filterNot(_.label == "__name__").map(lm => (lm.label.utf8, lm.value.utf8)).toMap
    rb.partKeyFromObjects(schema, metricName.utf8, tags)
    rb.currentContainer.get.foreach { case (b, o) =>
      println(schema.partKeySchema.toHexString(b, o).toLowerCase)
    }
  }

  def partKeyBrAsString(brHex: String): Unit = {
    val brHex2 = if (brHex.startsWith("0x")) brHex.substring(2) else brHex
    val array = new BigInteger(brHex2, 16).toByteArray
    val schema = Schemas.fromConfig(config).get.schemas.head._2
    println(schema.partKeySchema.stringify(array))
  }

  def decodeChunkInfo(hexChunkInfo: String): Unit = {
    val ciHex = if (hexChunkInfo.startsWith("0x")) hexChunkInfo.substring(2) else hexChunkInfo
    val array = new BigInteger(ciHex, 16).toByteArray
    val ci = ChunkSetInfoOnHeap(array, Array.empty)
    println(s"ChunkSetInfo id=${ci.id} numRows=${ci.numRows} startTime=${ci.startTime} endTime=${ci.endTime}")
  }

  def decodeVector(hexVector: String, vectorType: String): Unit = {
    val vec = if (hexVector.startsWith("0x")) hexVector.substring(2) else hexVector
    val array = new BigInteger(vec, 16).toByteArray
    val memReader = MemoryReader.fromArray(array)

    val clazz = vectorType match {
      case "i" => Classes.Int
      case "l" => Classes.Long
      case "d" => Classes.Double
      case "h" => Classes.Histogram
      case "s" => Classes.UTF8
      case _   => throw new IllegalArgumentException(s"--vectorType should be one of [i|l|d|h|s]")
    }

    val vecReader = BinaryVector.reader(clazz, memReader, 0)
    val str = vecReader.toHumanReadable(memReader, 0)
    println(vecReader.getClass.getSimpleName)
    println(str)
  }

  def executeQuery2(client: LocalClient, dataset: String, plan: LogicalPlan, options: QOptions, tsdbQueryParams:
  TsdbQueryParams): Unit = {
    val ref = DatasetRef(dataset)
    val spreadProvider: Option[SpreadProvider] = options.spread.map(s => StaticSpreadProvider(SpreadChange(0, s)))
    val qOpts = QueryOptions(spreadProvider, options.sampleLimit)
                             .copy(queryTimeoutSecs = options.timeout.toSeconds.toInt,
                                   shardOverrides = options.shardOverrides)
    println(s"Sending query command to server for $ref with options $qOpts...")
    println(s"Query Plan:\n$plan")
    options.everyN match {
      case Some(intervalSecs) =>
        val fut = Observable.intervalAtFixedRate(intervalSecs.seconds).foreach { n =>
          client.logicalPlan2Query(ref, plan, tsdbQueryParams, qOpts) match {
            case QueryResult(_, schema, result) => result.take(options.limit).foreach(rv => println(rv.prettyPrint()))
            case err: QueryError                => throw new ClientException(err)
          }
        }.recover {
          case e: ClientException => println(s"ERROR: ${e.getMessage}")
                                     exitCode = 2
        }
        while (!fut.isCompleted) { Thread sleep 1000 }
      case None =>
        try {
          client.logicalPlan2Query(ref, plan, tsdbQueryParams, qOpts) match {
            case QueryResult(_, schema, result) => println(s"Output schema: $schema")
                                                   println(s"Number of Range Vectors: ${result.size}")
                                                   result.take(options.limit).foreach(rv => println(rv.prettyPrint()))
            case QueryError(_,ex)               => println(s"QueryError: ${ex.getClass.getSimpleName} ${ex.getMessage}")
          }
        } catch {
          case e: ClientException =>  println(s"ClientException: ${e.getMessage}")
                                      exitCode = 2
        }
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
        case TimestampColumn => rowReader.as[Timestamp](position).toString
        case HistogramColumn => rowReader.getHistogram(position).toString
        case _ => throw new UnsupportedOperationException("Unsupported type: " + columns(position).columnType)
      }
      content.update(position, value)
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

}

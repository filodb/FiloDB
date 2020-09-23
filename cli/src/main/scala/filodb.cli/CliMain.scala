package filodb.cli

import java.io.OutputStream
import java.math.BigInteger
import java.sql.Timestamp

import scala.concurrent.duration._
import scala.util.Try

import com.opencsv.CSVWriter
import monix.reactive.Observable
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.exceptions.ScallopException
import org.scalactic._

import filodb.coordinator._
import filodb.coordinator.client._
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core._
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Column, Schemas}
import filodb.core.query._
import filodb.core.store.ChunkSetInfoOnHeap
import filodb.memory.MemFactory
import filodb.memory.format.{BinaryVector, Classes, MemoryReader, RowReader}
import filodb.prometheus.ast.{InMemoryParam, TimeRangeParams, TimeStepParams, WriteBuffersParam}
import filodb.prometheus.parse.Parser
import filodb.query._

// scalastyle:off
class Arguments(args: Seq[String]) extends ScallopConf(args) {


  val dataset = opt[String]()
  val database = opt[String]()
  val command = opt[String]()
  val filename = opt[String]()
  val configpath = opt[String]()
  // max # of results returned. Don't make it too high.
  val limit = opt[Int](default = Some(200))
  val samplelimit = opt[Int](default = Some(1000000))
  val timeoutseconds = opt[Int](default = Some(60))
  val outfile = opt[String]()
  val indexname = opt[String]()
  val host = opt[String]()
  val port = opt[Int](default = Some(2552))
  val promql = opt[String]()
  val schema = opt[String]()
  val hexpk = opt[String]()
  val hexvector = opt[String]()
  val hexchunkinfo = opt[String]()
  val vectortype = opt[String]()
  val matcher = opt[String]()
  val labelnames = opt[List[String]](default = Some(List()))
  val labelfilter = opt[Map[String, String]](default = Some(Map.empty))
  val currentTime = System.currentTimeMillis()/1000

//  val starts = opt[Long](default = Some(currentTime))
  val start = opt[Long](default = Some(currentTime))// promql argument is seconds since epoch
  val end = opt[Long](default = Some(currentTime))// promql argument is seconds since epoch
  val minutes = opt[String]()
  val step = opt[Long](default = Some(10)) // in seconds
  val chunks = opt[String]()   // select either "memory" or "buffers" chunks only
  val everynseconds = opt[String]()
  val shards = opt[List[String]]()
  val spread = opt[Int]()
  verify()

  override def onError(e: Throwable): Unit = e match {

    case ScallopException(message) => throw e
    case other => throw other
  }

}

object CliMain extends FilodbClusterNode {
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
    println("  --host <hostname/IP> [--port ...]  --dataset <dataset> --promql <query> --start <start> --step <step> --end <end>")
    println("  --host <hostname/IP> [--port ...] --command setup --filename <configFile> | --configpath <path>")
    println("  --host <hostname/IP> [--port ...] --command list")
    println("  --host <hostname/IP> [--port ...] --command status --dataset <dataset>")
    println("  --host <hostname/IP> [--port ...] --command labelvalues --labelName <lable-names> --labelfilter <label-filter> --dataset <dataset>")
    println("""  --command promFilterToPartKeyBR --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --schema prom-counter""")
    println("""  --command partKeyBrAsString --hexpk 0x2C0000000F1712000000200000004B8B36940C006D794D65747269634E616D650E00C104006D794E73C004006D795773""")
    println("""  --command decodeChunkInfo --hexchunkinfo 0x12e8253a267ea2db060000005046fc896e0100005046fc896e010000""")
    println("""  --command decodeVector --hexvector 0x1b000000080800000300000000000000010000000700000006080400109836 --vectortype d""")
    println("\nTo change config: pass -Dconfig.file=/path/to/config as first arg or set $FILO_CONFIG_FILE")
    println("  or override any config by passing -Dconfig.path=newvalue as first args")
    println("\nFor detailed debugging, uncomment the TRACE/DEBUG loggers in logback.xml and add these ")
    println("  options:  ./filo-cli -Dakka.loglevel=DEBUG -Dakka.actor.debug.receive=on -Dakka.actor.debug.autoreceive=on --command importcsv ...")
  }

  def getRef(args: Arguments): DatasetRef = DatasetRef(args.dataset(), args.database.toOption)

  def getClientAndRef(args: Arguments): (LocalClient, DatasetRef) = {
    require(args.host.isDefined && args.dataset.isDefined, "--host and --dataset must be defined")
    val remote = Client.standaloneClient(system, args.host(), args.port())
    (remote, DatasetRef(args.dataset()))
  }

  def getQueryRange(args: Arguments): TimeRangeParams =
    args.chunks.filter { cOpt => cOpt == "memory" || cOpt == "buffers" }
      .map {
        case "memory"  => InMemoryParam(args.step())
        case "buffers" => WriteBuffersParam(args.step())
      }.getOrElse {
      args.minutes.map { minArg =>
        val end = System.currentTimeMillis() / 1000
        TimeStepParams(end - minArg.toInt * 60, args.step(), end)
      }.getOrElse(TimeStepParams(args.start(), args.step(), args.end()))
    }

  def main(rawArgs: Array[String]): Unit = {
    val args = new Arguments(rawArgs)
    try {
      val timeout = args.timeoutseconds().seconds
      args.command.toOption match {
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
            listRegisteredDatasets(Client.standaloneClient(system, server, args.port()))
          }

        case Some("indexnames") =>
          val (remote, ref) = getClientAndRef(args)
          val names = remote.getIndexNames(ref)
          names.foreach(println)

        case Some("indexvalues") =>
          require(args.indexname.isDefined, "--indexName required")
          require(args.shards.isDefined, "--shards required")
          val (remote, ref) = getClientAndRef(args)
          val values = remote.getIndexValues(ref, args.indexname(), args.shards().head.toInt, args.limit())
          values.foreach { case (term, freq) => println(f"$term%40s\t$freq") }

        case Some("status") =>
          val (remote, ref) = getClientAndRef(args)
          dumpShardStatus(remote, ref)

        case Some("validateSchemas") => validateSchemas()

        case Some("promFilterToPartKeyBR") =>
          require(args.promql.isDefined && args.schema.isDefined, "--promql and --schema must be defined")
          promFilterToPartKeyBr(args.promql(), args.schema())

        case Some("partKeyBrAsString") =>
          require(args.hexpk.isDefined, "--hexPk must be defined")
          partKeyBrAsString(args.hexpk())

        case Some("decodeChunkInfo") =>
          require(args.hexchunkinfo.isDefined, "--hexChunkInfo must be defined")
          decodeChunkInfo(args.hexchunkinfo())

        case Some("decodeVector") =>
          require(args.hexvector.isDefined && args.vectortype.isDefined, "--hexVector and --vectorType must be defined")
          decodeVector(args.hexvector(), args.vectortype())

        case Some("timeseriesMetadata") =>
          require(args.host.isDefined && args.dataset.isDefined && args.matcher.isDefined, "--host, --dataset and --matcher must be defined")
          val remote = Client.standaloneClient(system, args.host(), args.port())
          val options = QOptions(args.limit(), args.samplelimit(), args.everynseconds.map(_.toInt).toOption,
            timeout, args.shards.map(_.map(_.toInt)).toOption, args.spread.toOption.map(Integer.valueOf))
          parseTimeSeriesMetadataQuery(remote, args.matcher(), args.dataset(),
            getQueryRange(args), true, options)

        case Some("labelValues") =>
          require(args.host.isDefined && args.dataset.isDefined && args.labelnames.isDefined, "--host, --dataset and --labelName must be defined")
          val remote = Client.standaloneClient(system, args.host(), args.port())
          val options = QOptions(args.limit(), args.samplelimit(), args.everynseconds.map(_.toInt).toOption,
            timeout, args.shards.map(_.map(_.toInt)).toOption, args.spread.toOption.map(Integer.valueOf))
          parseLabelValuesQuery(remote, args.labelnames(), args.labelfilter(), args.dataset(),
            getQueryRange(args), options)

        case x: Any =>
          // This will soon be deprecated
          args.promql.map { query =>
            require(args.host.isDefined && args.dataset.isDefined, "--host and --dataset must be defined")
            val remote = Client.standaloneClient(system, args.host(), args.port())
            val options = QOptions(args.limit(), args.samplelimit(), args.everynseconds.toOption.map(_.toInt),
              timeout, args.shards.toOption.map(_.map(_.toInt)), args.spread.toOption.map(Integer.valueOf))
            parsePromQuery2(remote, query, args.dataset(), getQueryRange(args), options)
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
                                   fetchFirstLastSampleTimes: Boolean,
                                   options: QOptions): Unit = {
    val logicalPlan = Parser.metadataQueryToLogicalPlan(query, timeParams, fetchFirstLastSampleTimes)
    executeQuery2(client, dataset, logicalPlan, options, UnavailablePromQlQueryParams)
  }

  def parseLabelValuesQuery(client: LocalClient, labelNames: Seq[String], constraints: Map[String, String], dataset: String,
                            timeParams: TimeRangeParams,
                            options: QOptions): Unit = {
    // TODO support all filters
    val filters = constraints.map { case (k, v) => ColumnFilter(k, Filter.Equals(v)) }.toSeq
    val logicalPlan = LabelValues(labelNames, filters, timeParams.start * 1000, timeParams.end * 1000)
    executeQuery2(client, dataset, logicalPlan, options, UnavailablePromQlQueryParams)
  }

  def parsePromQuery2(client: LocalClient, query: String, dataset: String,
                      timeParams: TimeRangeParams,
                      options: QOptions): Unit = {
    val logicalPlan = Parser.queryRangeToLogicalPlan(query, timeParams)
    // Routing is not supported with CLI
    executeQuery2(client, dataset, logicalPlan, options,
      PromQlQueryParams(query,timeParams.start, timeParams.step, timeParams.end))
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
    val str = vecReader.debugString(memReader, 0)
    println(vecReader.getClass.getSimpleName)
    println(str)
  }

  def executeQuery2(client: LocalClient, dataset: String, plan: LogicalPlan,
                    options: QOptions, tsdbQueryParams: TsdbQueryParams): Unit = {
    val ref = DatasetRef(dataset)
    val spreadProvider: Option[SpreadProvider] = options.spread.map(s => StaticSpreadProvider(SpreadChange(0, s)))
    val qOpts = QueryContext(tsdbQueryParams, spreadProvider, options.sampleLimit)
                             .copy(queryTimeoutMillis = options.timeout.toMillis.toInt,
                                   shardOverrides = options.shardOverrides)
    println(s"Sending query command to server for $ref with options $qOpts...")
    println(s"Query Plan:\n$plan")
    options.everyN match {
      case Some(intervalSecs) =>
        val fut = Observable.intervalAtFixedRate(intervalSecs.seconds).foreach { n =>
          client.logicalPlan2Query(ref, plan, qOpts) match {
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
          client.logicalPlan2Query(ref, plan, qOpts) match {
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

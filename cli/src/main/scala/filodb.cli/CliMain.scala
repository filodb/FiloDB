package filodb.cli

import akka.actor.ActorSystem
import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core.cassandra.CassandraDatastore
import filodb.core.datastore.Datastore
import filodb.core.ingest.CoordinatorActor
import filodb.core.messages._
import filodb.core.metadata.{Column, Partition}

//scalastyle:off
class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var partition: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
  var columns: Option[Map[String, String]] = None
  var version: Option[Int] = None
  var select: Option[Seq[String]] = None
  var limit: Int = 1000
  var outfile: Option[String] = None

  import Column.ColumnType._

  def toColumns(dataset: String, version: Int): Seq[Column] = {
    columns.map { colStrStr =>
      colStrStr.map { case (name, colType) =>
        colType match {
          case "int"    => Column(name, dataset, version, IntColumn)
          case "long"   => Column(name, dataset, version, LongColumn)
          case "double" => Column(name, dataset, version, DoubleColumn)
          case "string" => Column(name, dataset, version, StringColumn)
        }
      }.toSeq
    }.getOrElse(Nil)
  }
}

object CliMain extends ArgMain[Arguments] with CsvImportExport {
  // TODO: allow user to pass in config
  // TODO: get config from default reference/application.conf
  val CassConfigStr = """
                   | max-outstanding-futures = 128
                   """.stripMargin

  val system = ActorSystem("filo-cli")
  val datastore = new CassandraDatastore(ConfigFactory.parseString(CassConfigStr))
  val coordinator = system.actorOf(CoordinatorActor.props(datastore))

  def printHelp() {
    println("filo-cli help:")
    println("  commands: create importcsv list")
    println("  columns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string")
    println("  OR:  --select col1, col2  [--limit <n>]  [--outfile /tmp/out.csv]")
  }

  def main(args: Arguments) {
    try {
      val version = args.version.getOrElse(0)
      args.command match {
        case Some("list") =>
          args.dataset.map(dumpDataset).getOrElse(dumpAllDatasets())
        case Some("create") =>
          require(args.dataset.isDefined, "Need to specify a dataset")
          require(args.partition.isDefined || args.columns.isDefined, "Need --partition or --columns")
          val datasetName = args.dataset.get
          args.columns.map { colmap =>
            createDatasetAndColumns(datasetName, args.toColumns(datasetName, version))
          }.getOrElse(createPartition(datasetName, args.partition.get))
        case Some("importcsv") =>
          ingestCSV(args.dataset.get,
                    args.partition.get,
                    version,
                    args.filename.get)
        case x: Any =>
          args.select.map { selectCols =>
            exportCSV(args.dataset.get,
                      args.partition.get,
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
      system.shutdown()
      com.websudos.phantom.Manager.shutdown()
      sys.exit(exitCode)
    }
  }

  def dumpDataset(dataset: String) {
    parseResponse(datastore.getDataset(dataset, 200)) {
      case Datastore.TheDataset(datasetObj) =>
        println(s"Dataset name: ${datasetObj.name}")
        println("Partitions: " + datasetObj.partitions.mkString(", "))
      case NotFound =>
        println(s"Dataset $dataset not found!")
        return
    }
    parseResponse(datastore.getSchema(dataset, Int.MaxValue)) {
      case Datastore.TheSchema(schema) =>
        println("Columns:")
        schema.values.foreach { case Column(name, _, ver, colType, _, _, _) =>
          println("  %-35.35s %5d %s".format(name, ver, colType))
        }
    }
  }

  def dumpAllDatasets() { println("TODO") }

  def createDatasetAndColumns(dataset: String, columns: Seq[Column]) {
    println(s"Creating dataset $dataset...")
    awaitSuccess(datastore.newDataset(dataset))
    columns.foreach { col =>
      println(s"Creating column $col...")
      awaitSuccess(datastore.newColumn(col))
    }
  }

  def createPartition(dataset: String, partitionName: String) {
    println(s"Creating partition $partitionName for dataset $dataset...")
    awaitSuccess(datastore.newPartition(Partition(dataset, partitionName)))
  }
}
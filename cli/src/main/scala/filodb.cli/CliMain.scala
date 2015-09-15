package filodb.cli

import akka.actor.ActorSystem
import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.cassandra.metastore.CassandraMetaStore
import filodb.coordinator.{CoordinatorActor, DefaultCoordinatorSetup}
import filodb.core._
import filodb.core.metadata.{Column, Dataset}

//scalastyle:off
class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
  var columns: Option[Map[String, String]] = None
  var sortColumn: String = "NOT_A_COLUMN"
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

object CliMain extends ArgMain[Arguments] with CsvImportExport with DefaultCoordinatorSetup {

  val system = ActorSystem("filo-cli")
  val config = ConfigFactory.load
  lazy val columnStore = new CassandraColumnStore(config)
  lazy val metaStore = new CassandraMetaStore(config)

  def printHelp() {
    println("filo-cli help:")
    println("  commands: init create importcsv list")
    println("  columns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string")
    println("  OR:  --select col1, col2  [--limit <n>]  [--outfile /tmp/out.csv]")
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
          val datasetName = args.dataset.get
          createDatasetAndColumns(datasetName, args.toColumns(datasetName, version), args.sortColumn)
        case Some("importcsv") =>
          ingestCSV(args.dataset.get,
                    version,
                    args.filename.get)
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
      system.shutdown()
      com.websudos.phantom.Manager.shutdown()
      sys.exit(exitCode)
    }
  }

  def dumpDataset(dataset: String) {
    parse(metaStore.getDataset(dataset)) { datasetObj =>
      println(s"Dataset name: ${datasetObj.name}")
      println(s"Partition Column: ${datasetObj.partitionColumn}")
      println(s"Options: ${datasetObj.options}\n")
      datasetObj.projections.foreach(println)
    }
    parse(metaStore.getSchema(dataset, Int.MaxValue)) { schema =>
      println("Columns:")
      schema.values.foreach { case Column(name, _, ver, colType, _, _, _) =>
        println("  %-35.35s %5d %s".format(name, ver, colType))
      }
    }
  }

  def dumpAllDatasets() { println("TODO") }

  def createDatasetAndColumns(dataset: String,
                              columns: Seq[Column],
                              sortColumn: String) {
    if (!columns.find(_.name == sortColumn).isDefined) {
      println(s"SortColumn $sortColumn is not amongst list of columns")
      exitCode = 1
      return
    }
    println(s"Creating dataset $dataset with sort column $sortColumn...")
    val datasetObj = Dataset(dataset, sortColumn)
    actorAsk(coordinatorActor, CoordinatorActor.CreateDataset(datasetObj, columns)) {
      case CoordinatorActor.DatasetCreated =>
        println(s"Dataset $dataset created!")
        exitCode = 0
      case CoordinatorActor.DatasetError(errMsg) =>
        println(s"Error creating dataset $dataset: $errMsg")
        exitCode = 2
    }
  }
}
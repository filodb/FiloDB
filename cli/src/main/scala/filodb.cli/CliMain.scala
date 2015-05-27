package filodb.cli

import akka.actor.ActorSystem
import com.quantifind.sumac.{ArgMain, FieldArgs}
import com.typesafe.config.ConfigFactory
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core.cassandra.CassandraDatastore
import filodb.core.datastore.Datastore
import filodb.core.ingest.CoordinatorActor
import filodb.core.messages._

class Arguments extends FieldArgs {
  var dataset: Option[String] = None
  var partition: Option[String] = None
  var command: Option[String] = None
  var filename: Option[String] = None
}

object CliMain extends ArgMain[Arguments] {
  // TODO: allow user to pass in config
  // TODO: get config from default reference/application.conf
  val CassConfigStr = """
                   | max-outstanding-futures = 128
                   """.stripMargin

  val system = ActorSystem("filo-cli")
  val datastore = new CassandraDatastore(ConfigFactory.parseString(CassConfigStr))
  val coordinator = system.actorOf(CoordinatorActor.props(datastore))

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  var exitCode = 0

  def printHelp() {
    println("filo-cli help:")
    println("  commands: create import list")
    println("  columns: <colName1>:<type1>,<colName2>:<type2>,... ")
    println("  types:  int,long,double,string")
  }

  def main(args: Arguments) {
    try {
      args.command match {
        case Some("list") =>
          args.dataset.map(dumpDataset).getOrElse(dumpAllDatasets())
        case x: Any => printHelp
      }
    } finally {
      system.shutdown()
      sys.exit(exitCode)
    }
  }

  private def parseResponse(cmd: => Future[Response])(handler: PartialFunction[Response, Unit]) {
    Await.result(cmd, 5 seconds) match {
      case e: ErrorResponse =>
        println("ERROR: " + e)
        exitCode = 1
      case r: Response => handler(r)
    }
  }

  def dumpDataset(dataset: String) {
    parseResponse(datastore.getDataset(dataset)) {
      case Datastore.TheDataset(datasetObj) =>
        println(s"Dataset name: ${datasetObj.name}")
        println("Partitions: " + datasetObj.partitions.mkString(", "))
      case NotFound =>
        println(s"Dataset $dataset not found!")
    }
  }

  def dumpAllDatasets() { println("TODO") }
}
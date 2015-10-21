package filodb.cli

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.opencsv.{CSVReader, CSVWriter}
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core.metadata.MetaStore
import filodb.coordinator.{NodeCoordinatorActor, RowSource}
import filodb.coordinator.sources.CsvSourceActor
import filodb.core._

// Turn off style rules for CLI classes
//scalastyle:off
trait CsvImportExport {
  def system: ActorSystem
  val metaStore: MetaStore
  val coordinatorActor: ActorRef
  var exitCode = 0

  implicit val ec: ExecutionContext
  import scala.collection.JavaConversions._

  protected def parseResponse[B](cmd: => Future[Response])(handler: PartialFunction[Response, B]): B = {
    Await.result(cmd, 15 seconds) match {
      case e: ErrorResponse =>
        println("ERROR: " + e)
        exitCode = 1
        null.asInstanceOf[B]
      case r: Response => handler(r)
    }
  }

  protected def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 5 seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

  protected def actorAsk[B](actor: ActorRef, msg: Any,
                            askTimeout: FiniteDuration = 5 seconds)(f: PartialFunction[Any, B]): B = {
    implicit val timeout = Timeout(askTimeout)
    parse(actor ? msg, askTimeout)(f)
  }

  protected def awaitSuccess(cmd: => Future[Response]): Unit = {
    parseResponse(cmd) {
      case Success =>   println("Succeeded.")
    }
  }

  def ingestCSV(dataset: String,
                version: Int,
                csvPath: String,
                delimiter: Char,
                timeout: FiniteDuration): Unit = {
    val fileReader = new java.io.FileReader(csvPath)

    // TODO: consider using a supervisor actor to start these
    val csvActor = system.actorOf(CsvSourceActor.props(fileReader, dataset, version, coordinatorActor))
    actorAsk(csvActor, RowSource.Start, timeout) {
      case RowSource.SetupError(e) =>
        println(s"Error $e setting up CSV ingestion of $dataset/$version at $csvPath")
        exitCode = 2
        return
      case RowSource.AllDone =>
    }

    println("ingestCSV finished!")
    exitCode = 0
  }
}

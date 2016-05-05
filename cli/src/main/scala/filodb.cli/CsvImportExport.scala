package filodb.cli

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.opencsv.{CSVReader, CSVWriter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.coordinator.client.{Client, LocalClient}
import filodb.coordinator.sources.CsvSourceActor
import filodb.coordinator.RowSource
import filodb.core._
import filodb.core.store.MetaStore

// Turn off style rules for CLI classes
//scalastyle:off
trait CsvImportExport extends StrictLogging {
  def system: ActorSystem
  val metaStore: MetaStore
  def coordinatorActor: ActorRef
  def client: LocalClient
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

  protected def awaitSuccess(cmd: => Future[Response]): Unit = {
    parseResponse(cmd) {
      case Success =>   println("Succeeded.")
    }
  }

  def ingestCSV(dataset: DatasetRef,
                version: Int,
                csvPath: String,
                delimiter: Char,
                timeout: FiniteDuration): Unit = {
    val fileReader = new java.io.FileReader(csvPath)

    // TODO: consider using a supervisor actor to start these
    val csvActor = system.actorOf(CsvSourceActor.props(fileReader, dataset, version, coordinatorActor))
    Client.actorAsk(csvActor, RowSource.Start, timeout) {
      case RowSource.SetupError(e) =>
        println(s"Error $e setting up CSV ingestion of $dataset/$version at $csvPath")
        exitCode = 2
        return
      case RowSource.AllDone =>
    }

    // There might still be rows left after the latest flush is done, so initiate another flush
    val activeRows = client.ingestionStats(dataset, version).headOption.map(_.numRowsActive).getOrElse(-1)
    if (activeRows > 0) {
      logger.info(s"Still $activeRows left to flush in active memTable, triggering flush....")
      client.flush(dataset, version, timeout)
    } else if (activeRows < 0) {
      logger.warn(s"Unable to obtain any stats from ingestion, something is wrong.")
    }

    println(s"Ingestion of $csvPath finished!")
    exitCode = 0
  }
}

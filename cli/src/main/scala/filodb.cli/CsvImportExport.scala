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
import filodb.core.reprojector.{MemTable, Scheduler}
import filodb.coordinator.NodeCoordinatorActor
import filodb.coordinator.sources.CsvSourceActor
import filodb.core._

// Turn off style rules for CLI classes
//scalastyle:off
trait CsvImportExport {
  val system: ActorSystem
  val metaStore: MetaStore
  val memTable: MemTable
  val scheduler: Scheduler
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

  protected def awaitSuccess(cmd: => Future[Response]) {
    parseResponse(cmd) {
      case Success =>   println("Succeeded.")
    }
  }

  def ingestCSV(dataset: String, version: Int, csvPath: String, delimiter: Char) {
    val fileReader = new java.io.FileReader(csvPath)

    val reader = new CSVReader(fileReader, delimiter)
    val columns = reader.readNext.toSeq
    println(s"Ingesting CSV at $csvPath with columns $columns...")

    val ingestCmd = NodeCoordinatorActor.SetupIngestion(dataset, columns, version: Int)
    actorAsk(coordinatorActor, ingestCmd, 10 seconds) {
      case NodeCoordinatorActor.IngestionReady =>
      case NodeCoordinatorActor.UnknownDataset =>
        println(s"Dataset $dataset is not known, you need to --create it first!")
        exitCode = 2
        return
      case NodeCoordinatorActor.UndefinedColumns(undefCols) =>
        println(s"Some columns $undefCols are not defined, please define them with --create first!")
        exitCode = 2
        return
      case NodeCoordinatorActor.BadSchema(msg) =>
        println(s"BadSchema - $msg")
        exitCode = 2
        return
    }

    var linesIngested = 0
    reader.iterator.grouped(100).foreach { lines =>
      val mappedLines = lines.map(ArrayStringRowReader)
      var resp: MemTable.IngestionResponse = MemTable.PleaseWait
      do {
        resp = memTable.ingestRows(dataset, version, mappedLines)
        if (resp == MemTable.PleaseWait) {
          do {
            println("Waiting for MemTable to be able to ingest again...")
            Thread sleep 10000
          } while (!memTable.canIngest(dataset, version))
        }
      } while (resp != MemTable.Ingested)
      linesIngested += mappedLines.length
      if (linesIngested % 10000 == 0) println(s"Ingested $linesIngested lines!")
    }

    coordinatorActor ! NodeCoordinatorActor.Flush(dataset, version)
    println("Waiting for scheduler/memTable to finish flushing everything")
    Thread sleep 5000
    while (memTable.flushingDatasets.nonEmpty) {
      print(".")
      Thread sleep 1000
    }
    println("ingestCSV finished!")
    exitCode = 0
  }
}

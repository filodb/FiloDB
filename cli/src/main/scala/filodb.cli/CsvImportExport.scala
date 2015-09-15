package filodb.cli

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.opencsv.CSVWriter
import org.velvia.filo.{ArrayStringRowSetter, RowSetter}
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core.metadata.MetaStore
import filodb.coordinator.RowSource
import filodb.coordinator.sources.CsvSourceActor
import filodb.core._

// Turn off style rules for CLI classes
//scalastyle:off
trait CsvImportExport {
  val system: ActorSystem
  val metaStore: MetaStore
  val coordinatorActor: ActorRef
  var exitCode = 0

  implicit val ec: ExecutionContext

  protected def parseResponse[B](cmd: => Future[Response])(handler: PartialFunction[Response, B]): B = {
    Await.result(cmd, 5 seconds) match {
      case e: ErrorResponse =>
        println("ERROR: " + e)
        exitCode = 1
        null.asInstanceOf[B]
      case r: Response => handler(r)
    }
  }

  protected def parse[T, B](cmd: => Future[T])(func: T => B): B = {
    func(Await.result(cmd, 5 seconds))
  }

  protected def actorAsk[B](actor: ActorRef, msg: Any)(f: PartialFunction[Any, B]): B = {
    implicit val timeout = Timeout(5 seconds)
    parse(actor ? msg)(f)
  }

  protected def awaitSuccess(cmd: => Future[Response]) {
    parseResponse(cmd) {
      case Success =>   println("Succeeded.")
    }
  }

  def ingestCSV(dataset: String, version: Int, csvPath: String) {
    val fileReader = new java.io.FileReader(csvPath)
    println("Ingesting CSV at " + csvPath)
    val csvActor = system.actorOf(CsvSourceActor.props(fileReader, dataset, version, coordinatorActor))
    implicit val timeout = Timeout(60 minutes)
    Await.result(csvActor ? RowSource.Start, 61 minutes)
  }

  def exportCSV(dataset: String, version: Int,
                columnNames: Seq[String], limit: Int,
                outFile: Option[String]) {
    val columns = parse(metaStore.getSchema(dataset, version)) { schema =>
      columnNames.map(schema)
    }

    val outStream = outFile.map(new java.io.FileOutputStream(_)).getOrElse(System.out)
    val writer = new CSVWriter(new java.io.OutputStreamWriter(outStream))
    writer.writeNext(columnNames.toArray, false)

    println("Sorry, exportCSV functionality is temporarily unavailable")

    // val extractor = new ReadRowExtractor(datastore, partObj, version, columns, ArrayStringRowSetter)(system)
    // val row = Array.fill(columns.length)("")
    // var rowNo = 0
    // while (rowNo < limit && extractor.hasNext) {
    //   extractor.next(row)
    //   writer.writeNext(row, false)
    //   rowNo += 1
    // }
    // writer.flush()
  }
}

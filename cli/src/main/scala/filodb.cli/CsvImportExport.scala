package filodb.cli

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.opencsv.CSVWriter
import org.velvia.filo.{ArrayStringRowSetter, RowSetter}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core.datastore.{Datastore, ReadRowExtractor}
import filodb.core.ingest.RowSource
import filodb.core.ingest.sources.CsvSourceActor
import filodb.core.messages._

// Turn off style rules for CLI classes
//scalastyle:off
trait CsvImportExport {
  val system: ActorSystem
  val coordinator: ActorRef
  val datastore: Datastore
  var exitCode = 0

  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  protected def parseResponse[B](cmd: => Future[Response])(handler: PartialFunction[Response, B]): B = {
    Await.result(cmd, 5 seconds) match {
      case e: ErrorResponse =>
        println("ERROR: " + e)
        exitCode = 1
        null.asInstanceOf[B]
      case r: Response => handler(r)
    }
  }

  protected def awaitSuccess(cmd: => Future[Response]) {
    parseResponse(cmd) {
      case Success =>   println("Succeeded.")
    }
  }

  def ingestCSV(dataset: String, partition: String, version: Int, csvPath: String) {
    val fileReader = new java.io.FileReader(csvPath)
    println("Ingesting CSV at " + csvPath)
    val csvActor = system.actorOf(CsvSourceActor.props(fileReader, dataset, partition, version, coordinator))
    implicit val timeout = Timeout(60 minutes)
    Await.result(csvActor ? RowSource.Start, 61 minutes)
  }

  def exportCSV(dataset: String, partition: String, version: Int,
                columnNames: Seq[String], limit: Int) {
    val partObj = parseResponse(datastore.getPartition(dataset, partition)) {
      case Datastore.ThePartition(partObj) => partObj
    }
    val columns = parseResponse(datastore.getSchema(dataset, version)) {
      case Datastore.TheSchema(schema) => columnNames.map(schema)
    }

    val writer = new CSVWriter(new java.io.OutputStreamWriter(System.out))
    writer.writeNext(columnNames.toArray, false)

    val extractor = new ReadRowExtractor(datastore, partObj, version, columns, ArrayStringRowSetter)(system)
    val row = Array.fill(columns.length)("")
    var rowNo = 0
    while (rowNo < limit && extractor.hasNext) {
      extractor.next(row)
      writer.writeNext(row, false)
      rowNo += 1
    }
    writer.flush()
  }
}

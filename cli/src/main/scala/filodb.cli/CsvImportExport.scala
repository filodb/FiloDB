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
import filodb.core.reprojector.MemTable
import filodb.coordinator.CoordinatorActor
import filodb.coordinator.sources.CsvSourceActor
import filodb.core._

// Turn off style rules for CLI classes
//scalastyle:off
trait CsvImportExport {
  val system: ActorSystem
  val metaStore: MetaStore
  val memTable: MemTable
  val coordinatorActor: ActorRef
  var exitCode = 0

  implicit val ec: ExecutionContext
  import scala.collection.JavaConversions._

  protected def parseResponse[B](cmd: => Future[Response])(handler: PartialFunction[Response, B]): B = {
    Await.result(cmd, 5 seconds) match {
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

  def ingestCSV(dataset: String, version: Int, csvPath: String) {
    val fileReader = new java.io.FileReader(csvPath)

    val reader = new CSVReader(fileReader, ',')
    val columns = reader.readNext.toSeq
    println(s"Ingesting CSV at $csvPath with columns $columns...")

    val ingestCmd = CoordinatorActor.SetupIngestion(dataset, columns, version: Int)
    actorAsk(coordinatorActor, ingestCmd, 10 seconds) {
      case CoordinatorActor.IngestionReady =>
      case CoordinatorActor.UnknownDataset =>
        println(s"Dataset $dataset is not known, you need to --create it first!")
        exitCode = 2
        return
      case CoordinatorActor.UndefinedColumns(undefCols) =>
        println(s"Some columns $undefCols are not defined, please define them with --create first!")
        exitCode = 2
        return
      case CoordinatorActor.BadSchema(msg) =>
        println(s"BadSchema - $msg")
        exitCode = 2
        return
    }

    var linesIngested = 0
    reader.iterator.grouped(100).foreach { lines =>
      val mappedLines = lines.toSeq.map(ArrayStringRowReader)
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
    // val csvActor = system.actorOf(CsvSourceActor.props(fileReader, dataset, version, coordinatorActor))
    // actorAsk(csvActor, RowSource.Start, 61 minutes) {
    //   case RowSource.SetupError(err) =>
    //     println(s"ERROR: $err")
    //     exitCode = 2
    //   case RowSource.AllDone =>
        println("Waiting for scheduler/memTable to finish flushing everything")
        Thread sleep 5000
        while (memTable.flushingDatasets.nonEmpty) {
          print(".")
          Thread sleep 1000
        }
        println("ingestCSV finished!")
        exitCode = 0
    // }
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

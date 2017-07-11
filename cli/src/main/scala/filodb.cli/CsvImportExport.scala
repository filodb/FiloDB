package filodb.cli

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.opencsv.{CSVReader, CSVWriter}
import com.typesafe.scalalogging.StrictLogging
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.coordinator.client.{Client, LocalClient}
import filodb.coordinator.sources.CsvSourceActor
import filodb.coordinator.{NodeClusterActor, RowSource}
import filodb.core._
import filodb.core.store.MetaStore
import filodb.core.metadata.RichProjection

// Turn off style rules for CLI classes
//scalastyle:off
trait CsvImportExport extends StrictLogging {
  def system: ActorSystem
  def metaStore: MetaStore
  def coordinatorActor: ActorRef
  def client: LocalClient
  var exitCode = 0

  implicit val ec: ExecutionContext
  import scala.collection.JavaConversions._

  def ingestCSV(dataset: DatasetRef,
                version: Int,
                csvPath: String,
                delimiter: Char,
                timeout: FiniteDuration): Unit = {
    val fileReader = new java.io.FileReader(csvPath)

    val datasetObj = Client.parse(metaStore.getDataset(dataset)) { ds => ds }
    val schema = Client.parse(metaStore.getSchema(dataset, version)) { c => c }

    val (projection, reader, columnNames) =
      CsvSourceActor.getProjectionFromHeader(fileReader, datasetObj, schema, delimiter)
    client.setupIngestion(projection.datasetRef, columnNames, version) match {
      case Nil =>
        println(s"Ingestion set up for $dataset / $version, starting...")
      case errs: Seq[ErrorResponse] =>
        println(s"Errors setting up ingestion: $errs")
        exitCode = 2
        return
    }

    val clusterActor = system.actorOf(NodeClusterActor.singleNodeProps(coordinatorActor))
    val csvActor = system.actorOf(CsvSourceActor.propsNoHeader(reader, projection, version, clusterActor))
    Client.actorAsk(csvActor, RowSource.Start, timeout) {
      case RowSource.IngestionErr(msg, optErr) =>
        println(s"Error $msg setting up CSV ingestion of $dataset/$version at $csvPath")
        optErr.foreach { e => println(s"Error details:  $e") }
        exitCode = 2
        return
      case RowSource.AllDone =>
    }

    client.flushCompletely(projection.datasetRef, version, timeout)

    println(s"Ingestion of $csvPath finished!")
    exitCode = 0
  }
}

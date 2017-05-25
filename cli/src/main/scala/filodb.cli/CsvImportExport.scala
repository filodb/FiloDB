package filodb.cli

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.{ArrayStringRowReader, RowReader}
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.coordinator.client.{Client, LocalClient}
import filodb.coordinator.sources.{CsvSourceActor, CsvSourceFactory}
import filodb.coordinator.NodeClusterActor
import filodb.core._
import filodb.core.store.MetaStore
import filodb.core.metadata.RichProjection

// Turn off style rules for CLI classes
// scalastyle:off
trait CsvImportExport extends StrictLogging {
  def system: ActorSystem
  def metaStore: MetaStore
  def coordinatorActor: ActorRef
  def client: LocalClient
  var exitCode = 0

  implicit val ec: ExecutionContext
  import scala.collection.JavaConversions._
  import NodeClusterActor._

  def ingestCSV(dataset: DatasetRef,
                version: Int,
                csvPath: String,
                delimiter: Char,
                timeout: FiniteDuration): Unit = {
    val fileReader = new java.io.FileReader(csvPath)
    val headerCols = CsvSourceActor.getHeaderColumns(fileReader)

    val config = ConfigFactory.parseString(s"""header = true
                                           file = $csvPath
                                           """)
    client.setupDataset(dataset,
                        headerCols,
                        DatasetResourceSpec(1, 1),
                        IngestionSource(classOf[CsvSourceFactory].getName, config)).foreach {
      case e: ErrorResponse =>
        println(s"Errors setting up ingestion: $e")
        exitCode = 2
        return
    }

    // TODO: now we just have to wait.

    client.flushCompletely(dataset, version, timeout)

    println(s"Ingestion of $csvPath finished!")
    exitCode = 0
  }
}

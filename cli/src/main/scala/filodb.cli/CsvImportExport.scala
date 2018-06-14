package filodb.cli

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import filodb.coordinator.client.LocalClient
import filodb.coordinator.sources.CsvStreamFactory
import filodb.coordinator.NodeClusterActor
import filodb.core._
import filodb.core.store.{MetaStore, StoreConfig}

// Turn off style rules for CLI classes
// scalastyle:off
trait CsvImportExport extends StrictLogging {
  def system: ActorSystem
  def metaStore: MetaStore
  def coordinatorActor: ActorRef
  def client: LocalClient
  var exitCode = 0

  implicit val ec: ExecutionContext
  import NodeClusterActor._

  def ingestCSV(dataset: DatasetRef,
                csvPath: String,
                delimiter: Char,
                timeout: FiniteDuration): Unit = {
    val config = ConfigFactory.parseString(s"""header = true
                                           file = $csvPath
                                           """)

    // Just set up some defaults.  Not terribly important since CSV import is going away sometime.
    val storeConf = ConfigFactory.parseString(s"""
                         |  store {
                         |    flush-interval = 30m
                         |    shard-mem-size = 256MB
                         |  }""".stripMargin)

    client.setupDataset(dataset,
                        DatasetResourceSpec(1, 1),
                        IngestionSource(classOf[CsvStreamFactory].getName, config),
                        StoreConfig(storeConf)).foreach {
      case e: ErrorResponse =>
        println(s"Errors setting up ingestion: $e")
        exitCode = 2
        return
    }

    // TODO: now we just have to wait.

    client.flushCompletely(dataset, timeout)

    println(s"Ingestion of $csvPath finished!")
    exitCode = 0
  }
}

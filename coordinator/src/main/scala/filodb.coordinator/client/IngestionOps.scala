package filodb.coordinator.client

import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._

import filodb.core._
import filodb.coordinator._

trait IngestionOps extends ClientBase with StrictLogging {
  import IngestionCommands._
  import DatasetCoordinatorActor.Stats

  /**
   * Sends a message to all coordinators or FiloDB nodes to set up ingestion flow.
   * @param dataset the DatasetRef of the dataset to start ingesting
   * @param schema the column names in the exact order they will be presented in RowReaders sent to nodes
   * @param version the version of the dataset to start ingesting
   * @return a sequence of error responses. Nil means success
   */
  def setupIngestion(dataset: DatasetRef,
                     schema: Seq[String],
                     version: Int,
                     timeout: FiniteDuration = 30.seconds): Seq[ErrorResponse] = {
    askAllCoordinators(SetupIngestion(dataset, schema, version), timeout) { case x: NodeResponse => x
    }.collect { case e: ErrorResponse => e }
  }

  /**
   * Flushes the active memtable of the given dataset and version, no matter how much is in the memtable.
   * This should really only be done at the end of batch ingestions, to ensure that all data is finished
   * ingesting.  Calling this prematurely results in smaller chunks and less efficient storage.
   * All NodeCoordinators will be asked to flush, and the count of nodes flushing will be returned.
   * @param dataset the DatasetRef of the dataset to flush
   * @param version the version of the dataset to flush
   * @throws ClientException
   */
  def flush(dataset: DatasetRef, version: Int, timeout: FiniteDuration = 30.seconds): Int = {
    askAllCoordinators(Flush(dataset, version), timeout) {
      case Flushed => true
      case FlushIgnored => false
      case UnknownDataset => false
    }.filter(x => x).length
  }

  def flushByName(datasetName: String,
                  database: Option[String] = None,
                  version: Int = 0): Int = flush(DatasetRef(datasetName, database), version)

  /**
   * Retrieves the current ingestion stats from all nodes for a particular dataset and version.
   */
  def ingestionStats(dataset: DatasetRef,
                     version: Int,
                     timeout: FiniteDuration = 30.seconds): Seq[Stats] =
    askAllCoordinators(GetIngestionStats(dataset, version), timeout) {
      case stats: Stats => Some(stats)
      case UnknownDataset => None
    }.flatten

  /**
   * Repeatedly checks the ingestion stats and flushes until everything is flushed for a given dataset.
   */
  def flushCompletely(dataset: DatasetRef, version: Int, timeout: FiniteDuration = 30.seconds): Int = {
    var nodesFlushed: Int = 0

    def anyRowsLeft: Boolean = {
      val stats = ingestionStats(dataset, version, timeout)
      logger.debug(s"Stats from all ingestors: $stats")
      stats.collect { case s: Stats if s.numRowsActive >= 0 => s.numRowsActive }.sum > 0
    }

    // TODO(velvia): Think of a more reactive way to do this, maybe have each actor handle FlushCompletely.
    while(anyRowsLeft) {
      nodesFlushed = flush(dataset, version, timeout)
      Thread sleep 1000
    }

    nodesFlushed
  }
}
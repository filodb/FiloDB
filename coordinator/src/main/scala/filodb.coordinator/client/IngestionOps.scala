package filodb.coordinator.client

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator._
import filodb.core._

trait IngestionOps extends ClientBase with StrictLogging {
  import IngestionActor.IngestionStatus
  import IngestionCommands._

  /**
   * Flushes the active memtable of the given dataset and version, no matter how much is in the memtable.
   * This should really only be done at the end of batch ingestions, to ensure that all data is finished
   * ingesting.  Calling this prematurely results in smaller chunks and less efficient storage.
   * All NodeCoordinators will be asked to flush, and the count of nodes flushing will be returned.
   * @param dataset the DatasetRef of the dataset to flush
   * @throws ClientException
   */
  def flush(dataset: DatasetRef, timeout: FiniteDuration = 30.seconds): Int = {
    askAllCoordinators(Flush(dataset), timeout) {
      case Flushed => true
      case FlushIgnored => false
      case UnknownDataset => false
    }.filter(x => x).length
  }

  def flushByName(datasetName: String, database: Option[String] = None): Int =
    flush(DatasetRef(datasetName, database))

  /**
   * Retrieves the current ingestion stats from all nodes for a particular dataset and version.
   */
  def ingestionStats(dataset: DatasetRef,
                     timeout: FiniteDuration = 30.seconds): Seq[IngestionStatus] =
    askAllCoordinators(GetIngestionStats(dataset), timeout) {
      case stats: IngestionStatus => Some(stats)
      case UnknownDataset => None
    }.flatten

  /**
   * Repeatedly checks the ingestion stats and flushes until everything is flushed for a given dataset.
   */
  def flushCompletely(dataset: DatasetRef, timeout: FiniteDuration = 30.seconds): Int = {
    var nodesFlushed: Int = 0

    // TODO: need to reimplement this based on MemStore flush status
    // def anyRowsLeft: Boolean = {
    //   val stats = ingestionStats(dataset, version, timeout)
    //   logger.debug(s"Stats from all ingestors: $stats")
    //   stats.collect { case s: Stats if s.numRowsActive >= 0 => s.numRowsActive }.sum > 0
    // }

    // while(anyRowsLeft) {
    //   nodesFlushed = flush(dataset, version, timeout)
    //   Thread sleep 1000
    // }

    // nodesFlushed
    0
  }
}
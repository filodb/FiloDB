package filodb.coordinator.client

import scala.concurrent.duration._

import filodb.core._
import filodb.coordinator._

trait IngestionOps extends ClientBase {
  import IngestionCommands._

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
                     timeout: FiniteDuration = 30.seconds): Seq[DatasetCoordinatorActor.Stats] =
    askAllCoordinators(GetIngestionStats(dataset, version), timeout) {
      case stats: DatasetCoordinatorActor.Stats => Some(stats)
      case UnknownDataset => None
    }.flatten
}
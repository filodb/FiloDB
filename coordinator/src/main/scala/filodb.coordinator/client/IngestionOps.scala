package filodb.coordinator.client

import scala.concurrent.duration._

import filodb.core._
import filodb.coordinator._

trait IngestionOps extends ClientBase {
  import NodeCoordinatorActor._

  /**
   * Flushes the active memtable of the given dataset and version, no matter how much is in the memtable.
   * This should really only be done at the end of batch ingestions, to ensure that all data is finished
   * ingesting.  Calling this prematurely results in smaller chunks and less efficient storage.
   * @param dataset the DatasetRef of the dataset to flush
   * @param version the version of the dataset to flush
   * @throws ClientException
   */
  def flush(dataset: DatasetRef, version: Int, timeout: FiniteDuration = 30.seconds): Unit = {
    askCoordinator(Flush(dataset, version), timeout) {
      case Flushed =>
    }
  }

  def flushByName(datasetName: String,
                  database: Option[String] = None,
                  version: Int = 0): Unit = flush(DatasetRef(datasetName, database), version)

  /**
   * Retrieves the current ingestion stats for a particular dataset and version.
   */
  def ingestionStats(dataset: DatasetRef,
                     version: Int,
                     timeout: FiniteDuration = 30.seconds): DatasetCoordinatorActor.Stats =
    askCoordinator(GetIngestionStats(dataset, version), timeout) {
      case stats: DatasetCoordinatorActor.Stats => stats
    }
}
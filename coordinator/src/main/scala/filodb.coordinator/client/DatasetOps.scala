package filodb.coordinator.client

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator._
import filodb.core._

trait DatasetOps extends ClientBase with StrictLogging {
  import DatasetCommands._

  /**
   * Truncates the data for the given dataset, including all the data in the memstore and on disk.
   * @param dataset the dataset to truncate
   */
  def truncateDataset(dataset: DatasetRef,
                      timeout: FiniteDuration = 30.seconds): Unit = {
    logger.info(s"Truncating dataset $dataset...")
    askCoordinator(TruncateDataset(dataset), timeout) {
      case DatasetTruncated =>
    }
    sendAllIngestors(NodeCoordinatorActor.ClearState(dataset))
  }
}
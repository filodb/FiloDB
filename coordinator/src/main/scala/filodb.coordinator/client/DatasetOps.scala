package filodb.coordinator.client

import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.Dataset
import filodb.coordinator._

trait DatasetOps extends ClientBase with StrictLogging {
  import DatasetCommands._

  /**
   * Creates a new dataset, defining both columns and the dataset itself, and persisting it
   * in the MetaStore.
   * TODO: you cannot now transmit a Dataset over the network.  Refactor this to contain the components
   * of a dataset definition.
   */
  def createNewDataset(dataset: Dataset,
                       database: Option[String] = None,
                       timeout: FiniteDuration = 30.seconds): Unit = {
    logger.info(s"Creating dataset ${dataset.name}...")
    askCoordinator(CreateDataset(dataset, database), timeout) {
      case DatasetCreated =>
        logger.info(s"Dataset ${dataset.name} created successfully...")
      case DatasetAlreadyExists =>
        throw new RuntimeException(s"Dataset $dataset already exists!")
      case DatasetError(errMsg) =>
        throw new RuntimeException(s"Error creating dataset: $errMsg")
    }
  }

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
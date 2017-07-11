package filodb.coordinator.client

import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.Projection
import filodb.coordinator._

trait DatasetOps extends ClientBase with StrictLogging {
  import DatasetCommands._

  /**
   * Deletes both the metadata (dataset, columns) as well as drops the column store tables for a dataset.
   * Also resets cached state on all the coordinators.
   */
  def deleteDataset(dataset: DatasetRef, timeout: FiniteDuration = 30.seconds): Unit = {
    logger.info(s"Deleting dataset $dataset...")
    askCoordinator(DropDataset(dataset), timeout) {
      case DatasetDropped =>
    }
    // TODO: clear all versions
    sendAllIngestors(NodeCoordinatorActor.ClearState(dataset, 0))
  }

  /**
   * Truncates the data for the given dataset.  For now always works on projection 0.
   * Also resets cached state on each executor so it doesn't get out of sync.
   * @param dataset the dataset to truncate
   * @param version the version to truncate
   */
  def truncateDataset(dataset: DatasetRef,
                      version: Int,
                      timeout: FiniteDuration = 30.seconds): Unit = {
    logger.info(s"Truncating dataset $dataset...")
    val projection = Projection(0, dataset, Nil, "")
    askCoordinator(TruncateProjection(projection, version), timeout) {
      case ProjectionTruncated =>
    }
    sendAllIngestors(NodeCoordinatorActor.ClearState(dataset, version))
  }
}
package filodb.coordinator.client

import scala.concurrent.duration._

import filodb.core._
import filodb.coordinator._

trait DatasetOps extends ClientBase {
  import NodeCoordinatorActor._

  /**
   * Deletes both the metadata (dataset, columns) as well as drops the column store tables for a dataset.
   */
  def deleteDataset(dataset: DatasetRef, timeout: FiniteDuration = 30.seconds): Unit = {
    askCoordinator(DropDataset(dataset), timeout) {
      case DatasetDropped =>
    }
  }
}
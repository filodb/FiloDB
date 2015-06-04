package filodb.core.datastore

import scala.concurrent.Future

import filodb.core.messages._
import filodb.core.metadata.Dataset

/**
 * Low-level datastore API dealing with persistence of Datasets
 */
trait DatasetApi {
  import Datastore._

  /**
   * Creates a new dataset with the given name, if it doesn't already exist.
   * @param name Name of the dataset to create
   * @returns Success, or AlreadyExists, or StorageEngineException
   */
  def createNewDataset(name: String): Future[Response]

  /**
   * Retrieves a Dataset object of the given name.
   * @param name Name of the dataset to retrieve
   * @param TheDataset
   */
  def getDataset(name: String): Future[Response]

  /**
   * Adds a partition to a dataset.
   * @param name Name of the dataset
   * @param partition Name of the partition to add to the dataset
   */
  def addDatasetPartition(name: String, partition: String): Future[Response]

  /**
   * Attempts to delete a dataset with the given name.  Will fail if the dataset still has
   * partitions inside. You need to delete the partitions first.
   * @param name Name of the dataset to delete.
   * @returns Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(name: String): Future[Response]
}

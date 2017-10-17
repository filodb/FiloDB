package filodb.core.store

import scala.concurrent.Future

import filodb.core._
import filodb.core.metadata.Dataset

abstract class MetaStoreError(msg: String) extends Exception(msg)

/**
 * The MetaStore defines an API to read and write dataset metadata.
 * It is not responsible for sharding, partitioning, etc. which is the domain of the ColumnStore.
 * Like the ColumnStore, datasets are partitioned into "databases", like keyspaces in Cassandra.
 */
trait MetaStore {
  /**
   * Initializes the MetaStore so it is ready for further commands.
   */
  def initialize(): Future[Response]

  /**
   * Clears all dataset and column metadata from the MetaStore.
   */
  def clearAllData(): Future[Response]

  /**
   * ** Dataset API ***
   */

  /**
   * Writes new dataset metadata to the MetaStore.  If the dataset ref already existed, then nothing
   * should be modified and AlreadyExists returned.  This is to prevent dataset definition from changing
   * after it has been created.
   * @param dataset the Dataset to create.
   * @return Success, or AlreadyExists, or StorageEngineException
   */
  def newDataset(dataset: Dataset): Future[Response]

  /**
   * Retrieves a Dataset object of the given name, with all the existing column definitions
   * @param ref the DatasetRef defining the dataset to retrieve details for
   * @return a Dataset
   */
  def getDataset(ref: DatasetRef): Future[Dataset]

  def getDataset(dataset: String): Future[Dataset] = getDataset(DatasetRef(dataset))

  /**
   * Retrieves the names of all datasets registered in the metastore
   * @param database the name of the database/keyspace to retrieve datasets for.  If None, return all
   *                 datasets across all databases.
   */
  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]]

  /**
   * Deletes all dataset metadata.  Does not delete column store data.
   * @param ref the DatasetRef defining the dataset to delete
   * @return Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(ref: DatasetRef): Future[Response]

  /**
   * Shuts down the MetaStore, including any threads that might be hanging around
   */
  def shutdown(): Unit
}
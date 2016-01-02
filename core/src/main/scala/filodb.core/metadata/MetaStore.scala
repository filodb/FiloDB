package filodb.core.metadata

import scala.concurrent.{ExecutionContext, Future}

import filodb.core._

object MetaStore {
  case class IllegalColumnChange(reasons: Seq[String]) extends Exception {
    override def getMessage: String = reasons.mkString(", ")
  }
}

/**
 * The MetaStore defines an API to read and write dataset/column/projection metadata.
 * It is not responsible for sharding, partitioning, etc. which is the domain of the ColumnStore.
 */
trait MetaStore {
  import MetaStore._

  implicit val ec: ExecutionContext

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
   * Creates a new dataset with the given name, if it doesn't already exist.
   * @param dataset the Dataset to create.  Should have superprojection defined.
   * @return Success, or AlreadyExists, or StorageEngineException
   */
  def newDataset(dataset: Dataset): Future[Response]

  /**
   * Retrieves a Dataset object of the given name
   * @param name Name of the dataset to retrieve
   * @return a Dataset
   */
  def getDataset(name: String): Future[Dataset]

  /**
   * Deletes dataset metadata including all projections.  Does not delete column store data.
   * @param name Name of the dataset to delete.
   * @return Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(name: String): Future[Response]

  // TODO: add/delete projections

  /**
   * ** Column API ***
   */

  /**
   * Creates a new column for a particular dataset and effective version.
   * Can also be used to change the column type by "creating" the same column with changes for a higher
   * version.  Note that changes for a column must have an effective version higher than the last change.
   * See the notes in metadata/Column.scala regarding columns and versioning.
   * @param column the new Column to create.
   * @return Success if succeeds, or AlreadyExists, or IllegalColumnChange
   */
  def newColumn(column: Column): Future[Response] = {
    getSchema(column.dataset, Int.MaxValue).flatMap { schema =>
      val invalidReasons = Column.invalidateNewColumn(column.dataset, schema, column)
      if (invalidReasons.nonEmpty) { Future.failed(IllegalColumnChange(invalidReasons)) }
      else                         { insertColumn(column) }
    }
  }

  /**
   * Inserts or updates a column definition for a particular dataset and effective version.
   * That column definition must not exist already.
   * Does no validation against the column schema -- please use the higher level Datastore.newColumn.
   * @param column the new Column to insert
   * @return Success if succeeds, or AlreadyExists
   */
  def insertColumn(column: Column): Future[Response]

  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * Implementations should use Column.foldSchema.
   * @param dataset the name of the dataset to return the schema for
   * @param version the version of the dataset to return the schema for
   * @return a Schema, column name -> Column definition, or ErrorResponse
   */
  def getSchema(dataset: String, version: Int): Future[Column.Schema]

  /**
   * Shuts down the MetaStore, including any threads that might be hanging around
   */
  def shutdown(): Unit
}
package filodb.core.store

import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}

object MetaStore {
  case class IllegalColumnChange(reasons: Seq[String]) extends Exception {
    override def getMessage: String = reasons.mkString(", ")
  }
}

/**
 * The MetaStore defines an API to read and write dataset/column/projection metadata.
 * It is not responsible for sharding, partitioning, etc. which is the domain of the ColumnStore.
 * Like the ColumnStore, datasets are partitioned into "databases", like keyspaces in Cassandra.
 */
trait MetaStore {
  import MetaStore._

  implicit val ec: ExecutionContext

  /**
   * Initializes the MetaStore so it is ready for further commands.
   * @param database the name of the database/keyspace to initialize the MetaStore for.  May be ignored by
   *        some MetaStores.
   */
  def initialize(database: String): Future[Response]

  /**
   * Clears all dataset and column metadata from the MetaStore.
   * @param database the name of the database/keyspace to initialize the MetaStore for.  May be ignored by
   *        some MetaStores.
   */
  def clearAllData(database: String): Future[Response]

  /**
   * ** Dataset API ***
   */

  /**
   * Creates a new dataset with the given name, if it doesn't already exist.
   * @param dataset the Dataset to create.  Should have superprojection defined.
   * @param database optionally, the database in which to create the dataset
   * @return Success, or AlreadyExists, or StorageEngineException
   */
  def newDataset(dataset: Dataset, database: Option[String] = None): Future[Response]

  /**
   * Retrieves a Dataset object of the given name
   * @param ref the DatasetRef defining the dataset to retrieve details for
   * @return a Dataset
   */
  def getDataset(ref: DatasetRef): Future[Dataset]

  /**
   * Retrieves the names of all datasets registered in the metastore
   * @param database the name of the database/keyspace to initialize the MetaStore for.  May be ignored by
   *        some MetaStores.
   */
  def getAllDatasets(database: String): Future[Seq[String]]

  /**
   * Deletes dataset metadata including all projections and columns.  Does not delete column store data.
   * @param ref the DatasetRef defining the dataset to delete
   * @return Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(ref: DatasetRef): Future[Response]

  // TODO: add/delete projections

  /**
   * ** Column API ***
   */

  /**
   * Creates a new data column for a particular dataset and effective version.
   * Can also be used to change the column type by "creating" the same column with changes for a higher
   * version.  Note that changes for a column must have an effective version higher than the last change.
   * See the notes in metadata/Column.scala regarding columns and versioning.
   * @param column the new DataColumn to create.
   * @param dataset the DatasetRef for the dataset for which the column should be created
   * @return Success if succeeds, or AlreadyExists, or IllegalColumnChange
   */
  def newColumn(column: DataColumn, dataset: DatasetRef): Future[Response] = {
    getSchema(dataset, Int.MaxValue).flatMap { schema =>
      val invalidReasons = Column.invalidateNewColumn(schema, column)
      if (invalidReasons.nonEmpty) { Future.failed(IllegalColumnChange(invalidReasons)) }
      else                         { insertColumn(column, dataset) }
    }
  }

  /**
   * Inserts or updates a column definition for a particular dataset and effective version.
   * That column definition must not exist already.
   * Does no validation against the column schema -- please use the higher level Datastore.newColumn.
   * @param column the new Column to insert
   * @param dataset the DatasetRef for the dataset for which the column should be created
   * @return Success if succeeds, or AlreadyExists
   */
  def insertColumn(column: DataColumn, dataset: DatasetRef): Future[Response]

  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * Implementations should use Column.foldSchema.
   * @param ref the DatasetRef defining the dataset to retrieve the schema for
   * @param version the version of the dataset to return the schema for
   * @return a Schema, column name -> Column definition, or ErrorResponse
   */
  def getSchema(dataset: DatasetRef, version: Int): Future[Column.Schema]

  /**
   * Shuts down the MetaStore, including any threads that might be hanging around
   */
  def shutdown(): Unit
}
package filodb.core.datastore

import scala.concurrent.Future

import filodb.core.messages._
import filodb.core.metadata.Column

/**
 * Low-level datastore API dealing with persistence of Columns
 */
trait ColumnApi {
  /**
   * Inserts a column definition for a particular dataset and effective version.
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
  def getSchema(dataset: String, version: Int): Future[Response]
}

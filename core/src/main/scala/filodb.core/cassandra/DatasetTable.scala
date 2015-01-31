package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import scala.concurrent.Future

import filodb.core.metadata.Dataset

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its partitions
 */
sealed class DatasetTable extends CassandraTable[DatasetTable, Dataset] {
  object name extends StringColumn(this) with PartitionKey[String]
  object partitions extends SetColumn[DatasetTable, Dataset, String](this)

  override def fromRow(row: Row): Dataset =
    Dataset(name(row), partitions(row))
}

/**
 * Asynchronous methods to operate on datasets.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object DatasetTableOps extends DatasetTable with SimpleCassandraConnector {
  override val tableName = "datasets"

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  val keySpace = "test"

  import Util._
  import filodb.core.messages._

  // This is really an exception - deleting a non-empty partition should be a bug
  class NonEmptyDataset(partitions: Set[String]) extends Exception

  /**
   * Creates a new dataset with the given name, if it doesn't already exist.
   * @param name Name of the dataset to create
   * @returns Success, or AlreadyExists, or StorageEngineException
   */
  def createNewDataset(name: String): Future[Response] =
    insert.value(_.name, name).ifNotExists.future().toResponse(AlreadyExists)

  def getDataset(name: String): Future[Response] =
    select.where(_.name eqs name).one()
      .map(opt => opt.map(Dataset.Result(_)).getOrElse(NotFound))
      .handleErrors

  /**
   * Attempts to delete a dataset with the given name.  Will fail if the dataset still has
   * partitions inside. You need to delete the partitions first.
   * @param name Name of the dataset to delete.
   * @returns Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(name: String): Future[Response] = {
    def checkEmptyPartitionsThenDelete(optPartitions: Option[Set[String]]): Future[Response] = {
      val emptySet = Set.empty[String]
      optPartitions match {
        case None => Future(NotFound)
        case Some(`emptySet`) =>
          // NOTE: There is a potential race condition if someone added a partition
          // while someone else deletes the dataset.  One possible protection is to
          // use locking on dataset metadata.  We can also change the delete timestamp,
          // though this doesn't actually prevent someone else from adding the partition.
          // Two levels of locks seems overkill though.  Perhaps better idea is that deleting
          // a dataset requires locking all partitions, and only releasing all the locks when
          // the deletion is complete.
          delete.where(_.name eqs name).future().toResponse()
        case Some(partitions) =>
          Future(MetadataException(new NonEmptyDataset(partitions)))
      }
    }

    val partitions = select(_.partitions).where(_.name eqs name).one()
    partitions.flatMap { optParts => checkEmptyPartitionsThenDelete(optParts) }
              .handleErrors
  }

  // Partial function mapping commands to functions executing them
  val commandMapper: PartialFunction[Command, Future[Response]] = {
    case Dataset.NewDataset(dataset) => createNewDataset(dataset)
    case Dataset.DeleteDataset(name) => deleteDataset(name)
    case Dataset.GetDataset(name)    => getDataset(name)
  }
}

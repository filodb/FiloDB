package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.dsl._
import scala.concurrent.Future

import filodb.core.datastore.{Datastore, DatasetApi}
import filodb.core.metadata.Dataset

/**
 * Represents the "dataset" Cassandra table tracking each dataset and its partitions
 */
sealed class DatasetTable extends CassandraTable[DatasetTable, Dataset] {
  // scalastyle:off
  object name extends StringColumn(this) with PartitionKey[String]
  object partitions extends SetColumn[DatasetTable, Dataset, String](this)
  // scalastyle:on

  override def fromRow(row: Row): Dataset =
    Dataset(name(row), partitions(row))
}

/**
 * Asynchronous methods to operate on datasets.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object DatasetTableOps extends DatasetTable with DatasetApi with SimpleCassandraConnector {
  import Datastore._

  override val tableName = "datasets"

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  implicit val keySpace = KeySpace("unittest")

  import Util._
  import filodb.core.messages._

  def createNewDataset(name: String): Future[Response] =
    insert.value(_.name, name).ifNotExists.future().toResponse(AlreadyExists)

  def getDataset(name: String): Future[Response] =
    select.where(_.name eqs name).one()
      .map(opt => opt.map(TheDataset(_)).getOrElse(NotFound))
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
        case None => Future.successful(NotFound)
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
          logger.warn(s"Someone tried to delete a non-empty dataset $name with $partitions !!")
          Future.successful(MetadataException(new NonEmptyDataset(partitions)))
      }
    }

    val partitions = select(_.partitions).where(_.name eqs name).one()
    partitions.flatMap { optParts => checkEmptyPartitionsThenDelete(optParts) }
              .handleErrors
  }

  def addDatasetPartition(name: String, partition: String): Future[Response] = {
    update.where(_.name eqs name)
          .modify(_.partitions add partition)
          .future().toResponse()
  }
}

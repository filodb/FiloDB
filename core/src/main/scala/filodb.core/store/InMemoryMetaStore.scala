package filodb.core.store

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

import com.typesafe.scalalogging.StrictLogging

import filodb.core._
import filodb.core.metadata.Dataset

/**
 * An in-memory MetaStore.  Does not aim to keep data distributed, but is just a
 * single-node implementation, which works for example in the Spark driver since
 * the MetaStore is only called by the driver code and not by workers.
 *
 * NOTE: the database name is ignored by the InMemoryMetaStore.
 */
class InMemoryMetaStore(implicit val ec: ExecutionContext) extends MetaStore with StrictLogging {
  logger.info("Starting InMemoryMetaStore...")

  val datasets = new TrieMap[String, Dataset]

  def initialize(): Future[Response] = Future.successful(Success)

  def clearAllData(): Future[Response] = Future {
    logger.warn("Clearing all data!")
    datasets.clear()
    Success
  }

  /**
   * ** Dataset API ***
   */
  def newDataset(dataset: Dataset): Future[Response] =
    if (datasets contains dataset.name) {
      Future.successful(AlreadyExists)
    } else {
      datasets.put(dataset.name, dataset)
      Future.successful(Success)
    }

  def getDataset(ref: DatasetRef): Future[Dataset] =
    datasets.get(ref.dataset).map(Future.successful)
            .getOrElse(Future.failed(NotFoundError(ref.dataset)))

  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]] = {
    val filterFunc: DatasetRef => Boolean =
      database.map { db => (ref: DatasetRef) => ref.database.get == db }.getOrElse { ref => true }
    Future.successful(datasets.values.map(_.ref).filter(filterFunc).toSeq)
  }

  def deleteDataset(ref: DatasetRef): Future[Response] = {
    datasets.remove(ref.dataset)
    Future.successful(Success)
  }

  def shutdown(): Unit = {}
}
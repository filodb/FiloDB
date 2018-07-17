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
  val checkpoints = new TrieMap[(String, Int), Map[Int, Long]].withDefaultValue(Map.empty)
  val sources = new TrieMap[DatasetRef, IngestionConfig]
  val timebuckets = new TrieMap[(String, Int), Int]

  def initialize(): Future[Response] = Future.successful(Success)

  def clearAllData(): Future[Response] = Future {
    logger.warn("Clearing all data!")
    datasets.clear()
    checkpoints.clear()
    sources.clear()
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

  def deleteDataset(ref: DatasetRef): Future[Response] = Future {
    datasets.remove(ref.dataset).map(x => Success).getOrElse(NotFound)
  }

  def shutdown(): Unit = {}

  override def writeCheckpoint(dataset: DatasetRef, shardNum: Int, groupNum: Int, offset: Long): Future[Response] = {
    val groupMap = checkpoints((dataset.dataset, shardNum))
    checkpoints((dataset.dataset, shardNum)) = groupMap + (groupNum -> offset)
    Future.successful(Success)
  }

  override def readEarliestCheckpoint(dataset: DatasetRef, shardNum: Int): Future[Long] =
    readCheckpoints(dataset, shardNum).map {
      case checkpoints if checkpoints.nonEmpty => checkpoints.values.min
      case checkpoints                         => Long.MinValue
    }

  override def readCheckpoints(dataset: DatasetRef, shardNum: Int): Future[Map[Int, Long]] = Future {
    checkpoints((dataset.dataset, shardNum))
  }

  def writeIngestionConfig(state: IngestionConfig): Future[Response] = {
    sources.put(state.ref, state)
    Future.successful(Success)
  }

  def readIngestionConfigs(): Future[Seq[IngestionConfig]] =
    Future.successful(sources.values.toSeq)

  def deleteIngestionConfig(ref: DatasetRef): Future[Response] = Future {
    sources.remove(ref).map(x => Success).getOrElse(NotFound)
  }

  /**
    * Record highest time bucket for part key indexable data in meta store
    */
  override def writeHighestIndexTimeBucket(dataset: DatasetRef, shardNum: Int,
                                           highestTimeBucket: Int): Future[Response] = {
    timebuckets((dataset.dataset, shardNum)) = highestTimeBucket
    Future.successful(Success)

  }

  /**
    * Read highest time bucket for part key indexable data in meta store
    */
  override def readHighestIndexTimeBucket(dataset: DatasetRef,
                                          shardNum: Int): Future[Option[Int]] = {
    Future.successful(timebuckets.get((dataset.dataset, shardNum)))
  }
}
package filodb.core.store

import com.typesafe.scalalogging.StrictLogging
import java.util.concurrent.ConcurrentSkipListMap

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset, IngestionStateData}

/**
 * An in-memory MetaStore.  Does not aim to keep data distributed, but is just a
 * single-node implementation, which works for example in the Spark driver since
 * the MetaStore is only called by the driver code and not by workers.
 *
 * NOTE: the database name is ignored by the InMemoryMetaStore.
 */
class InMemoryMetaStore(implicit val ec: ExecutionContext) extends MetaStore with StrictLogging {
  import collection.JavaConverters._

  logger.info("Starting InMemoryMetaStore...")

  val datasets = new TrieMap[String, Dataset]
  type ColumnMap = ConcurrentSkipListMap[(Int, Types.ColumnId), DataColumn]
  val colMapOrdering = math.Ordering[(Int, Types.ColumnId)]
  val columns = new TrieMap[String, ColumnMap]
  val ingestionstates = new TrieMap[String, IngestionStateData]

  def initialize(): Future[Response] = Future.successful(Success)

  def clearAllData(): Future[Response] = Future {
    logger.warn("Clearing all data!")
    datasets.clear()
    columns.clear()
    ingestionstates.clear()
    Success
  }

  /**
   * ** Dataset API ***
   */

  def newDataset(dataset: Dataset): Future[Response] = {
    if (dataset.projections.isEmpty) {
      Future.failed(MetadataException(new IllegalArgumentException(s"Dataset $dataset has no projections")))
    } else {
      datasets.putIfAbsent(dataset.name, dataset) match {
        case None    => Future.successful(Success)
        case Some(x) =>
          logger.info(s"Ignoring newDataset($dataset); entry already exists")
          Future.successful(AlreadyExists)
      }
    }
  }

  def getDataset(ref: DatasetRef): Future[Dataset] =
    datasets.get(ref.dataset).map(Future.successful)
            .getOrElse(Future.failed(NotFoundError(ref.dataset)))

  def getAllDatasets(database: Option[String]): Future[Seq[DatasetRef]] = {
    val filterFunc: DatasetRef => Boolean =
      database.map { db => (ref: DatasetRef) => ref.database.get == db }.getOrElse { ref => true }
    Future.successful(datasets.values.map(_.projections.head.dataset).filter(filterFunc).toSeq)
  }

  def deleteDataset(ref: DatasetRef): Future[Response] = Future {
    datasets.remove(ref.dataset)
    columns.remove(ref.dataset)
    Success
  }

  /**
   * ** Column API ***
   */

  def insertColumns(newColumns: Seq[DataColumn], ref: DatasetRef): Future[Response] = {
    // See https://issues.scala-lang.org/browse/SI-7943
    val columnMap = columns.get(ref.dataset) match {
      case Some(cMap) => cMap
      case None =>
        val newCMap = new ColumnMap(colMapOrdering)
        columns.putIfAbsent(ref.dataset, newCMap).getOrElse(newCMap)
    }
    newColumns.foreach { c => columnMap.putIfAbsent((c.version, c.name), c) }
    Future.successful(Success)
  }

  def getSchema(dataset: DatasetRef, version: Int): Future[Column.Schema] = Future {
    columns.get(dataset.dataset).map { columnMap =>
      columnMap.entrySet.asScala
               .takeWhile(_.getKey()._1 <= version)
               .map(_.getValue)
               .foldLeft(Column.EmptySchema)(Column.schemaFold)
    }.getOrElse(Column.EmptySchema)
  }

  /**
   * ** IngestionState API ***
   */
  def insertIngestionState(actorAddress: String,
                           dataset: DatasetRef,
                           columns: String,
                           state: String,
                           version: Int,
                           exceptions: String = ""): Future[Response] = {
    ingestionstates.putIfAbsent(actorAddress,
      IngestionStateData(actorAddress,
        dataset.database.getOrElse("None"),
        dataset.dataset,
        version,
        columns,
        state,
        exceptions
      )) match {
      case None =>
        Future.successful(Success)
      case Some(x) =>
        logger.info(s"Ignoring ingestion state for dataset($dataset); entry already exists")
        Future.successful(AlreadyExists)
    }
  }

  def getAllIngestionEntries(actorPath: String): Future[Seq[IngestionStateData]] = {
    Future.successful(ingestionstates.get(actorPath).toSeq)
  }

  def updateIngestionState(actorAddress: String, dataset: DatasetRef,
                           state: String, exceptions: String, version: Int ): Future[Response] = {
    val ingStCols = ingestionstates.get(actorAddress).toString
    ingestionstates.remove(actorAddress)
    insertIngestionState(actorAddress, dataset, ingStCols.split("\u0001")(4), state, version, exceptions)
  }

  def shutdown(): Unit = {}
}
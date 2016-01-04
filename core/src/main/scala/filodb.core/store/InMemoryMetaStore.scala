package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.util.TreeMap
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.metadata.{Column, Dataset}

/**
 * An in-memory MetaStore.  Does not aim to keep data distributed, but is just a
 * single-node implementation, which works for example in the Spark driver since
 * the MetaStore is only called by the driver code and not by workers.
 */
class InMemoryMetaStore(implicit val ec: ExecutionContext) extends MetaStore with StrictLogging {
  import collection.JavaConverters._

  logger.info("Starting InMemoryMetaStore...")

  val datasets = new TrieMap[String, Dataset]
  type ColumnMap = TreeMap[(Int, Types.ColumnId), Column]
  val colMapOrdering = math.Ordering[(Int, Types.ColumnId)]
  val columns = new TrieMap[String, ColumnMap]

  def initialize(): Future[Response] = Future.successful(Success)

  def clearAllData(): Future[Response] = Future {
    logger.warn("Clearing all data!")
    datasets.clear()
    columns.clear()
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

  def getDataset(name: String): Future[Dataset] =
    datasets.get(name).map(Future.successful)
            .getOrElse(Future.failed(NotFoundError(name)))

  def deleteDataset(name: String): Future[Response] = Future {
    datasets.remove(name)
    columns.remove(name)
    Success
  }

  /**
   * ** Column API ***
   */

  def insertColumn(column: Column): Future[Response] = {
    val columnMap = columns.getOrElseUpdate(column.dataset, new ColumnMap(colMapOrdering))
    if (columnMap.containsKey((column.version, column.name))) {
      Future.successful(NotApplied)
    } else {
      columnMap.put((column.version, column.name), column)
      Future.successful(Success)
    }
  }

  def getSchema(dataset: String, version: Int): Future[Column.Schema] = Future {
    columns.get(dataset).map { columnMap =>
      columnMap.entrySet.asScala
               .takeWhile(_.getKey()._1 <= version)
               .map(_.getValue)
               .foldLeft(Column.EmptySchema)(Column.schemaFold)
    }.getOrElse(Column.EmptySchema)
  }

  def shutdown(): Unit = {}
}
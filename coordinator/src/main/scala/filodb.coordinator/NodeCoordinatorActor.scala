package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.Future
import scala.concurrent.duration._

import filodb.core._
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset, MetaStore, Projection}
import filodb.core.columnstore.ColumnStore
import filodb.core.reprojector.{MemTable, Reprojector}

/**
 * The NodeCoordinatorActor is the common API entry point for all FiloDB ingestion and metadata operations.
 * It is a singleton - there should be exactly one such actor per node/JVM process.
 * It is responsible for:
 * - Overall external FiloDB API.
 * - Staying aware of status of other NodeCoordinators around the ring
 * - Metadata changes (dataset/column changes)
 * - Caching changes to dataset metadata?
 * - Supervising, spinning up, cleaning up DatasetCoordinatorActors
 * - Forwarding new changes (rows) to other NodeCoordinatorActors if they are not local
 * - Forwarding rows to DatasetCoordinatorActors
 *
 * It is called by local (eg HTTP) as well as remote (eg Spark ETL) processes.
 */
object NodeCoordinatorActor {
  /**
   * Creates a new dataset with columns and a default projection.
   */
  case class CreateDataset(dataset: Dataset, columns: Seq[Column])

  case object DatasetCreated extends Response
  case class DatasetError(msg: String) extends ErrorResponse

  /**
   * Sets up ingestion for a given dataset, version, and schema of columns.
   * The dataset and columns must have been previously defined.
   *
   * @defaultPartitionKey if Some(key), a null value in partitioning column will cause key to be used.
   *                      if None, then NullPartitionValue will be thrown when null value
   *                        is encountered in a partitioning column.
   * @returns BadSchema if the partition column is unsupported, sort column invalid, etc.
   */
  case class SetupIngestion(dataset: String,
                            schema: Seq[String],
                            version: Int,
                            defaultPartitionKey: Option[PartitionKey] = None)

  case object IngestionReady extends Response
  case object UnknownDataset extends ErrorResponse
  case class UndefinedColumns(undefined: Seq[String]) extends ErrorResponse
  case class BadSchema(message: String) extends ErrorResponse

  /**
   * Ingests a new set of rows for a given dataset and version.
   * The partitioning column and sort column are set up in the dataset.
   *
   * @param seqNo the sequence number to be returned for acknowledging the entire set of rows
   * @returns Ack(seqNo) returned when the set of rows has been committed to the MemTable.
   */
  case class IngestRows(dataset: String, version: Int, rows: Seq[RowReader], seqNo: Long)

  case class Ack(seqNo: Long) extends Response

  /**
   * Initiates a flush of the remaining MemTable rows of the given dataset and version.
   * Usually used when at the end of ingesting some large blob of data.
   * @returns Flushed when the flush cycle has finished successfully, commiting data to columnstore.
   */
  case class Flush(dataset: String, version: Int)
  case object Flushed

  /**
   * Truncates all data from a projection of a dataset.  Waits for any pending flushes from said
   * dataset to finish first, and also clears the columnStore cache for that dataset.
   */
  case class TruncateProjection(projection: Projection, version: Int)
  case object ProjectionTruncated

  // Internal messages
  case class AddDatasetCoord(dataset: TableName, version: Int, dsCoordRef: ActorRef)

  def invalidColumns(columns: Seq[String], schema: Column.Schema): Seq[String] =
    (columns.toSet -- schema.keys).toSeq

  def props(memTable: MemTable,
            metaStore: MetaStore,
            reprojector: Reprojector,
            columnStore: ColumnStore,
            config: Config): Props =
    Props(classOf[NodeCoordinatorActor], memTable, metaStore, reprojector, columnStore, config)
}

/**
 * ==Configuration==
 * {{{
 *   {
 *     scheduler-interval = 1 s
 *     scheduler-reporting = false
 *     scheduler-reporting-interval = 30s
 *   }
 * }}}
 */
class NodeCoordinatorActor(memTable: MemTable,
                           metaStore: MetaStore,
                           reprojector: Reprojector,
                           columnStore: ColumnStore,
                           config: Config) extends BaseActor {
  import NodeCoordinatorActor._
  import context.dispatcher

  val dsCoordinators = new collection.mutable.HashMap[(TableName, Int), ActorRef]

  // Make sure the function below is NOT called from a Future, it updates state!
  private def createDatasetCoord(datasetObj: Dataset, version: Int): ActorRef = {
    val props = DatasetCoordinatorActor.props(datasetObj, version, columnStore, reprojector,
                                              config, memTable)
    context.actorOf(props, s"ds-coord-${datasetObj.name}-$version")
  }

  private def withDsCoord(originator: ActorRef, dataset: String, version: Int)
                         (func: ActorRef => Unit): Unit = {
    dsCoordinators.get((dataset, version)).map(func).getOrElse(originator ! UnknownDataset)
  }

  private def verifySchema(originator: ActorRef, dataset: String, version: Int, columns: Seq[String]):
      Future[Option[Column.Schema]] = {
    metaStore.getSchema(dataset, version).map { schema =>
      val undefinedCols = invalidColumns(columns, schema)
      if (undefinedCols.nonEmpty) {
        logger.info(s"Undefined columns $undefinedCols for dataset $dataset with schema $schema")
        originator ! UndefinedColumns(undefinedCols.toSeq)
        None
      } else {
        Some(schema)
      }
    }
  }

  private def createDataset(originator: ActorRef, datasetObj: Dataset, columns: Seq[Column]) = {
    if (datasetObj.projections.isEmpty) {
      originator ! DatasetError(s"There must be at least one projection in dataset $datasetObj")
    } else {
      (for { resp1 <- metaStore.newDataset(datasetObj)
             resp2 <- Future.sequence(columns.map(metaStore.newColumn(_)))
             resp3 <- columnStore.initializeProjection(datasetObj.projections.head) }
      yield {
        originator ! DatasetCreated
      }).recover {
        case e: StorageEngineException => originator ! e
      }
    }
  }

  private def setupIngestion(originator: ActorRef,
                             dataset: String,
                             columns: Seq[String],
                             version: Int,
                             defaultPartKey: Option[PartitionKey]): Unit = {
    // Try to get the dataset coordinator.  Create one if it doesn't exist.
    val dsCoordFuture: Future[ActorRef] =
      dsCoordinators.get((dataset, version)).map(Future.successful)
                    .getOrElse({
                      for { datasetObj <- metaStore.getDataset(dataset) } yield {
                        val ref = createDatasetCoord(datasetObj, version)
                        // This happens in a future, use messages so state updates in actor loop
                        self ! AddDatasetCoord(dataset, version, ref)
                        ref
                      }
                    })
    (for { dsCoord <- dsCoordFuture
           schema <- verifySchema(originator, dataset, version, columns) if schema.isDefined }
    yield {
      val columnSeq = columns.map(schema.get(_))
      dsCoord ! DatasetCoordinatorActor.Setup(originator, columnSeq, defaultPartKey)
    }).recover {
      case NotFoundError(what) => originator ! UnknownDataset
      case t: Throwable        => originator ! MetadataException(t)
    }
  }

  def receive: Receive = {
    case CreateDataset(datasetObj, columns) =>
      createDataset(sender, datasetObj, columns)

    case SetupIngestion(dataset, columns, version, defaultPartKey) =>
      setupIngestion(sender, dataset, columns, version, defaultPartKey)

    case IngestRows(dataset, version, rows, seqNo) =>
      withDsCoord(sender, dataset, version) { _ ! DatasetCoordinatorActor.NewRows(sender, rows, seqNo) }

    case flushCmd @ Flush(dataset, version) =>
      withDsCoord(sender, dataset, version) { _ ! DatasetCoordinatorActor.StartFlush(Some(sender)) }

    case TruncateProjection(projection, version) =>
      withDsCoord(sender, projection.dataset, version) {
        _ ! DatasetCoordinatorActor.ClearProjection(sender, projection)
      }

    case AddDatasetCoord(dataset, version, dsCoordRef) =>
      dsCoordinators((dataset, version)) = dsCoordRef
  }
}
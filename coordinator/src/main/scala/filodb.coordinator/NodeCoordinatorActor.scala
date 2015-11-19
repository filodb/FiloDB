package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.Future
import scala.concurrent.duration._

import filodb.core._
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset, MetaStore, Projection, RichProjection}
import filodb.core.columnstore.ColumnStore
import filodb.core.reprojector.Reprojector

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
  // Public, external Actor/Akka API, so every incoming command should be a NodeCommand
  sealed trait NodeCommand
  sealed trait NodeResponse

  /**
   * Creates a new dataset with columns and a default projection.
   */
  case class CreateDataset(dataset: Dataset, columns: Seq[Column]) extends NodeCommand

  case object DatasetCreated extends Response with NodeResponse
  case class DatasetError(msg: String) extends ErrorResponse with NodeResponse

  /**
   * Sets up ingestion for a given dataset, version, and schema of columns.
   * The dataset and columns must have been previously defined.
   *
   * @param defaultPartitionKey
   *        if Some(key), a null value in partitioning column will cause key to be used.
   *        if None, then NullPartitionValue will be thrown when null value is encountered
   *        in a partitioning column.
   * @return BadSchema if the partition column is unsupported, sort column invalid, etc.
   */
  case class SetupIngestion(dataset: String,
                            schema: Seq[String],
                            version: Int,
                            defaultPartitionKey: Option[PartitionKey] = None) extends NodeCommand

  case object IngestionReady extends NodeResponse
  case object UnknownDataset extends ErrorResponse with NodeResponse
  case class UndefinedColumns(undefined: Seq[String]) extends ErrorResponse with NodeResponse
  case class BadSchema(message: String) extends ErrorResponse with NodeResponse

  /**
   * Ingests a new set of rows for a given dataset and version.
   * The partitioning column and sort column are set up in the dataset.
   *
   * @param seqNo the sequence number to be returned for acknowledging the entire set of rows
   * @return Ack(seqNo) returned when the set of rows has been committed to the MemTable.
   */
  case class IngestRows(dataset: String, version: Int, rows: Seq[RowReader], seqNo: Long) extends NodeCommand

  case class Ack(seqNo: Long) extends NodeResponse

  /**
   * Initiates a flush of the remaining MemTable rows of the given dataset and version.
   * Usually used when at the end of ingesting some large blob of data.
   * @return Flushed when the flush cycle has finished successfully, commiting data to columnstore.
   */
  case class Flush(dataset: String, version: Int) extends NodeCommand
  case object Flushed extends NodeResponse

  /**
   * Checks to see if the DatasetCoordActor is ready to take in more rows.  Usually sent when an actor
   * is in a wait state.
   */
  case class CheckCanIngest(dataset: String, version: Int) extends NodeCommand
  case class CanIngest(can: Boolean) extends NodeResponse

  /**
   * Gets the latest ingestion stats from the DatasetCoordinatorActor
   */
  case class GetIngestionStats(dataset: String, version: Int) extends NodeCommand

  /**
   * Truncates all data from a projection of a dataset.  Waits for any pending flushes from said
   * dataset to finish first, and also clears the columnStore cache for that dataset.
   */
  case class TruncateProjection(projection: Projection, version: Int) extends NodeCommand
  case object ProjectionTruncated extends NodeResponse

  // Internal messages
  case class AddDatasetCoord(dataset: TableName, version: Int, dsCoordRef: ActorRef) extends NodeCommand
  case class DatasetCreateNotify(dataset: TableName, version: Int, msg: Any) extends NodeCommand

  def invalidColumns(columns: Seq[String], schema: Column.Schema): Seq[String] =
    (columns.toSet -- schema.keys).toSeq

  def props(metaStore: MetaStore,
            reprojector: Reprojector,
            columnStore: ColumnStore,
            config: Config): Props =
    Props(classOf[NodeCoordinatorActor], metaStore, reprojector, columnStore, config)
}

/**
 * ==Configuration==
 * {{{
 * }}}
 */
class NodeCoordinatorActor(metaStore: MetaStore,
                           reprojector: Reprojector,
                           columnStore: ColumnStore,
                           config: Config) extends BaseActor {
  import NodeCoordinatorActor._
  import context.dispatcher

  val dsCoordinators = new collection.mutable.HashMap[(TableName, Int), ActorRef]
  val dsCoordNotify = new collection.mutable.HashMap[(TableName, Int), List[ActorRef]]

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

  private def createDataset(originator: ActorRef, datasetObj: Dataset, columns: Seq[Column]): Unit = {
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

  // If the coordinator is already set up, then everything is already fine.
  // Otherwise get the dataset object and create a new actor, re-initializing state.
  private def setupIngestion(originator: ActorRef,
                             dataset: String,
                             columns: Seq[String],
                             version: Int): Unit = {
    def notify(msg: Any): Unit = { self ! DatasetCreateNotify(dataset, version, msg) }

    def createDatasetCoordActor(datasetObj: Dataset, richProj: RichProjection[_]): Unit = {
      val typedProj = richProj.asInstanceOf[RichProjection[richProj.helper.Key]]
      val props = DatasetCoordinatorActor.props(typedProj, version, columnStore, reprojector, config)
      val ref = context.actorOf(props, s"ds-coord-${datasetObj.name}-$version")
      self ! AddDatasetCoord(dataset, version, ref)
      notify(IngestionReady)
    }

    def createProjectionAndActor(datasetObj: Dataset, schema: Option[Column.Schema]): Unit = {
      val columnSeq = columns.map(schema.get(_))
      // Create the RichProjection, and ferret out any errors
      val proj = RichProjection.make(datasetObj, columnSeq)
      proj.recover {
        case RichProjection.BadSchema(reason) => notify(BadSchema(reason))
        case Dataset.BadPartitionColumn(reason) => notify(BadSchema(reason))
      }
      for { richProj <- proj } createDatasetCoordActor(datasetObj, richProj)
    }

    if (dsCoordinators contains (dataset -> version)) {
      originator ! IngestionReady
    } else if (dsCoordNotify contains (dataset -> version)) {
      // There is already a setupIngestion / dsCoordActor creation in progress.  Add to list of callbacks
      // for the final result.
      dsCoordNotify((dataset -> version)) = originator :: dsCoordNotify((dataset -> version))
    } else {
      dsCoordNotify((dataset -> version)) = List(originator)
      // Everything after this point happens in a future, asynchronously from the actor processing.
      // Thus 1) don't modify internal state, and 2) make sure we don't have multiple actor creations
      // happening in parallel, thus the need for dsCoordNotify.
      (for { datasetObj <- metaStore.getDataset(dataset)
             schema <- verifySchema(originator, dataset, version, columns) if schema.isDefined }
      yield {
        createProjectionAndActor(datasetObj, schema)
      }).recover {
        case NotFoundError(what) => notify(UnknownDataset)
        case t: Throwable        => notify(MetadataException(t))
      }
    }
  }

  def receive: Receive = {
    case CreateDataset(datasetObj, columns) =>
      createDataset(sender, datasetObj, columns)

    case SetupIngestion(dataset, columns, version, defaultPartKey) =>
      setupIngestion(sender, dataset, columns, version)

    case IngestRows(dataset, version, rows, seqNo) =>
      withDsCoord(sender, dataset, version) { _ ! DatasetCoordinatorActor.NewRows(sender, rows, seqNo) }

    case flushCmd @ Flush(dataset, version) =>
      withDsCoord(sender, dataset, version) { _ ! DatasetCoordinatorActor.StartFlush(Some(sender)) }

    case TruncateProjection(projection, version) =>
      withDsCoord(sender, projection.dataset, version) {
        _ ! DatasetCoordinatorActor.ClearProjection(sender, projection)
      }

    case CheckCanIngest(dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(DatasetCoordinatorActor.CanIngest) }

    case GetIngestionStats(dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(DatasetCoordinatorActor.GetStats) }

    case AddDatasetCoord(dataset, version, dsCoordRef) =>
      dsCoordinators((dataset, version)) = dsCoordRef

    case DatasetCreateNotify(dataset, version, msg) =>
      for { listener <- dsCoordNotify((dataset -> version)) } listener ! msg
      dsCoordNotify.remove((dataset -> version))
  }
}
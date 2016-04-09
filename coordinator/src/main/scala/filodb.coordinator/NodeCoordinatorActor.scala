package filodb.coordinator

import akka.actor.{Actor, ActorRef, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import scala.concurrent.Future
import scala.concurrent.duration._

import filodb.core._
import filodb.core.Types._
import filodb.core.metadata.{Column, DataColumn, Dataset, Projection, RichProjection}
import filodb.core.store.{ColumnStore, MetaStore}
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
object NodeCoordinatorActor extends NodeCommands {
  // Internal messages
  case object Reset
  case class AddDatasetCoord(dataset: DatasetRef, version: Int, dsCoordRef: ActorRef)
  case class DatasetCreateNotify(dataset: DatasetRef, version: Int, msg: Any)

  def invalidColumns(columns: Seq[String], schema: Column.Schema): Set[String] =
    (columns.toSet -- schema.keys)

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

  val dsCoordinators = new collection.mutable.HashMap[(DatasetRef, Int), ActorRef]
  val dsCoordNotify = new collection.mutable.HashMap[(DatasetRef, Int), List[ActorRef]]

  // By default, stop children DatasetCoordinatorActors when something goes wrong.
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def withDsCoord(originator: ActorRef, dataset: DatasetRef, version: Int)
                         (func: ActorRef => Unit): Unit = {
    dsCoordinators.get((dataset, version)).map(func).getOrElse(originator ! UnknownDataset)
  }

  private def verifySchema(originator: ActorRef, dataset: DatasetRef, version: Int, columns: Seq[String]):
      Future[Option[Column.Schema]] = {
    metaStore.getSchema(dataset, version).map { schema =>
      val undefinedCols = invalidColumns(columns, schema)
      if (undefinedCols.nonEmpty) {
        logger.info(s"Undefined columns $undefinedCols for dataset $dataset with schema $schema")
        originator ! UndefinedColumns(undefinedCols)
        None
      } else {
        Some(schema)
      }
    }
  }

  private def createDataset(originator: ActorRef,
                            datasetObj: Dataset,
                            ref: DatasetRef,
                            columns: Seq[DataColumn]): Unit = {
    if (datasetObj.projections.isEmpty) {
      originator ! DatasetError(s"There must be at least one projection in dataset $datasetObj")
    } else {
      (for { resp1 <- metaStore.newDataset(datasetObj) if resp1 == Success
             resp2 <- Future.sequence(columns.map(metaStore.newColumn(_, ref)))
             resp3 <- columnStore.initializeProjection(datasetObj.projections.head) }
      yield {
        originator ! DatasetCreated
      }).recover {
        case e: NoSuchElementException => originator ! DatasetAlreadyExists
        case e: StorageEngineException => originator ! e
        case e: Exception => originator ! DatasetError(e.toString)
      }
    }
  }

  private def truncateDataset(originator: ActorRef, projection: Projection): Unit = {
    columnStore.clearProjectionData(projection)
               .map { resp => originator ! ProjectionTruncated }
               .recover {
                 case e: Exception => originator ! DatasetError(e.getMessage)
               }
  }

  private def dropDataset(originator: ActorRef, dataset: DatasetRef): Unit = {
    (for { resp1 <- metaStore.deleteDataset(dataset)
           resp2 <- columnStore.dropDataset(dataset) if resp1 == Success } yield {
      if (resp2 == Success) originator ! DatasetDropped
    }).recover {
      case e: Exception => originator ! DatasetError(e.getMessage)
    }
  }

  // If the coordinator is already set up, then everything is already fine.
  // Otherwise get the dataset object and create a new actor, re-initializing state.
  private def setupIngestion(originator: ActorRef,
                             dataset: DatasetRef,
                             columns: Seq[String],
                             version: Int): Unit = {
    def notify(msg: Any): Unit = { self ! DatasetCreateNotify(dataset, version, msg) }

    def createDatasetCoordActor(datasetObj: Dataset, richProj: RichProjection): Unit = {
      val props = DatasetCoordinatorActor.props(richProj, version, columnStore, reprojector, config)
      val ref = context.actorOf(props, s"ds-coord-${datasetObj.name}-$version")
      self ! AddDatasetCoord(dataset, version, ref)
      notify(IngestionReady)
    }

    def createProjectionAndActor(datasetObj: Dataset, schema: Option[Column.Schema]): Unit = {
      val columnSeq = columns.map(schema.get(_))
      // Create the RichProjection, and ferret out any errors
      logger.debug(s"Creating projection from dataset $datasetObj, columns $columnSeq")
      val proj = RichProjection.make(datasetObj, columnSeq)
      proj.recover {
        case err: RichProjection.BadSchema => notify(BadSchema(err.toString))
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

  def datasetHandlers: Receive = LoggingReceive {
    case CreateDataset(datasetObj, columns, db) =>
      createDataset(sender, datasetObj, DatasetRef(datasetObj.name, db), columns)

    case TruncateProjection(projection, version) =>
      // First try through DS Coordinator so we could coordinate with flushes
      dsCoordinators.get((projection.dataset, version))
                    .map(_ ! DatasetCoordinatorActor.ClearProjection(sender, projection))
                    .getOrElse {
                      // Ok, so there is no DatasetCoordinatorActor, meaning no ingestion.  We should
                      // still be able to truncate a projection if it exists.
                      truncateDataset(sender, projection)
                    }

    case DropDataset(dataset) => dropDataset(sender, dataset)
  }

  def ingestHandlers: Receive = LoggingReceive {
    case SetupIngestion(dataset, columns, version) =>
      setupIngestion(sender, dataset, columns, version)

    case IngestRows(dataset, version, rows, seqNo) =>
      withDsCoord(sender, dataset, version) { _ ! DatasetCoordinatorActor.NewRows(sender, rows, seqNo) }

    case flushCmd @ Flush(dataset, version) =>
      withDsCoord(sender, dataset, version) { _ ! DatasetCoordinatorActor.StartFlush(Some(sender)) }

    case CheckCanIngest(dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(DatasetCoordinatorActor.CanIngest) }

    case GetIngestionStats(dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(DatasetCoordinatorActor.GetStats) }
  }

  def other: Receive = LoggingReceive {
    case Reset =>
      dsCoordinators.values.foreach(_ ! PoisonPill)
      dsCoordinators.clear()
      dsCoordNotify.clear()

    case AddDatasetCoord(dataset, version, dsCoordRef) =>
      dsCoordinators((dataset, version)) = dsCoordRef
      context.watch(dsCoordRef)

    case Terminated(childRef) =>
      dsCoordinators.find { case (key, ref) => ref == childRef }
                    .foreach { case (key, _) =>
                      logger.warn(s"Actor $childRef has terminated!  Ingestion for $key will stop.")
                      dsCoordinators.remove(key)
                    }

    case d @ DatasetCreateNotify(dataset, version, msg) =>
      logger.debug(s"$d")
      for { listener <- dsCoordNotify((dataset -> version)) } listener ! msg
      dsCoordNotify.remove((dataset -> version))
  }

  def receive: Receive = datasetHandlers orElse ingestHandlers orElse other
}
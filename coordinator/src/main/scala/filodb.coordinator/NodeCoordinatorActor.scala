package filodb.coordinator

import akka.actor.{Actor, ActorRef, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.cluster.Cluster
import akka.event.LoggingReceive
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import filodb.core._
import filodb.core.binaryrecord.RecordSchema
import filodb.core.metadata.{Column, DataColumn, Dataset, Projection, RichProjection}
import filodb.core.reprojector.Reprojector
import filodb.core.store.{ColumnStore, MetaStore}
import filodb.core.Types._

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
  // Internal messages
  case object Reset
  final case class ClearState(dataset: DatasetRef, version: Int)   // Clears the state of a single dataset
  final case class AddDatasetCoord(originator: ActorRef,
                                   dataset: DatasetRef,
                                   version: Int,
                                   dsCoordRef: ActorRef,
                                   reloadFlag: Boolean)
  final case class DatasetCreateNotify(dataset: DatasetRef, version: Int, msg: Any)
  case object ReloadDCA

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
  import DatasetCommands._
  import IngestionCommands._
  import context.dispatcher

  val dsCoordinators = new collection.mutable.HashMap[(DatasetRef, Int), ActorRef]
  val dsCoordNotify = new collection.mutable.HashMap[(DatasetRef, Int), List[ActorRef]]
  val clusterSelfAddr = Cluster(context.system).selfAddress
  val actorPath = clusterSelfAddr.host.getOrElse("None")

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
             resp2 <- metaStore.newColumns(columns, ref)
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
                             version: Int,
                             reloadFlag: Boolean = false): Unit = {
    def notify(msg: Any): Unit = { self ! DatasetCreateNotify(dataset, version, msg) }

    def createDatasetCoordActor(datasetObj: Dataset, richProj: RichProjection): Unit = {
      val props = DatasetCoordinatorActor.props(richProj, version, columnStore, reprojector, config, reloadFlag)
      val ref = context.actorOf(props, s"ds-coord-${datasetObj.name}-$version")
      self ! AddDatasetCoord(originator, dataset, version, ref, reloadFlag)
      if (!reloadFlag) {
        val colDefinitions = richProj.dataColumns.map(_.toString).mkString("\u0002")
        metaStore.insertIngestionState(actorPath, dataset, colDefinitions, "Started", version)
        notify(IngestionReady)
      }
    }

    def createProjectionAndActor(datasetObj: Dataset, schema: Option[Column.Schema]): Unit = {
      val columnSeq = columns.map(schema.get(_))
      Serializer.putSchema(RecordSchema(columnSeq))
      // Create the RichProjection, and ferret out any errors
      logger.debug(s"Creating projection from dataset $datasetObj, columns $columnSeq")
      val proj = RichProjection.make(datasetObj, columnSeq)
      proj.recover[Any] {
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

  private def reloadDatasetCoordActors(originator: ActorRef) : Unit = {
    logger.info(s"Reload of dataset coordinator actors has started for path: $actorPath")

    metaStore.getAllIngestionEntries(actorPath).map { entries =>
      if (entries.length > 0) {
        entries.foreach { ingestion =>
          val data = ingestion.toString().split("\u0001")
          val databaseOpt = if (data(1).isEmpty || data(1).equals("None")) None else Some(data(1))
          val ref = DatasetRef(data(2), databaseOpt)
          val columns = data(4).split("\u0002").map(col => DataColumn.fromString(col, data(2)))
          val projection = RichProjection(Await.result(metaStore.getDataset(ref), 10.second), columns)
          val colNames = projection.dataColumns.map(_.name)
          setupIngestion(originator, ref, colNames, data(3).toInt, true)
        }
      } else {
        originator ! DCAReady
      }
    }.recover { case e: Exception =>
      originator ! StorageEngineException(e)
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

    case ReloadIngestionState(originator, dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(DatasetCoordinatorActor.InitIngestion(originator)) }
  }

  def other: Receive = LoggingReceive {
    case Reset =>
      dsCoordinators.values.foreach(_ ! PoisonPill)
      dsCoordinators.clear()
      dsCoordNotify.clear()
      columnStore.reset()
      reprojector.clear()

    case ClearState(ref, version) =>
      dsCoordinators.get((ref, version)).foreach(_ ! PoisonPill)
      dsCoordinators.remove((ref, version))
      dsCoordNotify.remove((ref, version))
      // This is a bit heavy handed, it clears out the entire cache, not just for all datasets
      reprojector.clear()

    case AddDatasetCoord(originator, dataset, version, dsCoordRef, reloadFlag) =>
      dsCoordinators((dataset, version)) = dsCoordRef
      context.watch(dsCoordRef)
      if(reloadFlag){
        self ! ReloadIngestionState(originator, dataset, version)
      }

    case Terminated(childRef) =>
      dsCoordinators.find { case (key, ref) => ref == childRef }
                    .foreach { case ((datasetRef,version), _) =>
                      logger.warn(s"Actor $childRef has terminated!  Ingestion for ${(datasetRef,version)} will stop.")
                      dsCoordinators.remove((datasetRef,version))
                      // TODO @parekuti: update ingestion state
                      metaStore.updateIngestionState(actorPath, datasetRef, "Failed", "Error during ingestion", version)
                    }

    case d @ DatasetCreateNotify(dataset, version, msg) =>
      logger.debug(s"$d")
      for { listener <- dsCoordNotify((dataset -> version)) } listener ! msg
      dsCoordNotify.remove((dataset -> version))

    case ReloadDCA =>
      reloadDatasetCoordActors(sender)
  }

  def receive: Receive = ingestHandlers orElse datasetHandlers orElse other
}
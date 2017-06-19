package filodb.coordinator

import akka.actor.{Actor, ActorRef, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.cluster.Cluster
import akka.event.LoggingReceive
import com.typesafe.config.Config
import monix.execution.Scheduler
import net.ceedubs.ficus.Ficus._
import scala.collection.mutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import filodb.core._
import filodb.core.binaryrecord.RecordSchema
import filodb.core.memstore.MemStore
import filodb.core.metadata.{Column, DataColumn, Dataset, Projection, RichProjection}
import filodb.core.store.{ColumnStore, MetaStore}
import filodb.core.Types._

/**
 * The NodeCoordinatorActor is the common external API entry point for all FiloDB operations.
 * It is a singleton - there should be exactly one such actor per node/JVM process.
 * It is responsible for:
 * - Overall external FiloDB API.
 * - Metadata changes (dataset/column changes)
 * - Supervising, spinning up, cleaning up DatasetCoordinatorActors, QueryActors
 * - Forwarding new changes (rows) to other NodeCoordinatorActors if they are not local
 * - Forwarding rows to DatasetCoordinatorActors
 *
 * Since it is the API entry point its work should be very lightweight, mostly forwarding things to
 * other actors to do the real work.
 *
 * It is called by local (eg HTTP) as well as remote (eg Spark ETL) processes.
 */
object NodeCoordinatorActor {
  // Internal messages
  case object Reset
  final case class ClusterHello(clusterActor: ActorRef)            // Sent from ClusterActor upon joining
  final case class ClearState(dataset: DatasetRef, version: Int)   // Clears the state of a single dataset
  case object ReloadDCA

  def props(metaStore: MetaStore,
            memStore: MemStore,
            columnStore: ColumnStore,
            config: Config): Props =
    Props(classOf[NodeCoordinatorActor], metaStore, memStore, columnStore, config)
}

private[filodb] final class NodeCoordinatorActor(metaStore: MetaStore,
                                                 memStore: MemStore,
                                                 columnStore: ColumnStore,
                                                 config: Config) extends BaseActor {
  import NodeCoordinatorActor._
  import DatasetCommands._
  import IngestionCommands._
  import QueryCommands._
  import context.dispatcher

  val dsCoordinators = new HashMap[(DatasetRef, Int), ActorRef]
  val queryActors = new HashMap[DatasetRef, ActorRef]
  val shardMaps = new HashMap[DatasetRef, ShardMapper]
  val clusterSelfAddr = Cluster(context.system).selfAddress
  val actorPath = clusterSelfAddr.host.getOrElse("None")
  var clusterActor: Option[ActorRef] = None

  // The thread pool used by Monix Observables/reactive ingestion
  val ingestScheduler = Scheduler.computation(config.getInt("ingestion-threads"))

  // By default, stop children DatasetCoordinatorActors when something goes wrong.
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def withDsCoord(originator: ActorRef, dataset: DatasetRef, version: Int)
                         (func: ActorRef => Unit): Unit = {
    dsCoordinators.get((dataset, version)).map(func).getOrElse(originator ! UnknownDataset)
  }

  // For now, datasets need to be set up for ingestion before they can be queried (in-mem only)
  // TODO: if we ever support query API against cold (not in memory) datasets, change this
  private def withQueryActor(originator: ActorRef, dataset: DatasetRef)(func: ActorRef => Unit): Unit =
    queryActors.get(dataset).map(func).getOrElse(originator ! UnknownDataset)

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

  // Initializes both a DatasetCoordinatorActor as well as a QueryActor
  private def setupDataset(originator: ActorRef, ds: DatasetSetup): Unit = {
    val columns = ds.encodedColumns.map(s => DataColumn.fromString(s, ds.dataset.name))

    logger.debug(s"Creating projection from dataset ${ds.dataset}, columns $columns")
    // This should not fail, it should have been ferreted out by NodeClusterActor first
    val proj = RichProjection(ds.dataset, columns)
    val ref = proj.datasetRef
    Serializer.putPartitionSchema(RecordSchema(proj.partitionColumns))
    Serializer.putDataSchema(RecordSchema(proj.nonPartitionColumns))

    val props = MemStoreCoordActor.props(proj, memStore, ds.source)(ingestScheduler)
    val ingestRef = context.actorOf(props, s"ms-coord-${proj.dataset.name}-${ds.version}")
    dsCoordinators((ref, ds.version)) = ingestRef
    val shardMapMsg = NodeClusterActor.ShardMapUpdate(ref, shardMaps(ref))
    ingestRef ! shardMapMsg
    context.watch(ingestRef)

    logger.info(s"Creating QueryActor for dataset $ref")
    val queryRef = context.actorOf(QueryActor.props(memStore, proj), s"query-$ref")
    queryActors(ref) = queryRef
    queryRef ! shardMapMsg

    // Now, we should be good to go for both ingest and query.
    // TODO: Send status update to cluster actor
    logger.info(s"Coordinator set up for ingestion and querying for $ref...")
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
          // setupDataset(originator, projection.dataset, colNames, data(3).toInt)
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
    case ds @ DatasetSetup(dataset, _, version, _) =>
      val ref = dataset.projections.head.dataset
      if (!(dsCoordinators.contains((ref, version)))) { setupDataset(sender, ds) }
      else { logger.warn(s"Getting redundant DatasetSetup for dataset $dataset") }

    case IngestRows(dataset, version, rows) =>
      withDsCoord(sender, dataset, version) { _ ! MemStoreCoordActor.IngestRows(sender, rows) }

    case flushCmd @ Flush(dataset, version) =>
      withDsCoord(sender, dataset, version) { _ ! DatasetCoordinatorActor.StartFlush(Some(sender)) }

    case CheckCanIngest(dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(DatasetCoordinatorActor.CanIngest) }

    case GetIngestionStats(dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(MemStoreCoordActor.GetStatus) }

    case ReloadIngestionState(originator, dataset, version) =>
      withDsCoord(sender, dataset, version) { _.forward(DatasetCoordinatorActor.InitIngestion(originator)) }
  }

  def queryHandlers: Receive = LoggingReceive {
    case q: QueryCommand =>
      val originator = sender
      withQueryActor(originator, q.dataset) { _.tell(q, originator) }
  }

  def other: Receive = LoggingReceive {
    case Reset =>
      dsCoordinators.values.foreach(_ ! PoisonPill)
      dsCoordinators.clear()
      queryActors.values.foreach(_ ! PoisonPill)
      queryActors.clear()
      columnStore.reset()
      memStore.reset()

    case ClearState(ref, version) =>
      dsCoordinators.get((ref, version)).foreach(_ ! PoisonPill)
      dsCoordinators.remove((ref, version))
      // This is a bit heavy handed, it clears out the entire cache, not just for all datasets
      memStore.reset()

    case Terminated(childRef) =>
      dsCoordinators.find { case (key, ref) => ref == childRef }
                    .foreach { case ((datasetRef,version), _) =>
                      logger.warn(s"Actor $childRef has terminated!  Ingestion for ${(datasetRef,version)} will stop.")
                      dsCoordinators.remove((datasetRef,version))
                      // TODO @parekuti: update ingestion state
                      // metaStore.updateIngestionState(actorPath, datasetRef, "Failed", "Error during ingestion", version)
                    }

    case u @ NodeClusterActor.ShardMapUpdate(ref, newMap) =>
      logger.info(s"Received ShardMapUpdate for dataset $ref... updating QueryActor and DSCoordActor...")
      shardMaps(ref) = newMap
      queryActors.get(ref).foreach(_ ! u)
      dsCoordinators.get((ref, 0)).foreach(_ ! u)

    case ClusterHello(clusterRef) =>
      logger.info(s"NodeClusterActor $clusterRef said hello!")
      clusterActor = Some(clusterRef)

    case MiscCommands.GetClusterActor =>
      sender ! clusterActor

    case ReloadDCA =>
      reloadDatasetCoordActors(sender)
  }

  def receive: Receive = queryHandlers orElse ingestHandlers orElse datasetHandlers orElse other
}
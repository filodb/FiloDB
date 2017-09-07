package filodb.coordinator

import scala.collection.mutable.HashMap
import scala.concurrent.duration._
import scala.concurrent.Await

import akka.actor.{ActorRef, Address, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import monix.execution.Scheduler

import filodb.core._
import filodb.core.binaryrecord.RecordSchema
import filodb.core.memstore.MemStore
import filodb.core.metadata._
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

  /** Clears the state of a single dataset. */
  final case class ClearState(dataset: DatasetRef, version: Int)

  case object ReloadDCA

  def props(metaStore: MetaStore,
            memStore: MemStore,
            columnStore: ColumnStore,
            selfAddress: Address,
            config: Config): Props =
    Props(classOf[NodeCoordinatorActor], metaStore, memStore, columnStore, selfAddress, config)
}

private[filodb] final class NodeCoordinatorActor(metaStore: MetaStore,
                                                 memStore: MemStore,
                                                 columnStore: ColumnStore,
                                                 selfAddress: Address,
                                                 config: Config) extends NamingAwareBaseActor {
  import NodeCoordinatorActor._
  import NodeClusterActor._
  import DatasetCommands._
  import IngestionCommands._
  import context.dispatcher

  val settings = new FilodbSettings(config)
  val dsCoordinators = new HashMap[(DatasetRef, Int), ActorRef]
  val actorPath = selfAddress.host.getOrElse("None")
  var clusterActor: Option[ActorRef] = None
  var shardActor: Option[ActorRef] = None

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
   childFor(dataset, ActorName.Query).map(func).getOrElse(originator ! UnknownDataset)

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

  /** Creates a new ingestion actor initialized with the shard actor,
    * and sends it a sequence of `StartShardIngestion` commands created
    * during dataset setup.
    *
    * Creates a QueryActor, subscribes it to shard events, keeping
    * it decoupled from the shard actor. The QueryActor will receive an
    * initial `CurrentShardSnapshot` to initialize it's local `ShardMapper`
    * for the dataset, which is managed by the shard actor.
    */
  private def setupDataset(ds: DatasetSetup, origin: ActorRef): Unit = {
    import ActorName.{Ingestion, Query}

    val columns = ds.encodedColumns.map(s => DataColumn.fromString(s, ds.dataset.name))

    logger.debug(s"Creating projection from dataset ${ds.dataset}, columns $columns")
    // This should not fail, it should have been ferreted out by NodeClusterActor first
    val proj = RichProjection(ds.dataset, columns)
    val ref = proj.datasetRef
    Serializer.putPartitionSchema(RecordSchema(proj.partitionColumns))
    Serializer.putDataSchema(RecordSchema(proj.nonPartitionColumns))

    shardActor match {
      case Some(shardStatus) =>
        val props = MemStoreCoordActor.props(proj, memStore, ds.source, shardStatus)(ingestScheduler)
        val ingester = context.actorOf(props, s"$Ingestion-${proj.dataset.name}-${ds.version}")
        context.watch(ingester)
        dsCoordinators((ref, ds.version)) = ingester

        logger.info(s"Creating QueryActor for dataset $ref")
        val queryRef = context.actorOf(QueryActor.props(memStore, proj), s"$Query-$ref")
        shardStatus ! ShardSubscriptions.Subscribe(queryRef, ref)

        // TODO: Send status update to cluster actor
        logger.info(s"Coordinator set up for ingestion and querying for $ref.")
      case _ =>
        // shouldn't happen
        logger.error(s"Shard actor not set up for shard assignment, management and events.")
    }
  }

  private def reloadDatasetCoordActors(originator: ActorRef) : Unit = {
    logger.info(s"Reload of dataset coordinator actors has started for path: $actorPath")

    metaStore.getAllIngestionEntries(actorPath).map {
      case entries if entries.nonEmpty =>
        entries.foreach { ingestion =>
          val data = ingestion.toString().split("\u0001")
          val databaseOpt = if (data(1).isEmpty || data(1).equals("None")) None else Some(data(1))
          val ref = DatasetRef(data(2), databaseOpt)
          val columns = data(4).split("\u0002").map(col => DataColumn.fromString(col, data(2)))
          val projection = RichProjection(Await.result(metaStore.getDataset(ref), 10.second), columns)
          val colNames = projection.dataColumns.map(_.name)
          // setupDataset(originator, projection.dataset, colNames, data(3).toInt)
        }
      case entries =>
        originator ! DCAReady
    }.recover { case e: Exception =>
      originator ! StorageEngineException(e)
    }
  }

  def datasetHandlers: Receive = LoggingReceive {
    case CreateDataset(datasetObj, columns, db) =>
      createDataset(sender(), datasetObj, DatasetRef(datasetObj.name, db), columns)

    case TruncateProjection(projection, version) =>
      // First try through DS Coordinator so we could coordinate with flushes
      dsCoordinators.get((projection.dataset, version)) match {
        case Some(dsCoordinator) =>
          dsCoordinator ! DatasetCoordinatorActor.ClearProjection(sender(), projection)
        case _ =>
          // Ok, so there is no DatasetCoordinatorActor, meaning no ingestion.  We should
          // still be able to truncate a projection if it exists.
          truncateDataset(sender(), projection)
      }

    case DropDataset(dataset) => dropDataset(sender(), dataset)
  }

  def ingestHandlers: Receive = LoggingReceive {
    case ds @ DatasetSetup(dataset, _, version, _) =>
      val ref = dataset.projections.head.dataset
      if (!(dsCoordinators.contains((ref, version)))) { setupDataset(ds, sender()) }
      else { logger.warn(s"Getting redundant DatasetSetup for dataset $dataset") }

    case IngestRows(dataset, version, shard, rows) =>
      withDsCoord(sender(), dataset, version) { _ ! MemStoreCoordActor.IngestRows(sender(), shard, rows) }

    case flushCmd @ Flush(dataset, version) =>
      withDsCoord(sender(), dataset, version) { _ ! DatasetCoordinatorActor.StartFlush(Some(sender())) }

    case CheckCanIngest(dataset, version) =>
      withDsCoord(sender(), dataset, version) { _.forward(DatasetCoordinatorActor.CanIngest) }

    case GetIngestionStats(dataset, version) =>
      withDsCoord(sender(), dataset, version) { _.forward(MemStoreCoordActor.GetStatus) }

    case ReloadIngestionState(originator, dataset, version) =>
      withDsCoord(sender(), dataset, version) { _.forward(DatasetCoordinatorActor.InitIngestion(originator)) }
  }

  def queryHandlers: Receive = LoggingReceive {
    case q: QueryCommand =>
      val originator = sender()
      withQueryActor(originator, q.dataset) { _.tell(q, originator) }
    case q: QueryActor.SingleShardQuery =>
      val originator = sender()
      withQueryActor(originator, q.dataset) { _.tell(q, originator) }
  }

  def coordinatorReceive: Receive = LoggingReceive {
    case e: CoordinatorRegistered     => registered(e)
    case e: ShardCommand              => forward(e, sender())
    case Terminated(memstoreCoord)    => terminated(memstoreCoord)
    case MiscCommands.GetClusterActor => sender() ! clusterActor
    case ReloadDCA                    => reloadDatasetCoordActors(sender())
    case ClearState(ref, version)     => clearState(ref, version)
    case NodeProtocol.ResetState      => reset(sender())
  }

  def receive: Receive = queryHandlers orElse ingestHandlers orElse datasetHandlers orElse coordinatorReceive

  private def registered(e: CoordinatorRegistered): Unit = {
    logger.info(s"${e.clusterActor} said hello!")
    clusterActor = Some(e.clusterActor)
    shardActor = Some(e.shardActor)
  }

  /** Forwards shard commands to the ingester for the given dataset.
    * TODO version match if needed, when > 1, currently only 0.
    */
  private def forward(command: ShardCommand, origin: ActorRef): Unit =
    dsCoordinators.collectFirst { case ((ds, _), actor) if ds == command.ref => actor }
      match {
        case Some(actor) =>
          actor.tell(command, origin)
        case _ =>
          logger.warn(s"No MemstoreCoordinator for dataset ${command.ref}.")
      }

  private def terminated(memstoreCoord: ActorRef): Unit =
    dsCoordinators.find { case (key, ref) => ref == memstoreCoord }
      .foreach { case ((datasetRef,version), _) =>
        logger.warn(s"$memstoreCoord terminated. Stopping ingestion for ${(datasetRef, version)}.")
        dsCoordinators.remove((datasetRef,version))
        // TODO @parekuti: update ingestion state
        // metaStore.updateIngestionState(actorPath, datasetRef, "Failed", "Error during ingestion", version)
      }

  private def reset(origin: ActorRef): Unit = {
    context.children foreach (_ ! PoisonPill)
    dsCoordinators.clear()
    columnStore.reset()
    memStore.reset()
  }

  private def clearState(ref: DatasetRef, version: Int): Unit = {
    dsCoordinators.get((ref, version)).foreach(_ ! PoisonPill)
    dsCoordinators.remove((ref, version))
    // This is a bit heavy handed, it clears out the entire cache, not just for all datasets
    memStore.reset()
  }
}
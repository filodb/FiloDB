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
 * - Supervising, spinning up, cleaning up IngestionActors, QueryActors
 * - Forwarding new changes (rows) to other NodeCoordinatorActors if they are not local
 * - Forwarding rows to IngestionActors
 *
 * Since it is the API entry point its work should be very lightweight, mostly forwarding things to
 * other actors to do the real work.
 *
 * It is called by local (eg HTTP) as well as remote (eg Spark ETL) processes.
 */
object NodeCoordinatorActor {

  /** Clears the state of a single dataset. */
  final case class ClearState(dataset: DatasetRef, version: Int)

  def props(metaStore: MetaStore,
            memStore: MemStore,
            selfAddress: Address,
            config: Config): Props =
    Props(classOf[NodeCoordinatorActor], metaStore, memStore, selfAddress, config)
}

private[filodb] final class NodeCoordinatorActor(metaStore: MetaStore,
                                                 memStore: MemStore,
                                                 selfAddress: Address,
                                                 config: Config) extends NamingAwareBaseActor {
  import NodeCoordinatorActor._
  import NodeClusterActor._
  import DatasetCommands._
  import IngestionCommands._
  import context.dispatcher

  val settings = new FilodbSettings(config)
  val ingesters = new HashMap[(DatasetRef, Int), ActorRef]
  val actorPath = selfAddress.host.getOrElse("None")
  var clusterActor: Option[ActorRef] = None
  var shardActor: Option[ActorRef] = None

  // The thread pool used by Monix Observables/reactive ingestion
  val ingestScheduler = Scheduler.computation(config.getInt("ingestion-threads"))

  // By default, stop children IngestionActors when something goes wrong.
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def withIngester(originator: ActorRef, dataset: DatasetRef, version: Int)
                          (func: ActorRef => Unit): Unit = {
    ingesters.get((dataset, version)).map(func).getOrElse(originator ! UnknownDataset)
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
             resp2 <- metaStore.newColumns(columns, ref) }
      yield {
        originator ! DatasetCreated
      }).recover {
        case e: NoSuchElementException => originator ! DatasetAlreadyExists
        case e: StorageEngineException => originator ! e
        case e: Exception => originator ! DatasetError(e.toString)
      }
    }
  }

  // TODO: move createDataset and truncateDataset into NodeClusterActor.  truncate() needs distributed coord
  private def truncateDataset(originator: ActorRef, dataset: DatasetRef): Unit = {
    try {
      memStore.truncate(dataset)
      originator ! DatasetTruncated
    } catch {
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
        val props = IngestionActor.props(proj, memStore, ds.source, shardStatus)(ingestScheduler)
        val ingester = context.actorOf(props, s"$Ingestion-${proj.dataset.name}-${ds.version}")
        context.watch(ingester)
        ingesters((ref, ds.version)) = ingester

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

  def datasetHandlers: Receive = LoggingReceive {
    case CreateDataset(datasetObj, columns, db) =>
      createDataset(sender(), datasetObj, DatasetRef(datasetObj.name, db), columns)

    case TruncateDataset(ref) =>
      truncateDataset(sender(), ref)
  }

  def ingestHandlers: Receive = LoggingReceive {
    case ds @ DatasetSetup(dataset, _, version, _) =>
      val ref = dataset.projections.head.dataset
      if (!(ingesters.contains((ref, version)))) { setupDataset(ds, sender()) }
      else { logger.warn(s"Getting redundant DatasetSetup for dataset $dataset") }

    case IngestRows(dataset, version, shard, rows) =>
      withIngester(sender(), dataset, version) { _ ! IngestionActor.IngestRows(sender(), shard, rows) }

    case GetIngestionStats(dataset, version) =>
      withIngester(sender(), dataset, version) { _.forward(IngestionActor.GetStatus) }
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
    case ClearState(ref, version)     => clearState(ref, version)
    case NodeProtocol.ResetState      => reset(sender())
    case e: ShardEvent                => // NOP for now
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
    ingesters.collectFirst { case ((ds, _), actor) if ds == command.ref => actor }
      match {
        case Some(actor) =>
          actor.tell(command, origin)
        case _ =>
          logger.warn(s"No MemstoreCoordinator for dataset ${command.ref}.")
      }

  private def terminated(ingester: ActorRef): Unit =
    ingesters.find { case (key, ref) => ref == ingester }
      .foreach { case ((datasetRef,version), _) =>
        logger.warn(s"$ingester terminated. Stopping ingestion for ${(datasetRef, version)}.")
        ingesters.remove((datasetRef,version))
        // TODO @parekuti: update ingestion state
        // metaStore.updateIngestionState(actorPath, datasetRef, "Failed", "Error during ingestion", version)
      }

  private def reset(origin: ActorRef): Unit = {
    context.children foreach (_ ! PoisonPill)
    ingesters.clear()
    memStore.reset()
  }

  private def clearState(ref: DatasetRef, version: Int): Unit = {
    ingesters.get((ref, version)).foreach(_ ! PoisonPill)
    ingesters.remove((ref, version))
    // This is a bit heavy handed, it clears out the entire cache, not just for all datasets
    memStore.reset()
  }
}
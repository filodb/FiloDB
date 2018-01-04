package filodb.coordinator

import scala.collection.mutable.HashMap

import akka.actor.{ActorRef, PoisonPill, Props, SupervisorStrategy, Terminated}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import monix.execution.Scheduler

import filodb.core._
import filodb.core.memstore.MemStore
import filodb.core.metadata._
import filodb.core.store.MetaStore

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
  final case class ClearState(dataset: DatasetRef)

  def props(metaStore: MetaStore,
            memStore: MemStore,
            config: Config): Props =
    Props(classOf[NodeCoordinatorActor], metaStore, memStore, config)
}

private[filodb] final class NodeCoordinatorActor(metaStore: MetaStore,
                                                 memStore: MemStore,
                                                 config: Config) extends NamingAwareBaseActor {
  import context.dispatcher

  import DatasetCommands._
  import IngestionCommands._
  import NodeClusterActor._
  import NodeCoordinatorActor._

  val settings = new FilodbSettings(config)
  val ingesters = new HashMap[DatasetRef, ActorRef]
  var clusterActor: Option[ActorRef] = None

  // The thread pool used by Monix Observables/reactive ingestion
  val ingestScheduler = Scheduler.computation(config.getInt("ingestion-threads"))

  // By default, stop children IngestionActors when something goes wrong.
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  private def withIngester(originator: ActorRef, dataset: DatasetRef)
                          (func: ActorRef => Unit): Unit = {
    ingesters.get(dataset).map(func).getOrElse(originator ! UnknownDataset)
  }

  // For now, datasets need to be set up for ingestion before they can be queried (in-mem only)
  // TODO: if we ever support query API against cold (not in memory) datasets, change this
  private def withQueryActor(originator: ActorRef, dataset: DatasetRef)(func: ActorRef => Unit): Unit =
   childFor(dataset, ActorName.Query).map(func).getOrElse(originator ! UnknownDataset)

  private def createDataset(originator: ActorRef,
                            datasetObj: Dataset): Unit = {
    (for {
      resp1 <- metaStore.newDataset(datasetObj) if resp1 == Success
      resp2 <- memStore.sink.initialize(datasetObj.ref) if resp2 == Success
    }
    yield {
      originator ! DatasetCreated
    }).recover {
      case e: NoSuchElementException => originator ! DatasetAlreadyExists
      case e: StorageEngineException => originator ! e
      case e: Exception => originator ! DatasetError(e.toString)
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
  private def setupDataset(dataset: Dataset, source: IngestionSource, origin: ActorRef): Unit = {
    import ActorName.{Ingestion, Query}

    logger.debug(s"Recreated dataset $dataset from string")
    val ref = dataset.ref
    Serializer.putPartitionSchema(dataset.partitionBinSchema)
    Serializer.putDataSchema(dataset.dataBinSchema)

    clusterActor match {
      case Some(nca) =>
        val props = IngestionActor.props(dataset, memStore, source, nca)(ingestScheduler)
        val ingester = context.actorOf(props, s"$Ingestion-${dataset.name}")
        context.watch(ingester)
        ingesters(ref) = ingester

        logger.info(s"Creating QueryActor for dataset $ref")
        val queryRef = context.actorOf(QueryActor.props(memStore, dataset), s"$Query-$ref")
        nca.tell(SubscribeShardUpdates(ref), queryRef)

        // TODO: Send status update to cluster actor
        logger.info(s"Coordinator set up for ingestion and querying for $ref.")
      case _ =>
        // shouldn't happen
        logger.error(s"Shard actor not set up for shard assignment, management and events.")
    }
  }

  def datasetHandlers: Receive = LoggingReceive {
    case CreateDataset(datasetObj, db) =>
      createDataset(sender(), datasetObj.copy(database = db))

    case TruncateDataset(ref) =>
      truncateDataset(sender(), ref)
  }

  def ingestHandlers: Receive = LoggingReceive {
    case DatasetSetup(compactDSString, source) =>
      val dataset = Dataset.fromCompactString(compactDSString)
      if (!(ingesters contains dataset.ref)) { setupDataset(dataset, source, sender()) }
      else { logger.warn(s"Getting redundant DatasetSetup for dataset ${dataset.ref}") }

    case IngestRows(dataset, shard, rows) =>
      withIngester(sender(), dataset) { _ ! IngestionActor.IngestRows(sender(), shard, rows) }

    case GetIngestionStats(dataset) =>
      withIngester(sender(), dataset) { _.forward(IngestionActor.GetStatus) }
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
    case ClearState(ref)              => clearState(ref)
    case NodeProtocol.ResetState      => reset(sender())
    case e: ShardEvent                => // NOP for now
  }

  def receive: Receive = queryHandlers orElse ingestHandlers orElse datasetHandlers orElse coordinatorReceive

  private def registered(e: CoordinatorRegistered): Unit = {
    logger.info(s"${e.clusterActor} said hello!")
    clusterActor = Some(e.clusterActor)
  }

  /** Forwards shard commands to the ingester for the given dataset.
    * TODO version match if needed, when > 1, currently only 0.
    */
  private def forward(command: ShardCommand, origin: ActorRef): Unit =
    ingesters.get(command.ref) match {
      case Some(actor) =>
        actor.tell(command, origin)
      case _ =>
        logger.warn(s"No IngestionActor for dataset ${command.ref}")
    }

  private def terminated(ingester: ActorRef): Unit = {
    memStore.shutdown()
    ingesters.find { case (key, ref) => ref == ingester }
      .foreach { case (datasetRef, _) =>
        logger.warn(s"$ingester terminated. Stopping ingestion for ${(datasetRef)}.")
        ingesters.remove(datasetRef)
      }
  }

  private def reset(origin: ActorRef): Unit = {
    context.children foreach (_ ! PoisonPill)
    ingesters.clear()
    memStore.reset()
  }

  private def clearState(ref: DatasetRef): Unit = {
    ingesters.get((ref)).foreach(_ ! PoisonPill)
    ingesters.remove((ref))
    // This is a bit heavy handed, it clears out the entire cache, not just for all datasets
    memStore.reset()
  }
}
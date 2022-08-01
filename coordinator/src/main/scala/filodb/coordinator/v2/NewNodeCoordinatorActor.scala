package filodb.coordinator.v2

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}

import akka.actor.{ActorRef, OneForOneStrategy, Props}
import akka.actor.SupervisorStrategy.Resume
import akka.event.LoggingReceive
import kamon.Kamon

import filodb.coordinator._
import filodb.coordinator.v2.NewNodeCoordinatorActor.InitNewNodeCoordinatorActor
import filodb.core._
import filodb.core.downsample.DownsampleConfig
import filodb.core.memstore.TimeSeriesStore
import filodb.core.metadata._
import filodb.core.store.{IngestionConfig, StoreConfig}
import filodb.query.QueryCommand

final case class GetShardMapScatter(ref: DatasetRef)

object NewNodeCoordinatorActor {

  final case object InitNewNodeCoordinatorActor

  def props(memStore: TimeSeriesStore,
            clusterDiscovery: FiloDbClusterDiscovery,
            settings: FilodbSettings): Props =
    Props(classOf[NewNodeCoordinatorActor], memStore, clusterDiscovery, settings)
}

private[filodb] final class NewNodeCoordinatorActor(memStore: TimeSeriesStore,
                                                    clusterDiscovery: FiloDbClusterDiscovery,
                                                    settings: FilodbSettings) extends BaseActor {

  import NodeClusterActor._
  import client.IngestionCommands._

  private val ingestionActors = new HashMap[DatasetRef, ActorRef]
  private val queryActors = new HashMap[DatasetRef, ActorRef]
  private val localShardMaps = new HashMap[DatasetRef, ShardMapper]
  private val shardsOnThisNode = new HashMap[DatasetRef, Seq[Int]]
  private val ingestionConfigs = new mutable.HashMap[DatasetRef, IngestionConfig]()
  private val shardStats = new mutable.HashMap[DatasetRef, ShardHealthStats]()

  logger.info(s"Initializing NodeCoordActor at ${self.path}")

  private def initialize() = {
    logger.debug(s"Initializing stream configs: ${settings.streamConfigs}")
    settings.streamConfigs.foreach { config =>
      val dataset = settings.datasetFromStream(config)
      val ingestion = IngestionConfig(config, NodeClusterActor.noOpSource.streamFactoryClass).get
      initializeDataset(dataset, ingestion)
    }
  }

  // By default, stop children IngestionActors when something goes wrong.
  // restart query actors though
  override val supervisorStrategy = OneForOneStrategy() {
    case exception: Exception => Resume
  }

  // For now, datasets need to be set up for ingestion before they can be queried (in-mem only)
  // TODO: if we ever support query API against cold (not in memory) datasets, change this
  private def withQueryActor(originator: ActorRef, dataset: DatasetRef)(func: ActorRef => Unit): Unit =
    queryActors.get(dataset).map(func).getOrElse(originator ! UnknownDataset)

  private def initializeDataset(dataset: Dataset, ingestConfig: IngestionConfig): Unit = {
    logger.info(s"Initializing dataset ${dataset.ref}")
    ingestionConfigs.put(dataset.ref, ingestConfig)
    localShardMaps.put(dataset.ref, new ShardMapper(ingestConfig.numShards))
    shardStats.put(dataset.ref, new ShardHealthStats(dataset.ref))
    clusterDiscovery.registerDatasetForDiscovery(dataset.ref, ingestConfig.numShards)
    // FIXME initialization of cass tables below for dev environments is async - need to wait before continuing
    // for now if table is not initialized in dev on first run, simply restart server :(
    memStore.store.initialize(dataset.ref, ingestConfig.numShards)
    // if downsampling is enabled, then initialize downsample datasets
    ingestConfig.downsampleConfig
                .downsampleDatasetRefs(dataset.ref.dataset)
                .foreach { downsampleDataset => memStore.store.initialize(downsampleDataset, ingestConfig.numShards) }

    setupDataset( dataset,
                  ingestConfig.storeConfig,
                  IngestionSource(ingestConfig.streamFactoryClass, ingestConfig.sourceConfig),
                  ingestConfig.downsampleConfig)
    initShards(dataset, ingestConfig)
  }

  private def initShards(dataset: Dataset, ic: IngestionConfig): Unit = {
    val mapper = localShardMaps(dataset.ref)
    val shardsToStart = clusterDiscovery.shardsForLocalhost(ic.numShards)
    shardsToStart.foreach(sh => updateFromShardEvent(ShardAssignmentStarted(dataset.ref, sh, self)))
    shardsOnThisNode.put(dataset.ref, shardsToStart)
    ingestionActors(dataset.ref) ! ShardIngestionState(0, dataset.ref, mapper)
  }

  private def updateFromShardEvent(event: ShardEvent): Unit = {
    localShardMaps.get(event.ref).foreach { mapper =>
      mapper.updateFromEvent(event) match {
        case Failure(l) =>
          logger.error(s"updateFromShardEvent error for dataset=${event.ref} event $event. Mapper now: $mapper", l)
        case Success(r) =>
          logger.debug(s"updateFromShardEvent success for dataset=${event.ref} event $event. Mapper now: $mapper")
      }
      // update metrics
      shardStats(event.ref).update(mapper, skipUnassigned = true)
    }
  }

  /** Creates a new ingestion actor initialized with the shard actor,
    * and sends it a shard resync command created.
    *
    * Creates a QueryActor, subscribes it to shard events, keeping
    * it decoupled from the shard actor. The QueryActor will receive an
    * initial `CurrentShardSnapshot` to initialize it's local `ShardMapper`
    * for the dataset, which is managed by the shard actor.
    */
  private def setupDataset(dataset: Dataset,
                           storeConf: StoreConfig,
                           source: IngestionSource,
                           downsample: DownsampleConfig,
                           schemaOverride: Boolean = false): Unit = {
    import ActorName.Ingestion

    logger.debug(s"Recreated dataset $dataset from string")
    val ref = dataset.ref

    val schemas = if (schemaOverride) Schemas(dataset.schema) else settings.schemas
    if (schemaOverride) logger.info(s"Overriding schemas from settings: this better be a test!")
    val props = IngestionActor.props(dataset.ref, schemas, memStore,
                                     source, downsample, storeConf, self)
    val ingester = context.actorOf(props, s"$Ingestion-${dataset.name}")
    context.watch(ingester)
    ingestionActors(ref) = ingester

    val ttl = if (memStore.isDownsampleStore) downsample.ttls.last.toMillis
              else storeConf.diskTTLSeconds.toLong * 1000
    def earliestTimestampFn = System.currentTimeMillis() - ttl
    def clusterShardMapperFn = clusterDiscovery.shardMapper(dataset.ref)
    logger.info(s"Creating QueryActor for dataset $ref with dataset ttlMs=$ttl")
    val queryRef = context.actorOf(QueryActor.props(memStore, dataset, schemas,
                                                    clusterShardMapperFn, earliestTimestampFn))
    queryActors(ref) = queryRef

    // TODO: Send status update to cluster actor
    logger.info(s"Coordinator set up for ingestion and querying for $ref.")
  }

  def queryHandlers: Receive = LoggingReceive {
    case q: QueryCommand =>
      val originator = sender()
      Kamon.currentSpan().mark("NodeCoordinatorActor received query")
      withQueryActor(originator, q.dataset) { _.tell(q, originator) }
    case QueryActor.ThrowException(dataset) =>
      val originator = sender()
      withQueryActor(originator, dataset) { _.tell(QueryActor.ThrowException(dataset), originator) }
  }

  def shardManagementHandlers: Receive = LoggingReceive {
      // sent by ingestion actors when shard status changes
      case e: ShardEvent => updateFromShardEvent(e)

      // requested from CLI and HTTP API
      case g: GetShardMap =>
        try {
          val replyTo = sender()
          sender() ! CurrentShardSnapshot(g.ref, clusterDiscovery.shardMapper(g.ref))
        } catch { case e: Exception =>
          logger.error(s"Error occurred when processing message $g", e)
        }
    // requested from peer NewNodeCoordActors upon them receiving GetShardMap call
    case g: GetShardMapScatter =>
      try {
        sender() ! CurrentShardSnapshot(g.ref, localShardMaps(g.ref))
      } catch { case e: Exception =>
        logger.error(s"Error occurred when processing message $g", e)
      }
  }

  def initHandler: Receive = {
    case InitNewNodeCoordinatorActor => initialize()
  }

  def receive: Receive = queryHandlers orElse shardManagementHandlers orElse initHandler


}

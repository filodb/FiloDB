package filodb.coordinator

import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}

import akka.actor.{ActorRef, OneForOneStrategy, PoisonPill, Props}
import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.event.LoggingReceive
import kamon.Kamon

import filodb.core._
import filodb.core.downsample.DownsampleConfig
import filodb.core.memstore.TimeSeriesStore
import filodb.core.metadata._
import filodb.core.store.{IngestionConfig, StoreConfig}
import filodb.query.QueryCommand

object NewNodeCoordinatorActor {

  /** Clears the state of a single dataset. */
  final case class ClearState(dataset: DatasetRef)

  def props(memStore: TimeSeriesStore,
            shardAssignmentStrategy: ShardAssignmentStrategy,
            settings: FilodbSettings): Props =
    Props(classOf[NewNodeCoordinatorActor], memStore, shardAssignmentStrategy, settings)
}

private[filodb] final class NewNodeCoordinatorActor(memStore: TimeSeriesStore,
                                                    shardAssignmentStrategy: ShardAssignmentStrategy,
                                                    settings: FilodbSettings) extends BaseActor {

  import NodeClusterActor._
  import client.IngestionCommands._

  val ingesters = new HashMap[DatasetRef, ActorRef]
  val queryActors = new HashMap[DatasetRef, ActorRef]
  val shardMaps = new HashMap[DatasetRef, ShardMapper]

  logger.debug(s"Initializing stream configs: ${settings.streamConfigs}")
  settings.streamConfigs.foreach { config =>
    val dataset = settings.datasetFromStream(config)
    val ingestion = IngestionConfig(config, NodeClusterActor.noOpSource.streamFactoryClass).get
    initializeDataset(dataset, ingestion)
  }

  // By default, stop children IngestionActors when something goes wrong.
  // restart query actors though
  override val supervisorStrategy = OneForOneStrategy() {
    case exception: Exception =>
      val stackTrace = exception.getStackTrace
      if (stackTrace(0).getClassName equals QueryActor.getClass.getName)
        Restart
      else
        Stop
  }

  private def withIngester(originator: ActorRef, dataset: DatasetRef)
                          (func: ActorRef => Unit): Unit = {
    ingesters.get(dataset).map(func).getOrElse(originator ! UnknownDataset)
  }

  // For now, datasets need to be set up for ingestion before they can be queried (in-mem only)
  // TODO: if we ever support query API against cold (not in memory) datasets, change this
  private def withQueryActor(originator: ActorRef, dataset: DatasetRef)(func: ActorRef => Unit): Unit =
    queryActors.get(dataset).map(func).getOrElse(originator ! UnknownDataset)

  private def initializeDataset(dataset: Dataset, ingestConfig: IngestionConfig): Unit = {
    logger.info(s"Initializing dataset ${dataset.ref}")

    shardMaps.put(dataset.ref, new ShardMapper(ingestConfig.numShards))
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
    val mapper = shardMaps(dataset.ref)
    val spc = DatasetResourceSpec(ic.numShards, ic.minNumNodes)
    val shardsToStart = shardAssignmentStrategy.shardAssignments(self, dataset.ref, spc, mapper)
    shardsToStart.foreach(sh => updateFromShardEvent(ShardAssignmentStarted(dataset.ref, sh, self)))
    ingesters(dataset.ref) ! ShardIngestionState(0, dataset.ref, mapper)
  }

  private def updateFromShardEvent(event: ShardEvent): Unit = {
    shardMaps.get(event.ref).foreach { mapper =>
      mapper.updateFromEvent(event) match {
        case Failure(l) =>
          logger.error(s"updateFromShardEvent error for dataset=${event.ref} event $event. Mapper now: $mapper", l)
        case Success(r) =>
          logger.debug(s"updateFromShardEvent success for dataset=${event.ref} event $event. Mapper now: $mapper")
      }
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
    import ActorName.{Ingestion, Query}

    logger.debug(s"Recreated dataset $dataset from string")
    val ref = dataset.ref

    val schemas = if (schemaOverride) Schemas(dataset.schema) else settings.schemas
    if (schemaOverride) logger.info(s"Overriding schemas from settings: this better be a test!")
    val props = IngestionActor.props(dataset.ref, schemas, memStore,
                                     source, downsample, storeConf, self)
    val ingester = context.actorOf(props, s"$Ingestion-${dataset.name}")
    context.watch(ingester)
    ingesters(ref) = ingester

    val ttl = if (memStore.isDownsampleStore) downsample.ttls.last.toMillis
              else storeConf.diskTTLSeconds.toLong * 1000
    def earliestTimestampFn: Long = System.currentTimeMillis() - ttl
    logger.info(s"Creating QueryActor for dataset $ref with dataset ttlMs=$ttl")
    val queryRef = context.actorOf(QueryActor.props(memStore, dataset, schemas,
      shardMaps(ref), earliestTimestampFn), s"$Query-$ref")
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

  def shardEventHandlers: Receive = LoggingReceive {
    case e: ShardEvent => updateFromShardEvent(e)
  }

  def receive: Receive = queryHandlers orElse shardEventHandlers


}

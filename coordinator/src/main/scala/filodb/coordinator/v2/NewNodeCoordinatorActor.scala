package filodb.coordinator.v2

import scala.collection.mutable
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
case object LocalShardsHealthRequest
case class DatasetShardHealth(dataset: DatasetRef, shard: Int, status: ShardStatus)

object NewNodeCoordinatorActor {

  final case object InitNewNodeCoordinatorActor

  def props(memStore: TimeSeriesStore,
            clusterDiscovery: FiloDbClusterDiscovery,
            settings: FilodbSettings): Props =
    Props(new NewNodeCoordinatorActor(memStore, clusterDiscovery, settings))


  /**
   * Converts a ShardMapper.statuses to a bitmap representation where the bit is set to:
   *  - 1, if ShardStatus == ShardStatusActive
   *  - 0, any other ShardStatus like ShardStatusAssigned, ShardStatusRecovery etc.
   *  WHY this is the case ? This is because, the QueryActor is can only execute the query on the active shards.
   *
   * NOTE: bitmap is byte aligned. So extra bits are padded with 0.
   *
   * EXAMPLE - Following are some example of shards with statuses and their bit representation as below:
   *                   Status                          |  BitMap Representation      |  Hex Representation
   *  ---------------------------------------------------------------------------------------------------------------
   *  Assigned, Active, Recovery, Error                |      0100 0000              |      0x40
   *  Active, Active, Active, Active                   |      1111 0000              |      0xF0
   *  Error, Active, Active, Error, Active, Active     |      0110 1100              |      0x6C
   *
   * @param shardMapper ShardMapper object which stores the bitmap representation
   * @return A byte array where each byte represents 8 shards and the bit is set to 1 if the shard is active. Extra bits
   *         are padded with 0.
   */
  def shardMapperBitMapRepresentation(shardMapper: ShardMapper) : Array[Byte] = {
    val byteArray = new Array[Byte]((shardMapper.statuses.length + 7) / 8)
    for (i <- shardMapper.statuses.indices) {
      if (shardMapper.statuses(i) == ShardStatusActive) {
        byteArray(i / 8) = (byteArray(i / 8) | (1 << (7 - (i % 8)))).toByte
      }
    }
    byteArray
  }
}

private[filodb] final class NewNodeCoordinatorActor(memStore: TimeSeriesStore,
                                                    clusterDiscovery: FiloDbClusterDiscovery,
                                                    settings: FilodbSettings) extends BaseActor {

  import NodeClusterActor._
  import client.IngestionCommands._

  private val ingestionActors = new mutable.HashMap[DatasetRef, ActorRef]
  private val queryActors = new mutable.HashMap[DatasetRef, ActorRef]
  private val localShardMaps = new mutable.HashMap[DatasetRef, ShardMapper]
  private val ingestionConfigs = new mutable.HashMap[DatasetRef, IngestionConfig]()
  private val shardStats = new mutable.HashMap[DatasetRef, ShardHealthStats]()

  logger.info(s"[ClusterV2] Initializing NodeCoordActor at ${self.path}")

  private def initialize(): Unit = {
    logger.debug(s"[ClusterV2] Initializing stream configs: ${settings.streamConfigs}")
    settings.streamConfigs.foreach { config =>
      val dataset = settings.datasetFromStream(config)
      val ingestion = IngestionConfig(config, NodeClusterActor.noOpSource.streamFactoryClass).get
      initializeDataset(dataset, ingestion)
    }
    if (clusterDiscovery.ordinalOfLocalhost == 0) {
      startTenantIngestionMetering()
    }
  }

  override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case _: Exception => Resume
  }

  // For now, datasets need to be set up for ingestion before they can be queried (in-mem only)
  // TODO: if we ever support query API against cold (not in memory) datasets, change this
  private def withQueryActor(originator: ActorRef, dataset: DatasetRef)(func: ActorRef => Unit): Unit =
    queryActors.get(dataset).map(func).getOrElse(originator ! UnknownDataset)

  private def initializeDataset(dataset: Dataset, ingestConfig: IngestionConfig): Unit = {
    logger.info(s"[ClusterV2] Initializing dataset ${dataset.ref}")
    ingestionConfigs.put(dataset.ref, ingestConfig)
    localShardMaps.put(dataset.ref, new ShardMapper(ingestConfig.numShards))
    shardStats.put(dataset.ref, new ShardHealthStats(dataset.ref))
    clusterDiscovery.registerDatasetForDiscovery(dataset.ref, ingestConfig.numShards)
    // FIXME initialization of cass tables below for dev environments is async - need to wait before continuing
    // for now if table is not initialized in dev on first run, simply restart server :(
    memStore.store.initialize(dataset.ref, ingestConfig.numShards, ingestConfig.resources)
    // if downsampling is enabled, then initialize downsample datasets
    ingestConfig.downsampleConfig
                .downsampleDatasetRefs(dataset.ref.dataset)
                  .foreach { downsampleDataset =>
                    memStore.store.initialize(downsampleDataset, ingestConfig.numShards, ingestConfig.resources) }

    setupDataset( dataset,
                  ingestConfig.storeConfig, ingestConfig.numShards,
                  IngestionSource(ingestConfig.streamFactoryClass, ingestConfig.sourceConfig),
                  ingestConfig.downsampleConfig)
    initShards(dataset, ingestConfig)
  }

  private def initShards(dataset: Dataset, ic: IngestionConfig): Unit = {
    val mapper = localShardMaps(dataset.ref)
    val shardsToStart = clusterDiscovery.shardsForLocalhost(ic.numShards)
    shardsToStart.foreach(sh => updateFromShardEvent(ShardAssignmentStarted(dataset.ref, sh, self)))
    ingestionActors(dataset.ref) ! ShardIngestionState(0, dataset.ref, mapper)
  }

  private def updateFromShardEvent(event: ShardEvent): Unit = {
    localShardMaps.get(event.ref).foreach { mapper =>
      mapper.updateFromEvent(event) match {
        case Failure(l) =>
          logger.error(s"[ClusterV2] updateFromShardEvent error for dataset=${event.ref} " +
            s"event $event. Mapper now: $mapper", l)
        case Success(_) =>
          logger.debug(s"[ClusterV2] updateFromShardEvent success for dataset=${event.ref} " +
            s"event $event. Mapper now: $mapper")
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
                           numShards: Int,
                           source: IngestionSource,
                           downsample: DownsampleConfig,
                           schemaOverride: Boolean = false): Unit = {
    import ActorName.Ingestion

    logger.debug(s"Recreated dataset $dataset from string")
    val ref = dataset.ref

    val schemas = if (schemaOverride) Schemas(dataset.schema) else settings.schemas
    if (schemaOverride) logger.info(s"Overriding schemas from settings: this better be a test!")
    val props = IngestionActor.props(dataset.ref, schemas, memStore,
                                     source, downsample, storeConf, numShards, self)
    val ingester = context.actorOf(props, s"$Ingestion-${dataset.name}")
    context.watch(ingester)
    ingestionActors(ref) = ingester

    val ttl = if (memStore.isDownsampleStore) downsample.ttls.last.toMillis
              else storeConf.diskTTLSeconds * 1000
    def earliestTimestampFn = System.currentTimeMillis() - ttl
    def clusterShardMapperFn = clusterDiscovery.shardMapper(dataset.ref)
    logger.info(s"[ClusterV2] Creating QueryActor for dataset $ref with dataset ttlMs=$ttl")
    val queryRef = context.actorOf(QueryActor.props(memStore, dataset, schemas,
                                                    clusterShardMapperFn, earliestTimestampFn))
    queryActors(ref) = queryRef

    logger.info(s"[ClusterV2] Coordinator set up for ingestion and querying for $ref.")
  }

  private def startTenantIngestionMetering(): Unit = {
    if (settings.config.getBoolean("shard-key-level-ingestion-metrics-enabled")) {
      logger.info(s"[ClusterV2] Starting tenant level ingestion cardinality metering...")
      val inst = TenantIngestionMetering(
        settings,
        dsIterProducer = () => { localShardMaps.keysIterator },
        coordActorProducer = () => self)
      inst.schedulePeriodicPublishJob()
    }
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

  // scalastyle:off method.length
  def shardManagementHandlers: Receive = LoggingReceive {
    // sent by ingestion actors when shard status changes
    case ev: ShardEvent => try {
      updateFromShardEvent(ev)
    } catch { case e: Exception =>
      logger.error(s"[ClusterV2] Error occurred when processing message $ev", e)
    }

    // requested from CLI and HTTP API
    case g: GetShardMap =>
      try {
        sender() ! CurrentShardSnapshot(g.ref, clusterDiscovery.shardMapper(g.ref))
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message $g", e)
        // send a response to avoid blocking of akka caller for long time
        sender() ! InternalServiceError(s"Exception while executing GetShardMap for dataset: ${g.ref.dataset}")
      }
    /*
    * requested from HTTP API
    * What is the trade-off between GetShardMap vs GetShardMapV2 ?
    *
    * No | Ask Call        |   Size of Response (256 Shards)  |                Compute Used
    * -------------------------------------------------------------------------------------------------------------
    * 1  | GetShardMap     |      ~37KB                       | Baseline - Uses ShardMapper for shard update tracking
    * 2  | GetShardMapV2   |    172 Bytes with padding        | Additional CPU used to convert ShardMapper to BitMap
    *                                                         | Will save CPU at the caller by avoiding string parsing
    * */
    case g: GetShardMapV2 =>
      try {
        val shardBitMap = NewNodeCoordinatorActor.shardMapperBitMapRepresentation(clusterDiscovery.shardMapper(g.ref))
        val shardMapperV2 = ShardMapperV2(
          settings.minNumNodes.get,
          ingestionConfigs(g.ref).numShards,
          settings.k8sHostFormat.get,
          shardBitMap)
        sender() ! ShardSnapshot(shardMapperV2)
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message $g", e)
        // send a response to avoid blocking of akka caller for long time
        sender() ! InternalServiceError(s"Exception while executing GetShardMapV2 for dataset: ${g.ref.dataset}")
      }

    // requested from peer NewNodeCoordActors upon them receiving GetShardMap call
    case g: GetShardMapScatter =>
      try {
        sender() ! CurrentShardSnapshot(g.ref, localShardMaps(g.ref))
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message $g", e)
      }

    case ListRegisteredDatasets =>
      try {
        sender() ! localShardMaps.keys.toSeq
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message ListRegisteredDatasets", e)
      }

    case LocalShardsHealthRequest =>
      try {
        val resp = localShardMaps.flatMap { case (ref, mapper) =>
          mapper.statuses.zipWithIndex.filter(_._1 != ShardStatusUnassigned).map { case (status, shard) =>
            DatasetShardHealth(ref, shard, status)
          }
        }.toSeq
        sender() ! resp
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message LocalShardsHealthRequest", e)
      }
  }
  // scalastyle:on method.length

  def initHandler: Receive = {
    case InitNewNodeCoordinatorActor => initialize()
  }

  def receive: Receive = queryHandlers orElse shardManagementHandlers orElse initHandler

}

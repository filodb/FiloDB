package filodb.coordinator.v2

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

import akka.actor.{ActorRef, OneForOneStrategy, Props}
import akka.actor.SupervisorStrategy.Resume
import akka.event.LoggingReceive
import akka.pattern.pipe
import kamon.Kamon
import net.ceedubs.ficus.Ficus._

import filodb.coordinator._
import filodb.coordinator.v2.NewNodeCoordinatorActor.InitNewNodeCoordinatorActor
import filodb.core._
import filodb.core.GlobalScheduler.globalImplicitScheduler
import filodb.core.downsample.{DownsampleConfig, DownsampledTimeSeriesStore}
import filodb.core.memstore.{TimeSeriesMemStore, TimeSeriesStore}
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}
import filodb.core.metadata._
import filodb.core.query.QueryContext
import filodb.core.store.{IngestionConfig, StoreConfig}
import filodb.query.QueryCommand

final case class GetShardMapScatter(ref: DatasetRef)
case object LocalShardsHealthRequest
case class DatasetShardHealth(dataset: DatasetRef, shard: Int, status: ShardStatus)

/**
 * Asks a single node for the cardinality records of the shards it owns. Sent by peer
 * NewNodeCoordActors while serving a GetClusterCardinalities call, and answered without
 * involving the QueryActor or the query scheduler.
 *
 * NOTE: intentionally NOT a QueryCommand - queryHandlers matches on QueryCommand and would
 * forward this to the QueryActor, which is exactly the path this bypasses.
 *
 * @param shardKeyPrefix shard key prefix to scan under, e.g. Seq(ws) or Seq(ws, ns)
 * @param depth hierarchical depth to group at: 1 = ws, 2 = ns, 3 = metric
 */
final case class GetCardinalityScatter(ref: DatasetRef, shardKeyPrefix: Seq[String], depth: Int)

/**
 * Cardinality records for the shards owned by one node. Records are per-shard; summing
 * across shards is the caller's job.
 *
 * @param shards the shards actually scanned, all of which were ShardStatusActive
 */
final case class LocalCardinalities(ref: DatasetRef, shards: Seq[Int], records: Seq[CardinalityRecord])

/**
 * Asks the local NewNodeCoordActor for cluster-wide cardinalities. It scatters
 * GetCardinalityScatter to every node and merges the per-shard records by prefix.
 */
final case class GetClusterCardinalities(ref: DatasetRef, shardKeyPrefix: Seq[String], depth: Int)

/**
 * Cluster-wide cardinalities, merged across all shards of all nodes.
 *
 * @param cardinalities merged records; the `shard` field is meaningless post-merge and is set to -1
 * @param missingShards shards NOT covered by this result. Non-empty means the counts are an
 *                      undercount - callers metering on these numbers should fail rather than
 *                      publish them.
 */
final case class ClusterCardinalities(ref: DatasetRef,
                                      cardinalities: Seq[CardinalityRecord],
                                      missingShards: Seq[Int])

object ClusterCardinalities {

  /**
   * Folds per-node, per-shard cardinality records into cluster-wide totals grouped by shard key
   * prefix. `None` entries are nodes that failed or timed out.
   *
   * Any shard that no answering node reports as scanned lands in `missingShards`. A failed node
   * reports nothing, so all of its shards fall out of the covered set automatically - which is
   * why this needs no notion of which node owns which shard.
   */
  def merge(ref: DatasetRef,
            numShards: Int,
            results: Seq[Option[LocalCardinalities]]): ClusterCardinalities = {
    val acc = new mutable.HashMap[Seq[String], CardinalityValue]()
    val covered = mutable.Set[Long]()
    results.flatten.foreach { lc =>
      covered ++= lc.shards.map(_.toLong)
      lc.records.foreach { rec =>
        acc.update(rec.prefix, acc.get(rec.prefix).map(sum(_, rec.value)).getOrElse(rec.value))
      }
    }
    val missing = (0 until numShards).filterNot(sh => covered.contains(sh.toLong))
    // `shard` is meaningless once summed across shards
    ClusterCardinalities(ref, acc.toSeq.map { case (p, v) => CardinalityRecord(-1, p, v) }, missing)
  }

  /**
   * Sums cardinality counts across shards. childrenCount and childrenQuota are deliberately NOT
   * summed - they describe the trie shape and the quota of a single shard, neither of which
   * aggregates meaningfully across shards, so the max is carried instead.
   */
  private def sum(a: CardinalityValue, b: CardinalityValue): CardinalityValue =
    CardinalityValue(
      tsCount = a.tsCount + b.tsCount,
      activeTsCount = a.activeTsCount + b.activeTsCount,
      billableTsCount = a.billableTsCount + b.billableTsCount,
      childrenCount = math.max(a.childrenCount, b.childrenCount),
      childrenQuota = math.max(a.childrenQuota, b.childrenQuota))
}

object NewNodeCoordinatorActor {

  final case object InitNewNodeCoordinatorActor

  def props(memStore: TimeSeriesStore,
            clusterDiscovery: FiloDbClusterDiscovery,
            settings: FilodbSettings): Props =
    Props(new NewNodeCoordinatorActor(memStore, clusterDiscovery, settings))
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

  // Per-node budget for the cardinality scatter, covering actor resolution plus the scan ask.
  // Kept well below the caller's query.ask-timeout so a slow or unreachable node surfaces as our
  // 503 naming the missing shards, rather than the caller's ask timing out first (a generic 500).
  private val cardinalityAskTimeout =
    settings.config.as[FiniteDuration]("metering-scatter-timeout")

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

    // additional ColumnStore initialization for downsample datasets
    memStore match {
      case tsMemStore: DownsampledTimeSeriesStore =>
        tsMemStore.rawColStore.initialize(dataset.ref, ingestConfig.numShards, ingestConfig.resources)
        ingestConfig.downsampleConfig
          .downsampleDatasetRefs(dataset.ref.dataset)
          .foreach { downsampleDataset =>
            memStore.store.initialize(downsampleDataset, ingestConfig.numShards, ingestConfig.resources) }
      case rawTSMemStore: TimeSeriesMemStore =>
        val downsampleDatasetRefs : Seq[DatasetRef] = ingestConfig.downsampleConfig.downsampleDatasetRefs(dataset.name)
        if (!downsampleDatasetRefs.isEmpty && rawTSMemStore.writeDownsampleIndex) {
          val highestResDownsampleDatasetRef = downsampleDatasetRefs.last
          rawTSMemStore.downsampleStore.initialize(
            highestResDownsampleDatasetRef, ingestConfig.numShards, ingestConfig.resources
          )
        }
      case _ =>
    }

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
        val hostFormat = settings.hostNameFormat.isDefined match {
          case true => settings.hostNameFormat.get
          case false => "127.0.0.1" // default host in local dev environments
        }
        val shardMapperV2 = ShardMapperV2.apply(
          settings.minNumNodes.get,
          ingestionConfigs(g.ref).numShards,
          hostFormat,
          clusterDiscovery.shardMapper(g.ref))
        // send the shardMapV2 response to the caller.
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
        val resp = localShardMaps.toSeq.flatMap { case (ref, mapper) =>
          mapper.statuses.zipWithIndex.filter(_._1 != ShardStatusUnassigned).map { case (status, shard) =>
            DatasetShardHealth(ref, shard, status)
          }.toSeq
        }
        sender() ! resp
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message LocalShardsHealthRequest", e)
      }
  }
  // scalastyle:on method.length

  def initHandler: Receive = {
    case InitNewNodeCoordinatorActor => initialize()
  }

  /**
   * GetCardinalityScatter is answered inline, exactly like the sibling handlers above and like
   * QueryActor.execTopkCardinalityQuery, which already runs this same scan on an actor thread.
   *
   * GetClusterCardinalities cannot be: it scatters to every node INCLUDING this one, so blocking
   * this mailbox while waiting would prevent us from ever processing our own
   * GetCardinalityScatter, guaranteeing a self-timeout. Hence the Future + pipe. Its callbacks are
   * trivial and run on the shared globalImplicitScheduler (imported above), which is why the
   * synchronous handlers here need no ExecutionContext at all.
   */
  def cardinalityHandlers: Receive = LoggingReceive {
    case g: GetCardinalityScatter =>
      try {
        sender() ! scanLocalCardinalities(g)
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message $g", e)
        // send a response to avoid blocking of akka caller for long time
        sender() ! InternalServiceError(s"Exception while executing GetCardinalityScatter for " +
          s"dataset: ${g.ref.dataset}")
      }

    case g: GetClusterCardinalities =>
      val replyTo = sender()
      try {
        val numShards = ingestionConfigs(g.ref).numShards
        val resp = clusterDiscovery
          .reduceCardinalitiesFromAllNodes(g.ref, g.shardKeyPrefix, g.depth, numShards, cardinalityAskTimeout)
          .recover { case e: Exception =>
            logger.error(s"[ClusterV2] Error occurred when processing message $g", e)
            InternalServiceError(s"Exception while executing GetClusterCardinalities for " +
              s"dataset: ${g.ref.dataset}")
          }
        pipe(resp).pipeTo(replyTo)
      } catch { case e: Exception =>
        logger.error(s"[ClusterV2] Error occurred when processing message $g", e)
        replyTo ! InternalServiceError(s"Exception while executing GetClusterCardinalities " +
          s"for dataset: ${g.ref.dataset}")
      }
  }

  /**
   * Scans this node's shards for cardinality records.
   *
   * The owned shard set comes from clusterDiscovery.shardsForLocalhost - the same source initShards
   * uses - rather than from localShardMaps. It is cheap (pure arithmetic over the ordinal) and, more
   * importantly, independent of whether the shard assignment events actually applied:
   * updateFromShardEvent only logs on failure, so a dropped event would make a shard look "not mine"
   * and be silently omitted from the count. Deriving the expected set independently means such a
   * shard instead shows up as not-active below, and we refuse rather than undercount.
   *
   * Every owned shard must be ShardStatusActive. A shard that is assigned but recovering has a
   * partially built cardinality tracker, so including it would silently undercount - we fail the
   * whole node's scan instead. NOTE: this is deliberately stricter than HealthRoute's
   * healthyShardStatuses, which admits ShardStatusRecovery and ShardStatusAssigned.
   */
  private def scanLocalCardinalities(g: GetCardinalityScatter): Any = {
    (localShardMaps.get(g.ref), ingestionConfigs.get(g.ref)) match {
      case (Some(mapper), Some(ingestConfig)) =>
        val owned = clusterDiscovery.shardsForLocalhost(ingestConfig.numShards)
        val inactive = owned.filterNot(mapper.isActiveShard)
        if (inactive.nonEmpty) {
          val statuses = inactive.map(sh => s"$sh=${mapper.statusForShard(sh)}").mkString(", ")
          logger.warn(s"[ClusterV2] Refusing cardinality scan for dataset=${g.ref.dataset}: " +
            s"shards not active [$statuses]")
          InternalServiceError(s"Cardinality count would be incorrect for dataset " +
            s"${g.ref.dataset}: shards not active [$statuses]")
        } else {
          // intersect with the memstore's view so we never ask it for a shard it has not instantiated
          val shards = owned.intersect(memStore.activeShards(g.ref))
          if (shards.isEmpty) {
            // IMPORTANT: scanTsCardinalities treats an empty shard list as "ALL local shards", so
            // it must never be called with the empty result of this intersection.
            logger.warn(s"[ClusterV2] No shards to scan for cardinality on this node. " +
              s"dataset=${g.ref.dataset} ownedShards=$owned memStoreShards=${memStore.activeShards(g.ref)}")
            LocalCardinalities(g.ref, Nil, Nil)
          } else {
            val startMs = System.currentTimeMillis()
            val records = memStore.scanTsCardinalities(
              QueryContext(), g.ref, shards, g.shardKeyPrefix, g.depth)
            val elapsedMs = System.currentTimeMillis() - startMs
            // This scan runs on the actor thread, so its duration is mailbox occupancy for this
            // node - log it so slow scans are attributable without a profiler.
            logger.info(s"[ClusterV2] Cardinality scan complete. dataset=${g.ref.dataset} " +
              s"prefix=${g.shardKeyPrefix} depth=${g.depth} numShards=${shards.size} " +
              s"numRecords=${records.size} elapsedMs=$elapsedMs")
            LocalCardinalities(g.ref, shards, records)
          }
        }
      case _ =>
        InternalServiceError(s"Dataset ${g.ref.dataset} is not registered on this node")
    }
  }

  def receive: Receive =
    queryHandlers orElse shardManagementHandlers orElse cardinalityHandlers orElse initHandler

}

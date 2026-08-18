package filodb.coordinator.v2

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import filodb.coordinator.{ActorName, CurrentShardSnapshot, FilodbSettings, ShardMapper}
import filodb.core.DatasetRef
import filodb.core.memstore.ratelimit.{CardinalityRecord, CardinalityValue}
import filodb.core.metrics.FilodbMetrics

class FiloDbClusterDiscovery(settings: FilodbSettings,
                             system: ActorSystem,
                             failureDetectionInterval: FiniteDuration)
                            (implicit ec: Scheduler) extends StrictLogging {

  private val discoveryJobs = mutable.Map[DatasetRef, CancelableFuture[Unit]]()


  // Metric to track actor resolve failures
  val actorResolvedFailedCounter = FilodbMetrics.counter("actor-resolve-failed")
  // Metric to track cluster discovery runs
  val clusterDiscoveryCounter = FilodbMetrics.counter("filodb-cluster-discovery")
  // Metric to track if we have unassigned shards on a given pod
  val unassignedShardsGauge = FilodbMetrics.gauge("v2-unassigned-shards")
  // Metric to track per-node failures while scattering a cardinality scan
  val cardinalityScatterFailedCounter = FilodbMetrics.counter("cardinality-scatter-failed")

  lazy val ordinalOfLocalhost: Int = {
    if (settings.localhostOrdinal.isDefined) settings.localhostOrdinal.get
    else {
      val fullHostname = InetAddress.getLocalHost.getHostName
      val hostName = if (fullHostname.contains(".")) fullHostname.substring(0, fullHostname.indexOf('.'))
      else fullHostname

      val pat = "-\\d+$".r
      // extract ordinal at the end from hostname such as filodb-host-0, filodb-host-10
      val ordinal = pat.findFirstIn(hostName).map(ordinal => -Integer.parseInt(ordinal))
        .getOrElse(throw new IllegalArgumentException(s"hostname $hostName does not appear like a stateful sets host"))
      ordinal
    }
  }

  def shardsForOrdinal(ordinal: Int, numShards: Int): Seq[Int] = {
    require(settings.minNumNodes.isDefined, "Minimum Number of Nodes config not provided")
    require(ordinal < settings.minNumNodes.get, s"Ordinal $ordinal was not expected. " +
      s"Number of nodes is ${settings.minNumNodes.get}")
    val numShardsPerHost = numShards / settings.minNumNodes.get
    // Suppose we have a total of 8 shards and 2 hosts, assuming the hostnames are host-0 and host-1, we will map
    // host-0 to shard [0,1,2,3] and host-1 to shard [4,5,6,7]
    val numExtraShardsToAssign = numShards % settings.minNumNodes.get
    val (firstShardThisNode, numShardsThisHost) = if (numExtraShardsToAssign != 0) {
      logger.warn("For stateful shard assignment, numShards should be a multiple of nodes per shard, " +
        "using default strategy")
      // Unequal shard assignment isn't recommended in Kubernetes stateful sets, all pods in Kubernetes are
      // identical and having unequal shard allocation requires provisioning all pods with max required spec
      // and leads to underutilized resources in some pods.

      // The strategy in this case will first perform a floor division to ensure all pods get those number of
      // shards at the minimum. It will then take the extra number of shards and allocate 1 each to first n shards
      // For example, suppose the number of shards is 12 and number of nodes is 5, then all shards will be
      // assigned a minimum of 2 nodes (12 / 5, floor division). We are no left with two extra shards which will
      // be assigned to first two nodes. Thus the final number of shards allocated will be 3, 3, 2, 2, 2 for the
      // 5 nodes
      (ordinal * numShardsPerHost + ordinal.min(numExtraShardsToAssign),
        numShardsPerHost + (if (ordinal < numExtraShardsToAssign) 1 else 0))
    } else
      (ordinal * numShardsPerHost, numShardsPerHost)
    val shardsMapped = (firstShardThisNode until firstShardThisNode + numShardsThisHost).toList
    shardsMapped
  }

  def shardsForLocalhost(numShards: Int): Seq[Int] = shardsForOrdinal(ordinalOfLocalhost, numShards)

  lazy private val hostNames = {
    require(settings.minNumNodes.isDefined, "[ClusterV2] Minimum Number of Nodes config not provided")
    if (settings.hostNameFormat.isDefined) {
      // This is used in kubernetes setup. We read the host format config and resolve the FQDN using env variables
      val hosts = (0 until settings.minNumNodes.get)
        .map(i => String.format(settings.hostNameFormat.get, i.toString))
      logger.info(s"[ClusterV2] hosts to communicate: " + hosts)
      hosts.sorted
    } else if (settings.hostList.isDefined) {
      // All the required hosts are provided manually in the config. usually used for local runs/setup
      settings.hostList.get.sorted // sort to make order consistent on all nodes of cluster
    } else throw new IllegalArgumentException("[ClusterV2] Cluster Discovery mechanism not defined")
  }

  lazy private val nodeCoordActorSelections = {
    hostNames.map { h =>
      system.actorSelection(ActorName.nodeCoordinatorPathClusterV2(h))
    }
  }

  private def askShardSnapshot(nca: ActorRef,
                               ref: DatasetRef,
                               numShards: Int,
                               timeout: FiniteDuration): Observable[CurrentShardSnapshot] = {
    val t = Timeout(timeout)
    val empty = CurrentShardSnapshot(ref, new ShardMapper(numShards))
    def fut = (nca ? GetShardMapScatter(ref)) (t).asInstanceOf[Future[CurrentShardSnapshot]]
    Observable.fromFuture(fut).onErrorHandle { e =>
      logger.error(s"[ClusterV2] Saw exception on askShardSnapshot!", e)
      empty
    }
  }

  def mergeSnapshotResponses(ref: DatasetRef,
                             numShards: Int,
                             snapshots: Observable[CurrentShardSnapshot]): Observable[ShardMapper] = {
    val acc = new ShardMapper(numShards)
    snapshots.map(_.map).foldLeft(acc)(_.mergeFrom(_, ref))
  }

  private def reduceMappersFromAllNodes(ref: DatasetRef,
                                        numShards: Int,
                                        timeout: FiniteDuration): Observable[ShardMapper] = {
    val snapshots = for {
      nca <- Observable.fromIteratorUnsafe(nodeCoordActorSelections.iterator)
      ncaRef <- Observable.fromFuture(nca.resolveOne(settings.ResolveActorTimeout)
        .recover {
          // recovering with ActorRef.noSender
          case e =>
            // log the exception we got while trying to resolve and emit metric
            logger.error(s"[ClusterV2] Actor Resolve Failed ! actor: ${nca.toString()}", e)
            actorResolvedFailedCounter.increment(1, Map("dataset" -> ref.dataset, "actor" -> nca.toString()))
            ActorRef.noSender
        })
      if ncaRef != ActorRef.noSender
      snapshot <- askShardSnapshot(ncaRef, ref, numShards, timeout)
    } yield {
      snapshot
    }
    mergeSnapshotResponses(ref, numShards, snapshots)
  }

  private val datasetToMapper = new ConcurrentHashMap[DatasetRef, ShardMapper]()
  def registerDatasetForDiscovery(dataset: DatasetRef, numShards: Int): Unit = {
    require(failureDetectionInterval.toMillis > 5000, "failure detection interval should be > 5s")
    logger.info(s"[ClusterV2] Starting discovery pipeline for $dataset")
    datasetToMapper.put(dataset, new ShardMapper(numShards))
    val fut = for {
      _ <- Observable.intervalWithFixedDelay(failureDetectionInterval)
      mapper <- reduceMappersFromAllNodes(dataset, numShards, failureDetectionInterval - 5.seconds)
    } {
      datasetToMapper.put(dataset, mapper)
      clusterDiscoveryCounter.increment(1, Map("dataset" -> dataset.dataset))
      val unassignedShardsCount = mapper.unassignedShards.length.toDouble
      if (unassignedShardsCount > 0.0) {
        logger.error(s"[ClusterV2] Unassigned Shards > 0 !! Dataset: ${dataset.dataset} " +
          s"Shards Mapping is: ${mapper.prettyPrint} ")
      }
      unassignedShardsGauge.update(unassignedShardsCount, Map("dataset" -> dataset.dataset))
    }
    discoveryJobs += (dataset -> fut)
  }

  def shardMapper(ref: DatasetRef): ShardMapper = {
    val mapper = datasetToMapper.get(ref)
    if (mapper == null) new ShardMapper(0) else mapper
  }

  /**
   * Scatters a cardinality scan to every node of the cluster and merges the per-shard records
   * into cluster-wide totals, grouped by shard key prefix.
   *
   * Unlike askShardSnapshot, a node that fails or times out is NOT substituted with an empty
   * result. Every answering node reports the shards it actually scanned, and any shard nobody
   * reports is returned in `missingShards`. These counts are used for metering, so silently
   * omitting a node would produce an undercount that looks like real data.
   *
   * NOTE: missing shards are derived from what nodes report, NOT from shardsForOrdinal - the
   * hostNames list is sorted lexicographically, so its index is not the node ordinal
   * ("host-10" sorts before "host-2"). A failed node reports nothing, so all of its shards
   * already show up as uncovered.
   *
   * Nodes are queried in parallel - each does a RocksDB scan, so a sequential fan-out would make
   * latency the sum over pods rather than the max.
   */
  def reduceCardinalitiesFromAllNodes(ref: DatasetRef,
                                      shardKeyPrefix: Seq[String],
                                      depth: Int,
                                      numShards: Int,
                                      timeout: FiniteDuration): Future[ClusterCardinalities] = {
    val t = Timeout(timeout)
    val asks = nodeCoordActorSelections.zip(hostNames).map { case (nca, host) =>
      nca.resolveOne(settings.ResolveActorTimeout)
        .flatMap { ncaRef => (ncaRef ? GetCardinalityScatter(ref, shardKeyPrefix, depth))(t) }
        .map {
          case lc: LocalCardinalities => Some(lc)
          case other =>
            logger.error(s"[ClusterV2] Cardinality scatter to host=$host for dataset=$ref " +
              s"returned $other")
            None
        }
        .recover { case e =>
          logger.error(s"[ClusterV2] Cardinality scatter to host=$host failed for dataset=$ref", e)
          cardinalityScatterFailedCounter.increment(1, Map("dataset" -> ref.dataset))
          None
        }
    }

    Future.sequence(asks).map { results =>
      val merged = ClusterCardinalities.merge(ref, numShards, results)
      if (merged.missingShards.nonEmpty) {
        logger.error(s"[ClusterV2] Cardinality result for dataset=$ref is incomplete. " +
          s"nodesAnswered=${results.count(_.isDefined)}/${results.size} " +
          s"missingShards=${merged.missingShards}")
      }
      merged
    }
  }

  def shutdown(): Unit = {
    discoveryJobs.values.foreach(_.cancel())
  }

}


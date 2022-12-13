package filodb.coordinator.v2

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import monix.execution.{CancelableFuture, Scheduler}
import monix.reactive.Observable

import filodb.coordinator.{CurrentShardSnapshot, FilodbSettings, ShardMapper}
import filodb.core.DatasetRef

class FiloDbClusterDiscovery(settings: FilodbSettings,
                             system: ActorSystem,
                             failureDetectionInterval: FiniteDuration)
                            (implicit ec: Scheduler) extends StrictLogging {

  private val discoveryJobs = mutable.Map[DatasetRef, CancelableFuture[Unit]]()

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
    require(ordinal < settings.numNodes, s"Ordinal $ordinal was not expected. Number of nodes is ${settings.numNodes}")
    val numShardsPerHost = numShards / settings.numNodes
    // Suppose we have a total of 8 shards and 2 hosts, assuming the hostnames are host-0 and host-1, we will map
    // host-0 to shard [0,1,2,3] and host-1 to shard [4,5,6,7]
    val numExtraShardsToAssign = numShards % settings.numNodes
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
    if (settings.k8sHostFormat.isDefined) {
      (0 until settings.numNodes).map(i => String.format(settings.k8sHostFormat.get, i.toString))
    } else if (settings.hostList.isDefined) {
      settings.hostList.get.sorted // sort to make order consistent on all nodes of cluster
    } else throw new IllegalArgumentException("Cluster Discovery mechanism not defined")
  }

  lazy private val nodeCoordActorSelections = {
    hostNames.map { h =>
      val actorPath = s"akka.tcp://filo-standalone@$h/user/coordinator"
      system.actorSelection(actorPath)
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
      logger.error(s"Saw exception on askShardSnapshot: $e")
      empty
    }
  }

  private def reduceMappersFromAllNodes(ref: DatasetRef,
                                        numShards: Int,
                                        timeout: FiniteDuration): Observable[ShardMapper] = {
    val acc = new ShardMapper(numShards)
    val snapshots = for {
      nca <- Observable.fromIteratorUnsafe(nodeCoordActorSelections.iterator)
      ncaRef <- Observable.fromFuture(nca.resolveOne(settings.ResolveActorTimeout).recover{case _=> ActorRef.noSender})
      if ncaRef != ActorRef.noSender
      snapshot <- askShardSnapshot(ncaRef, ref, numShards, timeout)
    } yield {
      snapshot
    }
    snapshots.map(_.map).foldLeft(acc)(_.mergeFrom(_, ref))
  }

  private val datasetToMapper = new ConcurrentHashMap[DatasetRef, ShardMapper]()
  def registerDatasetForDiscovery(dataset: DatasetRef, numShards: Int): Unit = {
    require(failureDetectionInterval.toMillis > 5000, "failure detection interval should be > 5s")
    logger.info(s"Starting discovery pipeline for $dataset")
    datasetToMapper.put(dataset, new ShardMapper(numShards))
    val fut = for {
      _ <- Observable.intervalWithFixedDelay(failureDetectionInterval)
      mapper <- reduceMappersFromAllNodes(dataset, numShards, failureDetectionInterval - 5.seconds)
    } {
      datasetToMapper.put(dataset, mapper)
    }
    discoveryJobs += (dataset -> fut)
  }

  def shardMapper(ref: DatasetRef): ShardMapper = {
    val mapper = datasetToMapper.get(ref)
    if (mapper == null) new ShardMapper(0) else mapper
  }

  def shutdown(): Unit = {
    discoveryJobs.values.foreach(_.cancel())
  }

}


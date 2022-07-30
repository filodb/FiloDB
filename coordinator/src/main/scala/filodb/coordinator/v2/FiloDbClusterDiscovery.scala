package filodb.coordinator.v2

import java.net.InetAddress
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration.DurationInt

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.coordinator.FilodbSettings

class FiloDbClusterDiscovery(settings: FilodbSettings, system: ActorSystem)
                            (implicit ec: Scheduler) extends StrictLogging {

  val numNodes = settings.config.getInt("cluster-discovery.num-nodes")
  val k8sHostFormat = settings.config.as[Option[String]]("cluster-discovery.k8s-stateful-sets-hostname-format")
  val hostList = settings.config.as[Option[Seq[String]]]("cluster-discovery.host-list")
  val localhostOrdinal = settings.config.as[Option[Int]]("cluster-discovery.localhost-ordinal")

  val ordinalOfLocalhost = {
    if (localhostOrdinal.isDefined) localhostOrdinal.get
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
    val numShardsPerHost = numShards / numNodes
    // Suppose we have a total of 8 shards and 2 hosts, assuming the hostnames are host-0 and host-1, we will map
    // host-0 to shard [0,1,2,3] and host-1 to shard [4,5,6,7]
    val numExtraShardsToAssign = numShards % numNodes
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

  private val hostNames = {
    if (k8sHostFormat.isDefined) {
      (0 until numNodes).map(i => String.format(k8sHostFormat.get, i.toString))
    } else if (hostList.isDefined) {
      hostList.get.sorted // sort to make order consistent on all nodes of cluster
    } else throw new IllegalArgumentException("Cluster Discovery mechanism not defined")
  }

  private val nodeCoordActorSelections = {
    (0 until numNodes).zip(hostNames).map { case (ord, h) =>
      val actorPath = if (ordinalOfLocalhost == ord) s"akka://FiloDB/user/NodeCoordinatorActor"
      else s"akka.tcp://FiloDB@$h/user/NodeCoordinatorActor"
      ord -> system.actorSelection(actorPath)
    }
  }

  private val map = new ConcurrentHashMap[Int, ActorRef]()
  private val failureDetectionInterval = 10.seconds
  Observable.intervalWithFixedDelay(failureDetectionInterval)
    .flatMap { _ =>
      logger.debug(s"Resolving peers...")
      Observable.fromIteratorUnsafe(nodeCoordActorSelections.iterator)
    }
    .mapEval { case (ord, actSel) =>
      val fut = actSel.resolveOne(settings.ResolveActorTimeout)
        .recover { case ex =>
          logger.warn(s"Could not resolve ${actSel.pathString} ; marking as down: ${ex.getMessage}")
          ActorRef.noSender
        }
        .map { actRef =>
          if (actRef == ActorRef.noSender) map.remove(ord)
          else map.put(ord, actRef)
        }
      Task.fromFuture(fut)
    }
    .onErrorHandle(e => logger.error("Error in NCA discovery pipeline", e))
    .onErrorRestart(Long.MaxValue)
    .completedL.runToFuture

  def ordinalToNodeCoordActors(): Map[Int, ActorRef] = {
    import scala.collection.JavaConverters._
    map.asScala.toMap
  }

}


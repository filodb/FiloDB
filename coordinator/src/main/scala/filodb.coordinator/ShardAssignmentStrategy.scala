package filodb.coordinator

import java.net.InetAddress

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.NodeClusterActor.DatasetResourceSpec
import filodb.core.DatasetRef

trait ShardAssignmentStrategy {

  /**
    * Returns assignment recommendations for a single coord and dataset combination.
    *
    * It should ideally be stateless and idempotent, such that methods can be called from any state, and repeating
    * calls with identical parameters should yield identical results.
    *
    * TODO: Pass more information for this class to be able to make assignment decisions based on
    * node capacity, shard capacity etc.
    *
    */
  def shardAssignments(coord: ActorRef,
                       dataset: DatasetRef,
                       resources: DatasetResourceSpec,
                       mapper: ShardMapper): Seq[Int]

  /**
    * Method to find the remaining capacity left to take new shards for a co-ordinator
    */
  def remainingCapacity(coord: ActorRef,
                        dataset: DatasetRef,
                        resources: DatasetResourceSpec,
                        mapper: ShardMapper): Int

}

/**
 * Uses the trailing number in the host name to determine the shards that can be mapped to the host
 * the implementation falls back to DefaultShardAssignmentStrategy if assignment cannot be determined
 *
 * The implementation assumes the number of shards is a multiple of number of nodes and there are no
 * uneven assignments of shards to nodes
 */
class K8sStatefulSetShardAssignmentStrategy extends ShardAssignmentStrategy with StrictLogging {

  private val pat = "-\\d+$".r

  private[coordinator] def getOrdinalFromActorRef(coord: ActorRef): Option[(String, Int)] = {
    // if hostname is None from coordinator actor path, then its a local actor
    // If the host name does not contain an ordinal at the end (e.g filodb-host-0, filodb-host-10), it will match None
    coord.path.address.host
      .map(host => InetAddress.getByName(host).getHostName)
      .orElse(Some(InetAddress.getLocalHost.getHostName))
      .map(name => if (name.contains(".")) name.substring(0, name.indexOf('.')) else name)
      .flatMap(hostName => pat.findFirstIn(hostName).map(ordinal => (hostName, -Integer.parseInt(ordinal))))
  }

  override def shardAssignments(coord: ActorRef,
                                dataset: DatasetRef,
                                resources: DatasetResourceSpec,
                                mapper: ShardMapper): Seq[Int] =
     getOrdinalFromActorRef(coord) match {
        case Some((hostName, ordinal)) =>
          val numShardsPerHost = resources.numShards / resources.minNumNodes
          // Suppose we have a total of 8 shards and 2 hosts, assuming the hostnames are host-0 and host-1, we will map
          // host-0 to shard [0,1,2,3] and host-1 to shard [4,5,6,7]
          val numExtraShardsToAssign = resources.numShards % resources.minNumNodes
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
          logger.info("Using hostname resolution for shard mapping, mapping host={} to shards={}",
            hostName, shardsMapped)
          shardsMapped
        case None                      =>
          // Host name does not have the ordinal at the end like a stateful set needs to have, delegate to default
          //strategy
          logger.info(
            "Falling back to DefaultShardAssignmentStrategy as hostname does not match k8s StatefulSet convention" +
              " for coordinator {}", coord.path.address.host.getOrElse(InetAddress.getLocalHost.getHostName))
          DefaultShardAssignmentStrategy.shardAssignments(coord, dataset, resources, mapper)
      }


  override def remainingCapacity(coord: ActorRef,
                                 dataset: DatasetRef,
                                 resources: DatasetResourceSpec,
                                 mapper: ShardMapper): Int =
    getOrdinalFromActorRef(coord) match {
        // Host name has the ordinal at the end, we can thus use the logic we use for stateful sets
        // Difference between fixed number of shards the coordinator can take and those currently assigned
        case Some((_, ordinal))  =>
          val numShardsPerHost = resources.numShards / resources.minNumNodes
          val numExtraShardsToAssign = resources.numShards % resources.minNumNodes
          val numShardsThisNode = if (numExtraShardsToAssign != 0) {
            logger.warn("For stateful shard assignment, numShards should be a multiple of nodes per shard, " +
              "using default strategy")
            numShardsPerHost + (if (ordinal < numExtraShardsToAssign) 1 else 0)
          } else
            numShardsPerHost
          (numShardsThisNode - mapper.shardsForCoord(coord).size).max(0)
        // Flag to resolve shards using hostname set but hostname does not follow stateful set hostname pattern
        case None     => DefaultShardAssignmentStrategy.remainingCapacity(coord, dataset, resources, mapper)
      }
}

object DefaultShardAssignmentStrategy extends ShardAssignmentStrategy with StrictLogging {

  def shardAssignments(coord: ActorRef,
                       dataset: DatasetRef,
                       resources: DatasetResourceSpec,
                       mapper: ShardMapper): Seq[Int] = {

    val numNewShards = remainingCapacity(coord, dataset, resources, mapper)
    val unassignedShards = mapper.unassignedShards

    if (unassignedShards.nonEmpty && numNewShards > 0) {
      unassignedShards.take(numNewShards)
    } else {
      Seq.empty
    }
  }

  /**
    * We want to assign shards evenly to coords as they come up. Simply using
    * ceil or floor of shardToNode ratio wont spread it evenly. Instead, at any time
    * we divide unassigned shards by unassigned coords to figure out number of shards to
    * assign to a new coord. In case unassignedShards is 16 and numUnassignedCoords is 5,
    * first coord will get 4 shards and rest all coords will get 3 shards respectively.
    *
    * @return - the number of new shards that can be assigned to the given coordinator
    */
  def remainingCapacity(coord: ActorRef,
                        dataset: DatasetRef,
                        resources: DatasetResourceSpec,
                        mapper: ShardMapper): Int = {
    val unassignedShards = mapper.unassignedShards
    val numUnassignedShards = unassignedShards.size

    if (unassignedShards.nonEmpty) {
      val numAssignedCoords = mapper.numAssignedCoords
      val numUnassignedCoords = resources.minNumNodes - numAssignedCoords
      // This coord may already have some shards assigned. We need to check if there is room for more. This can
      // happen if uneven number of shards were assigned initially, and node with greater number of shards fails
      val numAlreadyAssignedToCoord = mapper.shardsForCoord(coord).size

      // To figure out how many shards should be assigned to this coord,
      // 1. shards = Find sum of unassigned shards and shards currently assigned to curent coord
      val shards = numUnassignedShards + numAlreadyAssignedToCoord
      // 2. nodes = Find number of nodes without shards assigned, and include current coord in the count
      val nodes = numUnassignedCoords + (if (numAlreadyAssignedToCoord > 0) 1 else 0)
      // 3. numAssignableToCoord = shards / nodes
      val numAssignableToCoord = (shards.toDouble / nodes).ceil.toInt
      // DivideByZero check: nodes should be >= 1 because current coord is always included in count

      // Max shards allowed on a single node
      val maxCapacity = (resources.numShards.toDouble / resources.minNumNodes).ceil.toInt
      val assignableToCoord = if (numAssignableToCoord > maxCapacity) maxCapacity else numAssignableToCoord

      val remainingCapacity = Math.max(0, assignableToCoord - numAlreadyAssignedToCoord)

      logger.debug(s"""
                      |shardAssignments:
                      |   coord=$coord
                      |   dataset=$dataset
                      |   resources=$resources
                      |   unassignedShards=$unassignedShards
                      |   numUnassignedShards=$numUnassignedShards
                      |   numAlreadyAssignedToCoord=$numAlreadyAssignedToCoord
                      |   numAssignedCoords=$numAssignedCoords
                      |   numUnassignedCoords=$numUnassignedCoords
                      |   shards=$shards
                      |   nodes=$nodes
                      |   numAssignableToCoord=$numAssignableToCoord
                      |   remainingCapacity=$remainingCapacity
         """.stripMargin)

      remainingCapacity
    } else {
      0
    }
  }

}

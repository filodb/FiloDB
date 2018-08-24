package filodb.coordinator

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

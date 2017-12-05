package filodb.coordinator

import scala.collection.{Map => CMap}

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.NodeClusterActor.DatasetResourceSpec
import filodb.core.DatasetRef

/**
 * A ShardAssignmentStrategy is responsible for assigning or removing shards to/from nodes based on some
 * policy, when state changes occur.
 * It should ideally be stateless and idempotent, such that methods can be called from any state, and repeating
 * calls with identical parameters should yield identical results.
 */
trait ShardAssignmentStrategy {
  import ShardAssignmentStrategy._

  def nodeAdded(coordRef: ActorRef,
                shardMaps: CMap[DatasetRef, ShardMapper],
                resources: CMap[DatasetRef, DatasetResources]): NodeAdded

  def nodeRemoved(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]): NodeRemoved

  def datasetAdded(dataset: DatasetRef,
                   coords: Set[ActorRef],
                   resources: DatasetResourceSpec,
                   shardMaps: CMap[DatasetRef, ShardMapper]): DatasetAdded
}

/** Local node commands returned by the [[filodb.coordinator.ShardAssignmentStrategy]].
  * INTERNAL API
  */
private[coordinator] object ShardAssignmentStrategy {
  final case class DatasetResources(shardToNodeRatio: Double)

  final case class DatasetAdded(ref: DatasetRef,
                                resources: DatasetResources,
                                mapper: ShardMapper,
                                shards: Map[ActorRef, Seq[Int]])

  final case class NodeAdded(node: ActorRef, shards: Seq[DatasetShards]) {
    def datasets: Seq[DatasetRef] = shards.map(_.ref)
  }

  final case class NodeRemoved(shards: Seq[DatasetShards])

  final case class DatasetShards(ref: DatasetRef, mapper: ShardMapper, shards: Seq[Int])

  final case class AddShards(howMany: Int, coordinator: ActorRef, shards: Seq[Int], map: ShardMapper)

}

/**
 * The default strategy waits for a minimum of N nodes to be up to allocate resources to a dataset.
 * It is relatively static, ie if a node goes down, it waits for a node to be up again.
 */
class DefaultShardAssignmentStrategy extends ShardAssignmentStrategy with StrictLogging {
  import ShardAssignmentStrategy._

  // Counts how many shards there are for each coordinator across ALL datasets
  private def computeShardsPerCoord(maps: CMap[DatasetRef, ShardMapper]): Map[ActorRef, Int] =
    maps.toSeq.flatMap { case (_, map) => map.shardValues }
        .filter { case (coordRef, statuses) => coordRef != ActorRef.noSender }  // get rid of nulls
        .groupBy(_._1)    // group by coordinator
        .mapValues { statuses => statuses.length }

  /** On node added `akka.cluster.ClusterEvent.MemberUp`. If the `coordRef` has
    * previously been added, and not yet removed by `akka.cluster.ClusterEvent.MemberRemoved`
    * this returns an empty NodeAdded - immutable.
    * The shardMaps and resources passed in are not mutated.
    */
  def nodeAdded(coordinator: ActorRef,
                shardMaps: CMap[DatasetRef, ShardMapper],
                resources: CMap[DatasetRef, DatasetResources]): NodeAdded = {
    var dss: Seq[DatasetShards] = Seq.empty

    // If the coordinator already has shards, then assume nothing to do here
    val shardsPerCoord = computeShardsPerCoord(shardMaps)
    if (shardsPerCoord contains coordinator) {
      NodeAdded(coordinator, Nil)
    } else {
      // what are the shard maps with most unassigned shards?
      // hmm.  how many shards can be added to a node?  Need to know roughly how many nodes we expect
      // to process a given dataset.
      shardMaps.toSeq
        .map { case (ref, map) => (ref, map, map.numAssignedShards, resources(ref)) }
        .sortBy(_._3)
        .takeWhile { case (ref, map, assignedShards, resources) =>
          val add = addShards(map, ref, coordinator, resources.shardToNodeRatio)
          if (add.shards.nonEmpty) dss :+= DatasetShards(ref, map, add.shards)
          (assignedShards < map.numShards) && (add.howMany > 0)
        }

      NodeAdded(coordinator, dss)
    }
  }

  /** Called on node removed `akka.cluster.ClusterEvent.MemberRemoved`
    * or through DeathWatch and `akka.actor.Terminated`.
    */
  def nodeRemoved(coordinator: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]): NodeRemoved = {
    val updated = shardMaps.map { case (ref, map) =>
      val shards = map.removeNode(coordinator)

      // Any spare capacity to allocate removed shared?
      // try to spread removed shards amongst remaining nodes in order from
      // least loaded nodes on up
      // NOTE: zip returns a list which is the smaller of the two lists

      // NOTE: for now disable this.  We want more static allocation.  Reshuffling nodes can cause
      // too much IO and will cause pain to spread across the cluster.
      // lessLoadedNodes.zip(shardsRemoved).foreach { case ((coord, _), shard) =>
      //   map.registerNode(Seq(shard), coord) match {
      //     case Success(x) =>
      //       shardsPerCoord(coord) += 1
      //       logger.info(s"Reallocated shard $shard from $coordRef to $coord")
      //     case Failure(ex) =>
      //       logger.error(s"Unable to add shards: $ex")
      //   }
      // }
      DatasetShards(ref, map, shards)
    }.toSeq.filterNot(_.shards.isEmpty)

    NodeRemoved(updated)
  }

  /**
   * Called when a new dataset is registered.  Causes assignment of shards for this dataset to happen.
   * The shardMaps passed in are not mutated.
   */
  def datasetAdded(dataset: DatasetRef,
                   coords: Set[ActorRef],
                   resources: DatasetResourceSpec,
                   shardMaps: CMap[DatasetRef, ShardMapper]): DatasetAdded = {
    val shardsPerCoord = coords.map(_ -> 0).toMap ++ computeShardsPerCoord(shardMaps)

    val shardToNodeRatio = resources.numShards / resources.minNumNodes.toDouble
    logger.info(s"shardToNodeRatio for $dataset is $shardToNodeRatio")

    // Does shardMaps contain dataset yet?  If not, create one
    val updates = if (!shardMaps.contains(dataset)) { Seq((dataset, new ShardMapper(resources.numShards))) }
                  else                              { Nil }
    val map = if (updates.nonEmpty) updates.head._2 else shardMaps(dataset)

    // Assign shards to remaining nodes?  Assign to whichever nodes have fewer shards first
    val lessLoadedNodes = shardsPerCoord.toSeq.sortBy(_._2)
    logger.debug(s"Trying to add nodes to $dataset in this order: $lessLoadedNodes")

    val clonedMap = ShardMapper.copy(map, dataset)
    val _added = lessLoadedNodes.toIterator.map { case (nodeCoord, existingShards) =>
      val added = addShards(clonedMap, dataset, nodeCoord, shardToNodeRatio)
      clonedMap.registerNode(added.shards, nodeCoord)
      added
    }.takeWhile(_.howMany > 0)

    val added = _added.toSeq
    logger.info(s"Added ${added.map(_.howMany).sum} shards total")
    val shards = added.map(a => a.coordinator -> a.shards).toMap

    val resourceInfo = DatasetResources(shardToNodeRatio)
    DatasetAdded(dataset, resourceInfo, clonedMap, shards)
  }

  /**
   * Recommends shards to add given the current state of a ShardMapper.  ShardMapper is not modified.
   */
  private def addShards(map: ShardMapper,
                        dataset: DatasetRef,
                        coordinator: ActorRef,
                        shardToNodeRatio: Double): AddShards = {
    val addHowMany = (shardToNodeRatio * (map.allNodes.size + 1)).toInt - map.numAssignedShards
    logger.debug(s"Add shards [dataset=$dataset, addHowMany=$addHowMany, " +
                 s"unassignedShards=${map.unassignedShards}], numAssignedShards=${map.numAssignedShards}")

    if (addHowMany > 0) {
      val shardsToAdd = map.unassignedShards.take(addHowMany)
      logger.info(s"Assigning [shards=$shardsToAdd, dataset=$dataset, node=$coordinator.")

      AddShards(addHowMany, coordinator, shardsToAdd, map)
    } else {
      logger.warn(s"Unable to add shards for dataset $dataset to coord $coordinator.")
      AddShards(0, coordinator, Seq.empty, map)
    }
  }
}

package filodb.coordinator

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import scala.collection.{Map => CMap, mutable}
import scala.util.{Success, Failure}

import filodb.core.DatasetRef
import NodeClusterActor.DatasetResourceSpec

/**
 * A ShardAssignmentStrategy is responsible for assigning or removing shards to/from nodes based on some
 * policy, when state changes occur.
 */
trait ShardAssignmentStrategy {
  def nodeAdded(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]): Seq[(DatasetRef, ShardMapper)]

  def nodeRemoved(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]):
    Seq[(DatasetRef, ShardMapper)]

  def datasetAdded(dataset: DatasetRef,
                   resources: DatasetResourceSpec,
                   coords: Set[ActorRef],
                   shardMaps: CMap[DatasetRef, ShardMapper]): Seq[(DatasetRef, ShardMapper)]
}

/**
 * The default strategy waits for a minimum of N nodes to be up to allocate resources to a dataset.
 * It is relatively static, ie if a node goes down, it waits for a node to be up again.
 */
class DefaultShardAssignmentStrategy extends ShardAssignmentStrategy with StrictLogging {
  private val shardToNodeRatio = new mutable.HashMap[DatasetRef, Double]
  private val shardsPerCoord = new mutable.HashMap[ActorRef, Int].withDefaultValue(0)

  // TODO: rebalance existing shards if a new node adds capacity
  def nodeAdded(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]):
    Seq[(DatasetRef, ShardMapper)] = {
    shardsPerCoord(coordRef) = 0
    // what are the shard maps with most unassigned shards?
    // hmm.  how many shards can be added to a node?  Need to know roughly how many nodes we expect
    // to process a given dataset.
    val sortedMaps = shardMaps.toSeq.map { case (ref, map) => (ref, map, map.numAssignedShards) }
                              .sortBy(_._3)
    sortedMaps.takeWhile { case (ref, map, assignedShards) =>
      (assignedShards < map.numShards) && (addShards(map, ref, coordRef) > 0)
    }.map { case (ref, map, assignedShards) => (ref, map) }
  }

  def nodeRemoved(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]):
    Seq[(DatasetRef, ShardMapper)] = {
    val lessLoadedNodes = shardsPerCoord.toSeq.sortBy(_._2)
    val updatedMaps = shardMaps.toSeq.filter { case (ref, map) =>
      val shardsRemoved = map.removeNode(coordRef)

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

      shardsRemoved.nonEmpty
    }
    updatedMaps
  }

  def datasetAdded(dataset: DatasetRef,
                   resources: DatasetResourceSpec,
                   coords: Set[ActorRef],
                   shardMaps: CMap[DatasetRef, ShardMapper]): Seq[(DatasetRef, ShardMapper)] = {
    shardToNodeRatio(dataset) = resources.numShards / resources.minNumNodes.toDouble
    logger.info(s"shardToNodeRatio for $dataset is ${shardToNodeRatio(dataset)}")

    // Does shardMaps contain dataset yet?  If not, create one
    val updates = if (!shardMaps.contains(dataset)) { Seq((dataset, new ShardMapper(resources.numShards))) }
                  else                              { Nil }
    val map = if (updates.nonEmpty) updates.head._2 else shardMaps(dataset)

    // Assign shards to remaining nodes?  Assign to whichever nodes have fewer shards first
    val lessLoadedNodes = shardsPerCoord.toSeq.sortBy(_._2)
    logger.debug(s"Trying to add nodes to $dataset in this order: $lessLoadedNodes")

    val added = lessLoadedNodes.toIterator.map { case (nodeCoord, existingShards) =>
      addShards(map, dataset, nodeCoord)
    }.takeWhile(_ > 0).sum    // sum is needed to actually count something
    logger.info(s"Added $added shards total")

    updates
  }

  private def addShards(map: ShardMapper, dataset: DatasetRef, coordinator: ActorRef): Int = {
    val addHowMany = (shardToNodeRatio(dataset) * (map.allNodes.size + 1)).toInt - map.numAssignedShards
    logger.debug(s"numAssignedShards=${map.numAssignedShards}  addHowMany=$addHowMany")

    if (addHowMany > 0) {
      val shardsToAdd = map.unassignedShards.take(addHowMany)
      logger.info(s"Adding shards $shardsToAdd for dataset $dataset to coord $coordinator...")
      map.registerNode(shardsToAdd, coordinator) match {
        case Success(x) =>
          shardsPerCoord(coordinator) += shardsToAdd.length
        case Failure(ex) =>
          logger.error(s"Unable to add shards: $ex")
          return 0
      }
    }
    addHowMany
  }
}
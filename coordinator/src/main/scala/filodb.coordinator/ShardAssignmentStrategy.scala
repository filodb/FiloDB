package filodb.coordinator

import scala.collection.{mutable, Map => CMap}

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import NodeClusterActor.DatasetResourceSpec

/**
 * A ShardAssignmentStrategy is responsible for assigning or removing shards to/from nodes based on some
 * policy, when state changes occur.
 */
trait ShardAssignmentStrategy {
  import ShardAssignmentStrategy._

  def nodeAdded(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]): NodeAdded

  def nodeRemoved(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]): NodeRemoved

  def datasetAdded(dataset: DatasetRef,
                   resources: DatasetResourceSpec,
                   shardMaps: CMap[DatasetRef, ShardMapper]): DatasetAdded
}

/** Local node commands returned by the [[filodb.coordinator.ShardAssignmentStrategy]].
  * INTERNAL API
  */
private[coordinator] object ShardAssignmentStrategy {

  final case class DatasetAdded(ref: DatasetRef, mapper: ShardMapper, shards: Map[ActorRef, Seq[Int]])

  final case class NodeAdded(shards: Seq[DatasetShards]) {
    def datasets: Seq[DatasetRef] = shards.map(_.ref)
  }

  final case class NodeRemoved(shards: Map[DatasetRef, (Seq[Int], ShardMapper)])

  final case class DatasetShards(ref: DatasetRef, mapper: ShardMapper, shards: Seq[Int])

}

/**
 * The default strategy waits for a minimum of N nodes to be up to allocate resources to a dataset.
 * It is relatively static, ie if a node goes down, it waits for a node to be up again.
 */
class DefaultShardAssignmentStrategy extends ShardAssignmentStrategy with StrictLogging {
  import DefaultShardAssignmentStrategy._, ShardAssignmentStrategy._

  private val shardToNodeRatio = new mutable.HashMap[DatasetRef, Double]
  private val shardsPerCoord = new mutable.HashMap[ActorRef, Int].withDefaultValue(0)

  /** On node added [[akka.cluster.ClusterEvent.MemberUp]].
    * TODO: rebalance existing shards if a new node adds capacity
    */
  def nodeAdded(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]): NodeAdded = {
    shardsPerCoord(coordRef) = 0
    // what are the shard maps with most unassigned shards?
    // hmm.  how many shards can be added to a node?  Need to know roughly how many nodes we expect
    // to process a given dataset.
    val sortedMaps = shardMaps.toSeq.map { case (ref, map) => (ref, map, map.numAssignedShards) }
                              .sortBy(_._3)

    // can we do without takeWhile to avoid this?
    var dss: Seq[DatasetShards] = Seq.empty

    sortedMaps.takeWhile { case (ref, map, assignedShards) =>
      val add = addShards(map, ref, coordRef)
      dss :+= DatasetShards(ref, map, add.shards)
      (assignedShards < map.numShards) && (add.howMany > 0)
    }

    NodeAdded(dss)
  }

  /** Called on node removed [[akka.cluster.ClusterEvent.MemberRemoved]]
    * or through DeathWatch and [[akka.actor.Terminated]].
    */
  def nodeRemoved(coordRef: ActorRef, shardMaps: CMap[DatasetRef, ShardMapper]): NodeRemoved = {
    val updatedMaps = shardMaps.map { case (ref, map) =>
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

      ref -> Tuple2(shardsRemoved, map)
    }.toMap

    shardsPerCoord -= coordRef

    NodeRemoved(updatedMaps)
  }

  def datasetAdded(dataset: DatasetRef,
                   resources: DatasetResourceSpec,
                   shardMaps: CMap[DatasetRef, ShardMapper]): DatasetAdded = {

    shardToNodeRatio(dataset) = resources.numShards / resources.minNumNodes.toDouble
    logger.info(s"shardToNodeRatio for $dataset is ${shardToNodeRatio(dataset)}")

    // Does shardMaps contain dataset yet?  If not, create one
    val updates = if (!shardMaps.contains(dataset)) { Seq((dataset, new ShardMapper(resources.numShards))) }
                  else                              { Nil }
    val map = if (updates.nonEmpty) updates.head._2 else shardMaps(dataset)

    // Assign shards to remaining nodes?  Assign to whichever nodes have fewer shards first
    val lessLoadedNodes = shardsPerCoord.toSeq.sortBy(_._2)
    logger.debug(s"Trying to add nodes to $dataset in this order: $lessLoadedNodes")

    val _added = lessLoadedNodes.toIterator.map { case (nodeCoord, existingShards) =>
      addShards(map, dataset, nodeCoord)
    }.takeWhile(_.howMany > 0)

    val added = _added.toSeq
    logger.info(s"Added ${added.map(_.howMany).sum} shards total")
    val shards = added.map(a => a.coord -> a.shards).toMap

    DatasetAdded(dataset, map, shards)
  }

  private def addShards(map: ShardMapper, dataset: DatasetRef, coordinator: ActorRef): AddShards = {
    val addHowMany = (shardToNodeRatio(dataset) * (map.allNodes.size + 1)).toInt - map.numAssignedShards
    logger.debug(s"Add shards [dataset=$dataset, addHowMany=$addHowMany, unassignedShards=${map.unassignedShards}], numAssignedShards=${map.numAssignedShards}")

    if (addHowMany > 0) {
      val shardsToAdd = map.unassignedShards.take(addHowMany)
      logger.info(s"Assigning [shards=$shardsToAdd, dataset=$dataset, node=$coordinator.")
      shardsPerCoord(coordinator) += shardsToAdd.length
      AddShards(addHowMany, coordinator, shardsToAdd)

    } else {
      logger.warn(s"Unable to add shards for dataset $dataset to coord $coordinator.")
      AddShards(0, coordinator, Seq.empty)
    }
  }
}

private object DefaultShardAssignmentStrategy {
  /** INTERNAL API. */
  final case class AddShards(howMany: Int, coord: ActorRef, shards: Seq[Int])

}

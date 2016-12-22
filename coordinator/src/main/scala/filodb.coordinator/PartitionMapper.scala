package filodb.coordinator

import akka.actor.{ActorRef, Address}
import java.util.TreeMap
import net.jpountz.xxhash.XXHashFactory

case class PartitionNode(addr: Address, coordRef: ActorRef)

object PartitionMapper {
  type PartitionMap = TreeMap[Int, PartitionNode]

  val xxhashFactory = XXHashFactory.fastestInstance
  val hasher32 = xxhashFactory.hash32
  val Seed = 0x9747b28c

  val empty = new PartitionMapper(new PartitionMap)
}

/**
 * The PartitionMapper keeps track of how partition keys are mapped to different nodes in a FiloDB cluster.
 * Namely, it must
 * 1) very quickly, look up the ActorRef for the coordinator for sending a row to given a hash of a part key
 * 2) be able to return a new PartitionMapper when adding or removing a node
 *
 * The API is immutable, even if the state inside is mutable  :-p
 *
 * Consistent hashing is used based on the algorithm outlined in this article:
 * http://www.tom-e-white.com/2007/11/consistent-hashing.html
 *
 * TODO: make the lookup more performant by filling an array of virtual nodes so lookup is O(1) not O(lg n)
 */
case class PartitionMapper private[coordinator](map: PartitionMapper.PartitionMap) {
  import PartitionMapper._
  import collection.JavaConverters._

  def isEmpty: Boolean = map.isEmpty

  def numNodes: Int = map.size

  /**
   * Returns the coordRef for a given hash.  User is responsible for computing the hash of the partition key.
   * Currently this lookup is O(lg n) where n is the number of nodes * replicas
   * Since the API is immutable, this call is multi-thread safe.
   * @return the ActorRef of the coordinator of the matching node
   */
  def lookupCoordinator(hash: Int): ActorRef = {
    require(!map.isEmpty, "Cannot lookup coordinator when partition map is empty")
    Option(map.ceilingEntry(hash)) match {
      case Some(entry) => entry.getValue.coordRef
      case None        => map.firstEntry.getValue.coordRef
    }
  }

  def addNode(someNode: Address, coordRef: ActorRef, numReplicas: Int = 8): PartitionMapper = {
    val clonedMap = map.clone.asInstanceOf[PartitionMap]
    for { i <- 0 until numReplicas } {
      val nodeStrBytes = s"$someNode-$i".getBytes("UTF-8")
      val hashForReplica = hasher32.hash(nodeStrBytes, 0, nodeStrBytes.size, Seed)
      clonedMap.put(hashForReplica, PartitionNode(someNode, coordRef))
    }
    new PartitionMapper(clonedMap)
  }

  /**
   * Produces a new PartitionMapper without nodes containing adderess someNode.
   * If no nodes match the given address, then returns a cloned PartitionMapper.
   * Takes O(n) time.
   */
  def removeNode(someNode: Address): PartitionMapper = {
    val clonedMap = map.clone.asInstanceOf[PartitionMap]
    map.entrySet.iterator.asScala.foreach { entry =>
      if (entry.getValue.addr == someNode) clonedMap.remove(entry.getKey)
    }
    new PartitionMapper(clonedMap)
  }
}
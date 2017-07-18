package filodb.coordinator

import akka.actor.{Address, ActorRef}
import scala.collection.mutable.HashMap
import scala.util.{Try, Success, Failure}

object ShardMapper {
  val empty = new ShardMapper(0)

  final def toShard(n: Int, numShards: Int): Int = (((n & 0xffffffffL) * numShards) >> 32).toInt
}

final case class ShardAndNode(shard: Int, coord: ActorRef)

/**
 * Each FiloDB dataset is divided into a fixed number of shards for ingestion and distributed in-memory
 * querying. The ShardMapper keeps track of the mapping between shards and nodes for a single dataset.
 * - Given a partition hash, find the shard and node coordinator
 * - Given a shard key hash and # bits, find the shards and node coordinators to query
 * - Given a shard key hash and partition hash, # bits, compute the shard (for ingestion partitioning)
 * - Register a node to given shard numbers
 *
 * It is not multi thread safe for mutations (registrations) but reads should be fine.
 *
 * The shard finding given a hash needs to be VERY fast, it is in the hot query and ingestion path.
 */
class ShardMapper(val numShards: Int) extends Serializable {
  import ShardMapper._

  private final val shardMap = Array.fill(numShards)(ActorRef.noSender)

  override def equals(other: Any): Boolean = other match {
    case s: ShardMapper => s.numShards == numShards && s.shardValues == shardValues
    case o: Any         => false
  }

  override def hashCode: Int = shardValues.hashCode

  override def toString: String = s"ShardMapper {${shardValues.zipWithIndex}}"

  def shardValues: Seq[ActorRef] = shardMap.toBuffer

  /**
   * Maps a partition hash to a shard number and a NodeCoordinator ActorRef
   */
  def partitionToShardNode(partitionHash: Int): ShardAndNode = {
    val shard = toShard(partitionHash, numShards)
    ShardAndNode(shard, shardMap(shard))
  }

  def coordForShard(shardNum: Int): ActorRef = shardMap(shardNum)

  /**
   * Maps a shard key to a range of shards.  Used to limit shard distribution for queries when one knows
   * the shard key.
   * @param numShardBits the number of upper bits of the hash to use for the shard key
   * @return a list or range of shards
   */
  def shardKeyToShards(shardHash: Int, numShardBits: Int): Seq[Int] = {
    val partHashMask = (0xffffffffL >> numShardBits).toInt
    val shardKeyMask = ~partHashMask
    val startingShard = toShard(shardHash & shardKeyMask, numShards)
    val endingShard = toShard(shardHash & shardKeyMask | partHashMask, numShards)
    (startingShard to endingShard)
  }

  /**
   * Computes the shard number given a regular partition hash, a shard key hash, and number of bits.
   * This computes the shard and overall hash in such a way that when the number of bits needs to decrease
   * or the sharding needs to go to more shards, the original shards are preserved, thus avoiding
   * fragmentation and simplifying the querying logic.
   * @param shardHash the 32-bit hash of the shard key
   * @param partitionHash the 32-bit hash of the overall partition or time series key, containing all tags
   * @param numShardBits the number of upper bits of the hash to use for the shard key
   */
  def hashToShard(shardHash: Int, partitionHash: Int, numShardBits: Int): Int = {
    val partHashMask = (0xffffffffL >> numShardBits).toInt
    val shardKeyMask = ~partHashMask
    toShard((partitionHash & partHashMask) | (shardHash & shardKeyMask), numShards)
  }

  /**
   * Returns all shards that match a given address - typically used to compare to cluster.selfAddress
   * for that node's own shards
   */
  def shardsForAddress(addr: Address): Seq[Int] =
    shardMap.toSeq.zipWithIndex.collect {
      case (ref, shardNum) if ref != ActorRef.noSender && ref.path.address == addr => shardNum
    }

  /**
   * Returns all the shards that have not yet been assigned
   */
  def unassignedShards: Seq[Int] =
    shardMap.toSeq.zipWithIndex.collect { case (ActorRef.noSender, shard) => shard }

  def assignedShards: Seq[Int] =
    shardMap.toSeq.zipWithIndex.collect { case (ref, shard) if ref != ActorRef.noSender => shard }

  def numAssignedShards: Int = numShards - unassignedShards.length

  /**
   * Returns a set of unique NodeCoordinator ActorRefs for all assigned shards
   */
  def allNodes: Set[ActorRef] = shardMap.toSeq.filter(_ != ActorRef.noSender).toSet

  /**
   * Registers a new node to the given shards.  Modifies state in place.
   */
  def registerNode(shards: Seq[Int], coordinator: ActorRef): Try[Unit] = {
    shards.foreach { shard =>
      if (shardMap(shard) != ActorRef.noSender) {
        // Alreaady assigned!
        return Failure(new IllegalArgumentException(s"Shard $shard is already assigned!"))
      } else {
        shardMap(shard) = coordinator
      }
    }
    Success(())
  }

  /**
   * Removes a coordinator ref from all shards mapped to it.  Resets the shards to no owner and
   * returns the shards removed.
   */
  def removeNode(coordinator: ActorRef): Seq[Int] = {
    shardMap.toSeq.zipWithIndex.collect {
      case (ref, i) if ref == coordinator =>
        shardMap(i) = ActorRef.noSender
        i
    }
  }

  def clear(): Unit = {
    for { i <- 0 until numShards } { shardMap(i) = ActorRef.noSender }
  }
}
package filodb.coordinator

import akka.actor.ActorRef

import filodb.core._

/** The base marker. This can manifast as commands, events or state. */
sealed trait ShardAction extends Serializable

/** Sent once to newly-subscribed subscribers to initialize their local ShardMapper. */
final case class CurrentShardSnapshot(ref: DatasetRef,
                                      map: ShardMapper) extends ShardAction with Response

/**
 * Optimized form of the ShardMapper state representation.
 * NOTE: It doesn't track the shard status updates from coordinator or Ingestion actors. It is just
 * a wrapper which compresses the response of ShardMapper state to reduce network transmission costs.
 *
 * @param nodeCountInCluster Number of replicas in the filodb cluster
 * @param numShards Number of shards in the filodb cluster
 * @param hostNameFormat K8s host format. Valid ONLY for ClusterV2 shard assignment strategy
 * @param shardState ByteArray. Each bit of the byte represents the shard status.
 *                   For example: lets say we have 4 shards with following status:
 *                   Seq[ShardStatusAssigned, ShardStatusRecovery, ShardStatusAssigned, ShardStatusAssigned]
 *                   Then the shardState would be an array of single byte whose bit representation is - 1000 0000
 *                   Explanation - corresponding bit is set to 1 if the shard is assigned, else 0
 */
final case class ShardMapperV2(nodeCountInCluster: Int, numShards: Int, hostNameFormat: String,
                               shardState: Array[Byte])

object ShardMapperV2 {
  /**
   * Converts a ShardMapper.statuses to a bitmap representation where the bit is set to:
   *  - 1, if ShardStatus == ShardStatusActive
   *  - 0, any other ShardStatus like ShardStatusAssigned, ShardStatusRecovery etc.
   *  WHY this is the case ? This is because, the QueryActor is can only execute the query on the active shards.
   *
   * NOTE: bitmap is byte aligned. So extra bits are padded with 0.
   *
   * EXAMPLE - Following are some example of shards with statuses and their bit representation as below:
   *                   Status                          |  BitMap Representation      |  Hex Representation
   *  ---------------------------------------------------------------------------------------------------------------
   *  Assigned, Active, Recovery, Error                |      0100 0000              |      0x40
   *  Active, Active, Active, Active                   |      1111 0000              |      0xF0
   *  Error, Active, Active, Error, Active, Active     |      0110 1100              |      0x6C
   *
   * @param shardMapper ShardMapper object which stores the bitmap representation
   * @return A byte array where each byte represents 8 shards and the bit is set to 1 if the shard is active. Extra bits
   *         are padded with 0.
   */
  def shardMapperBitMapRepresentation(shardMapper: ShardMapper) : Array[Byte] = {
    val byteArray = new Array[Byte]((shardMapper.statuses.length + 7) / 8)
    for (i <- shardMapper.statuses.indices) {
      if (shardMapper.statuses(i) == ShardStatusActive) {
        byteArray(i / 8) = (byteArray(i / 8) | (1 << (7 - (i % 8)))).toByte
      }
    }
    byteArray
  }

  /**
   * @param nodeCountInCluster Number of nodes in the filodb cluster
   * @param numShards Number of shards in the filodb cluster
   * @param hostNameFormat K8s host format. Valid ONLY for ClusterV2 shard assignment strategy
   * @param shardMapper ShardMapper object which stores the bitmap representation
   * @return ShardMapperV2 object
   */
  def apply(nodeCountInCluster: Int, numShards: Int, hostNameFormat: String, shardMapper: ShardMapper) :
  ShardMapperV2 = {
    val shardStatusBitMap = shardMapperBitMapRepresentation(shardMapper)
    ShardMapperV2(nodeCountInCluster, numShards, hostNameFormat, shardStatusBitMap)
  }

  /**
   * @param byte Byte to be converted to bits
   * @return String representation of the byte in bits
   */
  def byteToBits(byte: Byte): String = {
    (0 until 8).map(i => (byte >> (7 - i)) & 1).mkString
  }

  /**
   * @param shardMapperV2 ShardMapperV2 object
   * @return String representation of the byte array in bits
   */
  def bitMapRepresentation(shardMapperV2: ShardMapperV2): String = {
    val shardsPerNode = shardMapperV2.numShards / shardMapperV2.nodeCountInCluster
    shardMapperV2.shardState.map(x => byteToBits(x).grouped(shardsPerNode).mkString(" ")).mkString(" ")
  }

  /**
   * Helper function to print the bitmap, node and shard status. This is useful in debugging.
   */
  def prettyPrint(shardMapperV2: ShardMapperV2): Unit = {
    println(s"NumNodes:  ${shardMapperV2.nodeCountInCluster} NumShards:  ${shardMapperV2.numShards} NodeFormat:  ${shardMapperV2.hostNameFormat}") // scalastyle:ignore
    println(s"ShardStatusBytes:  ${shardMapperV2.shardState.mkString("Array(", ", ", ")")}") // scalastyle:ignore
    println(s"ShardStatusBitMap:  ${bitMapRepresentation(shardMapperV2)}") // scalastyle:ignore
    println() // scalastyle:ignore
    val printFormat = s"%5s\t%20s\t\t%s"
    val shardsPerNode = shardMapperV2.numShards / shardMapperV2.nodeCountInCluster
    println(printFormat.format("Shard", "Status", "Address")) // scalastyle:ignore
    for (shardIndex <- 0 until shardMapperV2.numShards) {
      val byteIndex = shardIndex / 8
      val bitIndex = shardIndex % 8
      val bit = (shardMapperV2.shardState(byteIndex) >> (7 - bitIndex)) & 1
      val status = if (bit == 1) "Shard Active" else "Shard Not Active"
      val nodeNum = shardIndex / shardsPerNode
      // NOTE: In local environment, the Address is dependent on the `filodb.cluster-discovery.hostList` config.
      // The address printed here is an approximation and may not be accurate.
      println(printFormat.format(shardIndex, status, ActorName.nodeCoordinatorPathClusterV2(String.format(shardMapperV2.hostNameFormat, nodeNum.toString)))) // scalastyle:ignore
    }
  }
}

/**
 * Response to GetShardMapV2 request. Uses the optimized ShardMapperV2 representation. Only applicable
 * for ClusterV2 shard assignment strategy.
 * @param map ShardMapperV2
 */
final case class ShardSnapshot(map: ShardMapperV2) extends ShardAction with Response

/**
  * Full state of all shards, sent to all ingestion actors. They react by starting/stopping
  * ingestion for the shards they own or no longer own. The version is expected to be global
  * and monotonically increasing, but if the version is 0, then the actor should skip the
  * version check and blindly apply the resync action.
  */
final case class ShardIngestionState(version: Long, ref: DatasetRef, map: ShardMapper) extends ShardAction

/**
  * The events are sent by the IngestionActor on a node when the actual ingestion stream
  * starts, errors, or by the NodeClusterActor singleton upon detection of node failure / disconnect
  * via Akka Cluster events.
  *
  * These events are subscribed to by the NodeClusterActor, any QueryActors, and any other
  * interested parties. For example in Spark, executors waiting to know when they can start
  * sending records.
  *
  * A ShardMapper can be updated using these events.
  * Events should start with a noun.
  */
sealed trait ShardEvent extends ShardAction {
  def ref: DatasetRef
  def shard: Int
}

/** Used by ShardAssignmentStrategy to assign a temporary state. */
final case class ShardAssignmentStarted(ref: DatasetRef, shard: Int, node: ActorRef) extends ShardEvent

final case class IngestionStarted(ref: DatasetRef, shard: Int, node: ActorRef) extends ShardEvent

final case class RecoveryInProgress(ref: DatasetRef, shard: Int, node: ActorRef, progressPct: Int) extends ShardEvent

final case class IngestionError(ref: DatasetRef, shard: Int, err: Throwable) extends ShardEvent

final case class IngestionStopped(ref: DatasetRef, shard: Int) extends ShardEvent

final case class RecoveryStarted(ref: DatasetRef, shard: Int, node: ActorRef, progressPct: Int) extends ShardEvent

final case class ShardDown(ref: DatasetRef, shard: Int, node: ActorRef) extends ShardEvent

sealed trait ShardStatus extends ShardAction {
  /**
    * Generates the minimal set of events needed to reach the given status.
    */
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent]
}

case object ShardStatusUnassigned extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] = Nil
}

/** Used by ShardAssignmentStrategy to mark the shard as assigned, but ingestion not yet confirmed. */
case object ShardStatusAssigned extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(ShardAssignmentStarted(ref, shard, node))
}

case object ShardStatusActive extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionStarted(ref, shard, node))
}

// NOTE: why are we not using the full throwable instead of String? This is because
// ShardStatus is used in many API responses in ClusterRoute and HealthRoute. Having a `Throwable` type member
// variable will lead to Marshalling errors. Hence, we are using a String here to store the exception message.
case class ShardStatusError(error: String) extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionError(ref, shard, new Exception(error)))
}

final case class ShardStatusRecovery(progressPct: Int) extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(RecoveryInProgress(ref, shard, node, progressPct))
}

case object ShardStatusStopped extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionStarted(ref, shard, node), IngestionStopped(ref, shard))
}

case object ShardStatusDown extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionStarted(ref, shard, node), ShardDown(ref, shard, node))
}

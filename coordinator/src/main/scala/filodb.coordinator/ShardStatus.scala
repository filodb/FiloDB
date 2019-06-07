package filodb.coordinator

import akka.actor.ActorRef

import filodb.core._

/** The base marker. This can manifast as commands, events or state. */
sealed trait ShardAction extends Serializable

/** Sent once to newly-subscribed subscribers to initialize their local ShardMapper. */
final case class CurrentShardSnapshot(ref: DatasetRef,
                                      map: ShardMapper) extends ShardAction with Response

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

case object ShardStatusError extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionStarted(ref, shard, node))
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

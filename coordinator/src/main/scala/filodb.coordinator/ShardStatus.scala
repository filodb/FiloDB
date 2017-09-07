package filodb.coordinator

import akka.actor.ActorRef

import filodb.core._

/** The base marker. This can manifast as commands, events or state. */
sealed trait ShardAction extends Serializable

/** Sent once to newly-subscribed subscribers to initialize their local ShardMapper. */
final case class CurrentShardSnapshot(ref: DatasetRef, map: ShardMapper) extends ShardAction

/**
  * These commands are sent by the NodeClusterActor to the right nodes upon events or
  * changes to the cluster. For example a new node joins, StartShardIngestion might be sent.
  * Should start with a verb, since these are commands.
  */
sealed trait ShardCommand extends ShardAction {
  def ref: DatasetRef
  def shard: Int
}

final case class StartShardIngestion(ref: DatasetRef, shard: Int, offset: Option[Long]) extends ShardCommand

final case class StopShardIngestion(ref: DatasetRef, shard: Int) extends ShardCommand

/** Direct result of sending an invalid [[ShardCommand]]. It is acked to the
  * sender if the shard command's shard or dataset is not valid based on the
  * projection or shard state. It is located with the shard commands because
  * this is not a potential result of an Ingestion command and flows through
  * a node's coordinator, one of its ingesters, the cluster shard actor and
  * its [[filodb.coordinator.ShardAssignmentStrategy]].
  *
  * Use cases: result of invalid state and injecting failure to the memstore
  * during stream creation in the [[StartShardIngestion]] protocol.
  */
final case class InvalidIngestionCommand(ref: DatasetRef, shard: Int)

/**
  * The events are sent by the MemStoreCoordActor on a node when the actual ingestion stream
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
}

final case class IngestionStarted(ref: DatasetRef, shard: Int, node: ActorRef) extends ShardEvent

final case class RecoveryStarted(ref: DatasetRef, shard: Int, node: ActorRef) extends ShardEvent

final case class IngestionError(ref: DatasetRef, shard: Int, err: Throwable) extends ShardEvent

final case class ShardDown(ref: DatasetRef, shard: Int) extends ShardEvent

final case class IngestionStopped(ref: DatasetRef, shard: Int) extends ShardEvent

sealed trait ShardStatus extends ShardAction {
  /**
    * Generates the minimal set of events needed to reach the given status.
    */
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent]
}

case object ShardUnassigned extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] = Nil
}

case object ShardStatusNormal extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionStarted(ref, shard, node))
}

case object ShardStatusRecovery extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(RecoveryStarted(ref, shard, node))
}

case object ShardStatusDown extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionStarted(ref, shard, node), ShardDown(ref, shard))
}

case object ShardStatusStopped extends ShardStatus {
  def minimalEvents(ref: DatasetRef, shard: Int, node: ActorRef): Seq[ShardEvent] =
    Seq(IngestionStarted(ref, shard, node), IngestionStopped(ref, shard))
}

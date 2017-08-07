package filodb.coordinator

import akka.actor.ActorRef

import filodb.core._

/**
  * These commands are sent by the NodeClusterActor to the right nodes upon events or
  * changes to the cluster. For example a new node joins, StartShardIngestion might be sent.
  * Should start with a verb, since these are commands.
  */
sealed trait ShardCommand

final case class StartShardIngestion(ref: DatasetRef, shard: Int, offset: Option[Long]) extends ShardCommand

final case class StopShardIngestion(ref: DatasetRef, shard: Int) extends ShardCommand

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
sealed trait ShardEvent

final case class IngestionStarted(ref: DatasetRef, shard: Int, node: ActorRef) extends ShardEvent

final case class RecoveryStarted(ref: DatasetRef, shard: Int, node: ActorRef) extends ShardEvent

final case class IngestionError(ref: DatasetRef, shard: Int, err: Throwable) extends ShardEvent

final case class ShardDown(ref: DatasetRef, shard: Int) extends ShardEvent

final case class IngestionStopped(ref: DatasetRef, shard: Int) extends ShardEvent


sealed trait ShardStatus {
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

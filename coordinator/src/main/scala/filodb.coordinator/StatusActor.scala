package filodb.coordinator

import scala.collection.mutable.{HashMap, HashSet, Map => MMap}
import scala.concurrent.duration._

import akka.actor.{ActorRef, Cancellable, Props}
import akka.event.LoggingReceive

import filodb.core.DatasetRef

object StatusActor {
  final case class EventEnvelope(sequence: Int, events: Seq[ShardEvent])
  final case class StatusAck(sequence: Int)
  case object GetCurrentEvents

  private[coordinator] case object CheckAck

  def props(clusterProxy: ActorRef, ackTimeout: FiniteDuration): Props =
    Props(new StatusActor(clusterProxy, ackTimeout))
}

/**
 * StatusActor has several jobs:
 * 1) Repeatedly send the latest ShardEvent updates until they are acknowledged by the ClusterActor
 * 2) Keep track of these updates for the local node and respond to requests for these updates
 *
 * The flow is like this:
 * - Every time, all of the ShardEvents from all shards of a dataset are sent
 * - A check is scheduled in the future
 * - If a new ShardEvent comes, then the check is canceled and delayed, sequence bumped, and all events sent again
 * - An ack will cancel the check if the sequence number has caught up
 * - If an ack has not arrived by the check, then the stuff is sent again
 *
 * The maximum size sent is all datasets and events for all shards.  Since each node has a limited number of shards
 * this should not be that big -- we just keep the latest event for each shard.
 *
 * TODO: consider taking action if we cannot reach the NodeCluster singleton after N retries.  Should we shut
 * ourselves down?
 *
 */
private[coordinator] class StatusActor(clusterProxy: ActorRef, ackTimeout: FiniteDuration) extends BaseActor {
  import StatusActor._

  val statuses = new HashMap[DatasetRef, MMap[Int, ShardEvent]]
  var sequenceNo = 0
  var ackedSeqNo = -1
  val updatedRefs = new HashSet[DatasetRef]
  var scheduledCheck: Option[Cancellable] = None
  var _proxy = clusterProxy

  val scheduler = context.system.scheduler
  import context.dispatcher

  def receive: Receive = LoggingReceive {
    case e: ShardEvent    => updateStatuses(e)
                             sendAndScheduleCheck()
    case StatusAck(seqNo) => handleAck(seqNo)
    case CheckAck         => checkAck()
    case GetCurrentEvents => sender() ! statuses.mapValues(_.values.toBuffer)
    case newProxy: ActorRef => _proxy = newProxy
  }

  private def updateStatuses(event: ShardEvent) = {
    val shardMap = statuses.getOrElseUpdate(event.ref, new HashMap[Int, ShardEvent])
    updatedRefs += event.ref
    shardMap(event.shard) = event
  }

  private def sendAndScheduleCheck() = {
    scheduledCheck.foreach(_.cancel())
    sequenceNo += 1
    logger.debug(s"Sending events for refs $updatedRefs, sequenceNo=$sequenceNo")
    updatedRefs.foreach { ref =>
      _proxy ! EventEnvelope(sequenceNo, statuses(ref).values.toSeq)
    }
    scheduledCheck = Some(scheduler.scheduleOnce(ackTimeout, self, CheckAck))
  }

  private def handleAck(seqNo: Int) = {
    ackedSeqNo = seqNo
    if (ackedSeqNo >= sequenceNo) {
      updatedRefs.clear()
      scheduledCheck.foreach(_.cancel())
      logger.debug(s"Acked sequence number $seqNo, reached parity")
    }
  }

  private def checkAck() = {
    logger.debug(s"Checking acks: ackedSeqNo=$ackedSeqNo  sequenceNo=$sequenceNo")
    if (ackedSeqNo < sequenceNo) sendAndScheduleCheck()
  }
}
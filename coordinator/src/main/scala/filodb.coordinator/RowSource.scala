package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap
import scala.concurrent.duration._

import filodb.core._

object RowSource {
  case object Start
  case object GetMoreRows
  case object AllDone
  case object CheckCanIngest
  case class SetupError(err: ErrorResponse)
}

/**
 * RowSource is a trait to make it easy to write sources (Actors) for specific
 * input methods - eg from HTTP JSON, or CSV, or Kafka, etc.
 * It has logic to handle flow control/backpressure.
 * It talks to a NodeCoordinatorActor of a FiloDB node.  RowSource may be remote.
 *
 * To start initialization and reading from source, send the Start message.
 *
 * Backpressure and at least once mechanism:  RowSource sends batches of rows to the NodeCoordinator,
 * but does not send more than maxUnackedBatches before getting an ack back.  As long as it receives
 * acks it will keep sending, but will stop once maxUnackedBatches is reached.  It then goes into a waiting
 * state, waiting for Acks to come back.  Once waitingPeriod has elapsed, if it is allowed to ingest again,
 * then it will replay unacked messages, hopefully get acks back, and once all messages are acked will go
 * back into regular ingestion mode.  This ensures that we do not skip any incoming messages.
 */
trait RowSource extends Actor with StrictLogging {
  import RowSource._

  // Maximum number of unacked batches to push at a time.  Used for flow control.
  def maxUnackedBatches: Int

  def waitingPeriod: FiniteDuration = 5.seconds

  def coordinatorActor: ActorRef

  // Returns the SetupIngestion message needed for initialization
  def getStartMessage(): NodeCoordinatorActor.SetupIngestion

  def dataset: String
  def version: Int

  // Returns newer batches of rows.
  // The seqIDs should be increasing and unique per batch.
  def batchIterator: Iterator[(Long, Seq[RowReader])]

  // Anything additional to do when we hit end of data and it's all acked, before killing oneself
  def allDoneAndGood(): Unit = {}

  private var whoStartedMe: Option[ActorRef] = None
  private val outstanding = new HashMap[Long, Seq[RowReader]]

  import context.dispatcher

  def start: Receive = {
    case Start =>
      whoStartedMe = Some(sender)
      coordinatorActor ! getStartMessage()

    case NodeCoordinatorActor.IngestionReady =>
      self ! GetMoreRows
      logger.info(s" ==> Setup is all done, starting ingestion...")
      context.become(reading)

    case e: ErrorResponse =>
      whoStartedMe.foreach(_ ! SetupError(e))
  }

  def reading: Receive = {
    case GetMoreRows if batchIterator.hasNext => sendRows()
    case GetMoreRows =>
      logger.info(s" ==> (${self.path.name}) doneReading: outstanding seqIds = ${outstanding.keys}")
      if (outstanding.isEmpty)  { sendFlush() }
      else                      { schedule(waitingPeriod, CheckCanIngest) }
      context.become(doneReading)

    case NodeCoordinatorActor.Ack(lastSequenceNo) =>
      if (outstanding contains lastSequenceNo) outstanding.remove(lastSequenceNo)

    case CheckCanIngest =>
  }

  def waitingAck: Receive = {
    case NodeCoordinatorActor.Ack(lastSequenceNo) =>
      if (outstanding contains lastSequenceNo) outstanding.remove(lastSequenceNo)
      if (outstanding.isEmpty) {
        logger.debug(s" ==> reading, all unacked messages acked")
        self ! GetMoreRows
        context.become(reading)
      }
  }

  def waiting: Receive = waitingAck orElse replay

  val replay: Receive = {
    case CheckCanIngest =>
      coordinatorActor ! NodeCoordinatorActor.CheckCanIngest(dataset, version)

    case NodeCoordinatorActor.CanIngest(can) =>
      if (can) {
        logger.debug(s"Yay, we're allowed to ingest again!  Replaying unacked messages")
        outstanding.foreach { case (seqId, batch) =>
          coordinatorActor ! NodeCoordinatorActor.IngestRows(dataset, version, batch, seqId)
        }
      }
      logger.debug(s"Scheduling another CheckCanIngest in case any unacked messages left")
      schedule(waitingPeriod, CheckCanIngest)
  }

  def doneReadingAck: Receive = {
    case NodeCoordinatorActor.Ack(lastSequenceNo) =>
      if (outstanding contains lastSequenceNo) outstanding.remove(lastSequenceNo)
      if (outstanding.isEmpty) sendFlush()

    // Only until we get the Flushed signal do we know all rows are finished flushing.
    case NodeCoordinatorActor.Flushed =>
      finish()
  }

  // If we don't get acks back, need to keep trying
  def doneReading: Receive = doneReadingAck orElse replay

  // Gets the next batch of data from the batchIterator, then determine if we can get more rows
  // or need to wait.
  def sendRows(): Unit = {
    val (nextSeqId, nextBatch) = batchIterator.next
    outstanding(nextSeqId) = nextBatch
    coordinatorActor ! NodeCoordinatorActor.IngestRows(dataset, version, nextBatch, nextSeqId)
    // Go get more rows
    if (outstanding.size < maxUnackedBatches) {
      self ! GetMoreRows
    } else {
      logger.debug(s" ==> waiting: outstanding seqIds = ${outstanding.keys}")
      schedule(waitingPeriod, CheckCanIngest)
      context.become(waiting)
    }
  }

  def sendFlush(): Unit = coordinatorActor ! NodeCoordinatorActor.Flush(dataset, version)

  private var scheduledTask: Option[Cancellable] = None
  def schedule(delay: FiniteDuration, msg: Any): Unit = {
    val task = context.system.scheduler.scheduleOnce(delay, self, msg)
    scheduledTask = Some(task)
  }

  def finish(): Unit = {
    logger.info(s"(${self.path.name}) Ingestion is all done")
    allDoneAndGood()
    whoStartedMe.foreach(_ ! AllDone)
    scheduledTask.foreach(_.cancel)
    self ! PoisonPill
  }

  val receive = start
}

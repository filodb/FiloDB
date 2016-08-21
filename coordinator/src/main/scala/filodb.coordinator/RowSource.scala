package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill}
import akka.event.LoggingReceive
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
  case class IngestionErr(msg: String, cause: Option[Throwable] = None)
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
  def getStartMessage(): IngestionCommands.SetupIngestion

  def dataset: DatasetRef
  def version: Int

  // Returns newer batches of rows.
  // The seqIDs should be increasing and unique per batch.
  def batchIterator: Iterator[(Long, Seq[RowReader])]

  // Anything additional to do when we hit end of data and it's all acked, before killing oneself
  def allDoneAndGood(): Unit = {}

  private var whoStartedMe: Option[ActorRef] = None
  private val outstanding = new HashMap[Long, Seq[RowReader]]

  import context.dispatcher

  def start: Receive = LoggingReceive {
    case Start =>
      whoStartedMe = Some(sender)
      coordinatorActor ! getStartMessage()

    case IngestionCommands.IngestionReady =>
      self ! GetMoreRows
      logger.info(s" ==> Setup is all done, starting ingestion...")
      context.become(reading)

    case e: ErrorResponse =>
      whoStartedMe.foreach(_ ! SetupError(e))
  }

  def reading: Receive = LoggingReceive {
    case GetMoreRows if batchIterator.hasNext => sendRows()
    case GetMoreRows =>
      if (outstanding.isEmpty)  { finish() }
      else                      {
        logger.info(s" ==> (${self.path.name}) doneReading: outstanding seqIds = ${outstanding.keys}")
        schedule(waitingPeriod, CheckCanIngest)
        context.become(doneReading)
      }

    case IngestionCommands.Ack(lastSequenceNo) =>
      if (outstanding contains lastSequenceNo) outstanding.remove(lastSequenceNo)

    case IngestionCommands.UnknownDataset =>
        whoStartedMe.foreach(_ ! IngestionErr("Ingestion actors shut down, check error logs"))

    case CheckCanIngest =>
  }

  def waitingAck: Receive = LoggingReceive {
    case IngestionCommands.Ack(lastSequenceNo) =>
      if (outstanding contains lastSequenceNo) outstanding.remove(lastSequenceNo)
      if (outstanding.isEmpty) {
        logger.debug(s" ==> reading, all unacked messages acked")
        self ! GetMoreRows
        context.become(reading)
      }
  }

  def waiting: Receive = waitingAck orElse replay

  val replay: Receive = LoggingReceive {
    case CheckCanIngest =>
      logger.debug(s"Checking if dataset $dataset can ingest...")
      coordinatorActor ! IngestionCommands.CheckCanIngest(dataset, version)

    case IngestionCommands.CanIngest(can) =>
      if (can) {
        logger.debug(s"Yay, we're allowed to ingest again!  Replaying unacked messages")
        outstanding.foreach { case (seqId, batch) =>
          coordinatorActor ! IngestionCommands.IngestRows(dataset, version, batch, seqId)
        }
      }
      logger.debug(s"Scheduling another CheckCanIngest in case any unacked messages left")
      schedule(waitingPeriod, CheckCanIngest)

    case IngestionCommands.UnknownDataset =>
        whoStartedMe.foreach(_ ! IngestionErr("Ingestion actors shut down, check worker error logs"))

    case t: Throwable =>
        whoStartedMe.foreach(_ ! IngestionErr(t.getMessage, Some(t)))

    case e: ErrorResponse =>
        whoStartedMe.foreach(_ ! IngestionErr(e.toString))
  }

  def doneReadingAck: Receive = LoggingReceive {
    case IngestionCommands.Ack(lastSequenceNo) =>
      if (outstanding contains lastSequenceNo) outstanding.remove(lastSequenceNo)
      if (outstanding.isEmpty) finish()
  }

  // If we don't get acks back, need to keep trying
  def doneReading: Receive = doneReadingAck orElse replay

  // Gets the next batch of data from the batchIterator, then determine if we can get more rows
  // or need to wait.
  def sendRows(): Unit = {
    val (nextSeqId, nextBatch) = batchIterator.next
    outstanding(nextSeqId) = nextBatch
    coordinatorActor ! IngestionCommands.IngestRows(dataset, version, nextBatch, nextSeqId)
    // Go get more rows
    if (outstanding.size < maxUnackedBatches) {
      self ! GetMoreRows
    } else {
      logger.debug(s" ==> waiting: outstanding seqIds = ${outstanding.keys}")
      schedule(waitingPeriod, CheckCanIngest)
      context.become(waiting)
    }
  }

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

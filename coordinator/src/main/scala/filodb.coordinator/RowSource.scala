package filodb.coordinator

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader
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
 * Backpressure and at least once mechanism:  RowSource sends rows to the NodeCoordinator,
 * but does not send more than maxUnackedRows before getting an ack back.  As long as it receives
 * acks it will keep sending, but will stop once maxUnackedRows is reached.  It then goes into a waiting
 * state, waiting for Acks to come back, or periodically pinging with a CheckCanIngest message.  If it
 * is allowed to ingest again, then it proceeds but rewinds from the last acked message.  This ensures
 * that we do not skip any incoming messages.
 */
trait RowSource extends Actor with StrictLogging {
  import RowSource._

  // Maximum number of unacked rows to push at a time.  Used for flow control.
  def maxUnackedRows: Int

  // rows to read at a time
  def rowsToRead: Int

  def waitingPeriod: FiniteDuration
   = 5.seconds

  def coordinatorActor: ActorRef

  // Returns the SetupIngestion message needed for initialization
  def getStartMessage(): NodeCoordinatorActor.SetupIngestion

  def dataset: String
  def version: Int

  // Returns a new row from source => (seqID, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, RowReader)]

  // Anything additional to do when we hit end of data and it's all acked, before killing oneself
  def allDoneAndGood(): Unit = {}

  // Rewinds the input source to the given sequence number, usually called after a timeout
  def rewindTo(seqNo: Long): Unit = {}

  // Needs to be initialized to the first sequence # at the beginning
  var lastAckedSeqNo: Long

  private var currentHiSeqNo: Long = lastAckedSeqNo
  private var whoStartedMe: ActorRef = _

  import context.dispatcher

  def start: Receive = {
    case Start =>
      whoStartedMe = sender
      coordinatorActor ! getStartMessage()

    case NodeCoordinatorActor.IngestionReady =>
      self ! GetMoreRows
      logger.info(s" ==> Setup is all done, starting ingestion...")
      context.become(reading)

    case e: ErrorResponse =>
      whoStartedMe ! SetupError(e)
  }

  def reading: Receive = {
    case GetMoreRows =>
      val rows = (1 to rowsToRead).iterator
                   .map(i => getNewRow())
                   .takeWhile(_.isDefined)
                   .toSeq.flatten
      if (rows.nonEmpty) {
        val maxSeqId = rows.map(_._1).max
        currentHiSeqNo = Math.max(currentHiSeqNo, maxSeqId)
        coordinatorActor ! NodeCoordinatorActor.IngestRows(dataset, version, rows.map(_._2), currentHiSeqNo)
        // Go get more rows
        if (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows) {
          self ! GetMoreRows
        } else {
          logger.debug(s" ==> waiting: currentHi = $currentHiSeqNo, lastAcked = $lastAckedSeqNo")
          context.system.scheduler.scheduleOnce(waitingPeriod, self, CheckCanIngest)
          context.become(waiting)
        }
      } else {
        logger.debug(s" ==> doneReading: HiSeqNo = $currentHiSeqNo, lastAcked = $lastAckedSeqNo")
        if (currentHiSeqNo == lastAckedSeqNo) { finish() }
        else { context.become(doneReading) }
      }

    case NodeCoordinatorActor.Ack(lastSequenceNo) =>
      lastAckedSeqNo = lastSequenceNo

    case CheckCanIngest =>
  }

  def waiting: Receive = {
    case NodeCoordinatorActor.Ack(lastSequenceNo) =>
      lastAckedSeqNo = lastSequenceNo
      if (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows) {
        logger.debug(s" ==> reading")
        self ! GetMoreRows
        context.become(reading)
      }

    case CheckCanIngest =>
      coordinatorActor ! NodeCoordinatorActor.CheckCanIngest(dataset, version)

    case NodeCoordinatorActor.CanIngest(can) =>
      if (can) {
        logger.debug(s"Yay, we're allowed to ingest again!  Rewinding to $lastAckedSeqNo")
        rewindTo(lastAckedSeqNo)
        self ! GetMoreRows
        context.become(reading)
      } else {
        logger.debug(s"Still waiting...")
        context.system.scheduler.scheduleOnce(waitingPeriod, self, CheckCanIngest)
      }
  }

  def doneReading: Receive = {
    case NodeCoordinatorActor.Ack(lastSequenceNo) =>
      lastAckedSeqNo = lastSequenceNo
      if (currentHiSeqNo == lastAckedSeqNo) finish()
  }

  def finish(): Unit = {
    logger.info(s"Ingestion is all done")
    coordinatorActor ! NodeCoordinatorActor.Flush(dataset, version)
    allDoneAndGood()
    whoStartedMe ! AllDone
    self ! PoisonPill
  }

  val receive = start
}

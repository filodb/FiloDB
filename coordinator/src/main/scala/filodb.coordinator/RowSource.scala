package filodb.coordinator

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader

import filodb.core._

object RowSource {
  case object Start
  case object GetMoreRows
  case object AllDone
  case class SetupError(err: ErrorResponse)
}

/**
 * RowSource is a trait to make it easy to write sources (Actors) for specific
 * input methods - eg from HTTP JSON, or CSV, or Kafka, etc.
 * It has logic to handle flow control/backpressure.
 * It talks to a CoordinatorActor of a FiloDB node.  RowSource may be remote.
 *
 * To start initialization and reading from source, send the Start message.
 */
trait RowSource extends Actor with StrictLogging {
  import RowSource._

  // Maximum number of unacked rows to push at a time.  Used for flow control.
  def maxUnackedRows: Int

  // rows to read at a time
  def rowsToRead: Int

  def coordinatorActor: ActorRef

  // Returns the SetupIngestion message needed for initialization
  def getStartMessage(): CoordinatorActor.SetupIngestion

  def dataset: String
  def version: Int

  // Returns a new row from source => (seqID, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, RowReader)]

  // Anything additional to do when we hit end of data and it's all acked, before killing oneself
  def allDoneAndGood(): Unit = {}

  // Needs to be initialized to the first sequence # at the beginning
  var lastAckedSeqNo: Long

  private var currentHiSeqNo: Long = lastAckedSeqNo
  private var isDoneReading: Boolean = false
  private var whoStartedMe: ActorRef = _

  def receive: Receive = {
    case Start =>
      whoStartedMe = sender
      coordinatorActor ! getStartMessage()

    case CoordinatorActor.IngestionReady =>
      self ! GetMoreRows

    case e: ErrorResponse =>
      whoStartedMe ! SetupError(e)

    case GetMoreRows =>
      val rows = (1 to rowsToRead).iterator
                   .map(i => getNewRow())
                   .takeWhile(_.isDefined)
                   .toSeq.flatten
      if (rows.nonEmpty) {
        val maxSeqId = rows.map(_._1).max
        currentHiSeqNo = Math.max(currentHiSeqNo, maxSeqId)
        coordinatorActor ! CoordinatorActor.IngestRows(dataset, version, rows.map(_._2), currentHiSeqNo)
        // Go get more rows
        if (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows) {
          self ! GetMoreRows
        } else {
          logger.debug(s"Over high water mark: currentHi = $currentHiSeqNo, lastAcked = $lastAckedSeqNo")
        }
      } else {
        logger.debug(s"Marking isDoneReading as true: HiSeqNo = $currentHiSeqNo, lastAcked = $lastAckedSeqNo")
        isDoneReading = true
      }

    case CoordinatorActor.Ack(lastSequenceNo) =>
      lastAckedSeqNo = lastSequenceNo
      if (!isDoneReading && (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows)) self ! GetMoreRows
      if (isDoneReading && currentHiSeqNo == lastAckedSeqNo) {
        logger.info(s"Ingestion is all done")
        coordinatorActor ! CoordinatorActor.Flush(dataset, version)
        allDoneAndGood()
        whoStartedMe ! AllDone
        self ! PoisonPill
      }
  }
}

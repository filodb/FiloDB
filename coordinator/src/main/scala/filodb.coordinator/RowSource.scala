package filodb.coordinator

import akka.actor.{Actor, ActorRef, PoisonPill}
import com.typesafe.scalalogging.slf4j.StrictLogging

import filodb.core.columnstore.RowReader

object RowSource {
  case object Start
  case object GetMoreRows
  case object AllDone
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

    case GetMoreRows =>
      val rows = (1 to rowsToRead).iterator
                   .map(i => getNewRow())
                   .takeWhile { row =>
                     if (!row.isDefined) isDoneReading = true
                     row.isDefined
                   }.collect { case Some((seqID, row)) =>
                     currentHiSeqNo = seqID
                     row
                   }.toSeq
      if (rows.nonEmpty) {
        coordinatorActor ! CoordinatorActor.IngestRows(dataset, version, rows, currentHiSeqNo)
      }
      if (isDoneReading) {
        logger.debug("Marking isDoneReading as true")
        coordinatorActor ! CoordinatorActor.Flush(dataset, version)
      } else if (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows) {
        self ! GetMoreRows
      }

    case CoordinatorActor.Ack(lastSequenceNo) =>
      lastAckedSeqNo = lastSequenceNo
      if (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows) self ! GetMoreRows
      if (isDoneReading && currentHiSeqNo == lastAckedSeqNo) {
        logger.info(s"Ingestion is all done")
        allDoneAndGood()
        whoStartedMe ! AllDone
        self ! PoisonPill
      }
  }
}

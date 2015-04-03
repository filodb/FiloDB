package filodb.core.ingest

import akka.actor.{Actor, ActorRef}

object RowSource {
  case object GetMoreRows
}

/**
 * RowSource is a trait to make it easy to write sources (Actors) for specific
 * input methods - eg from HTTP JSON, or CSV, or Kafka, etc.
 * It has logic to handle flow control/backpressure.
 *
 * To start itself, send a message to itself GetMoreRows.
 */
trait RowSource[R] extends Actor {
  import RowSource._

  // Maximum number of unacked rows to push at a time.
  // Should be greater than the chunkSize.
  def maxUnackedRows: Int

  // rows to read at a time
  def rowsToRead: Int

  // Should come from RowIngesterReady message
  def rowIngesterActor: ActorRef

  // Returns a new row from source => (seqID, rowID, version, row)
  // The seqIDs should be increasing.
  // Returns None if the source reached the end of data.
  def getNewRow(): Option[(Long, Long, Int, R)]

  // What to do when we hit end of data and it's all acked. Typically, return OK and kill oneself.
  def allDoneAndGood(): Unit

  // Needs to be initialized to the first sequence # at the beginning
  var lastAckedSeqNo: Long

  private var currentHiSeqNo: Long = lastAckedSeqNo
  private var isDoneReading: Boolean = false

  def receive: Receive = {
    case GetMoreRows =>
      for { i <- 1 to rowsToRead } {
        getNewRow() match {
          case Some((seqID, rowID, version, row)) =>
            currentHiSeqNo = seqID
            rowIngesterActor ! RowIngesterActor.Row(seqID, rowID, version, row)
          case None =>
            rowIngesterActor ! RowIngesterActor.Flush
            isDoneReading = true
        }
      }
      if (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows) self ! GetMoreRows
    case IngesterActor.Ack(_, _, lastSequenceNo) =>
      lastAckedSeqNo = lastSequenceNo
      if (currentHiSeqNo - lastAckedSeqNo < maxUnackedRows) self ! GetMoreRows
      if (isDoneReading && currentHiSeqNo == lastAckedSeqNo) allDoneAndGood()
  }
}

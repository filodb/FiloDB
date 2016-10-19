package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.event.LoggingReceive
import com.typesafe.scalalogging.slf4j.StrictLogging
import kamon.Kamon
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap
import scala.concurrent.duration._

import filodb.core._

object RowSource {
  case object Start
  case object GetMoreRows
  case object AllDone
  case object CheckCanIngest
  case object AckTimeout
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
 * acks it will keep sending. If no acks are received in ackTimeout, it goes into a waiting state, checking
 * if it can replay messages, replaying them, and waiting for acks for all of them to be received.
 * At that point it will go back into regular reading mode.
 * If the memtable is full, then we receive a Nack, and also go into waiting mode, but the coordinator will
 * send us a ResumeIngest message so we know when to start sending again.
 *
 * ackTimeouts should be an exceptional event, not a regular occurrence.  Make sure to adjust for network
 * response times.
 */
trait RowSource extends Actor with StrictLogging {
  import RowSource._

  // Maximum number of unacked batches to push at a time.  Used for flow control.
  def maxUnackedBatches: Int

  def waitingPeriod: FiniteDuration = 5.seconds
  def ackTimeout: FiniteDuration = 20.seconds

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

  // *** Metrics ***
  private val kamonTags = Map("dataset" -> dataset.dataset, "version" -> version.toString)
  private val rowsIngested = Kamon.metrics.counter("source-rows-ingested", kamonTags)
  private val rowsReplayed = Kamon.metrics.counter("source-rows-replayed", kamonTags)
  private val unneededAcks = Kamon.metrics.counter("source-unneeded-acks", kamonTags)

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

  private def handleAck(seqNo: Long): Unit = {
      if (outstanding contains seqNo) { outstanding.remove(seqNo) }
      else                            { unneededAcks.increment }
  }

  def errorCatcher: Receive = LoggingReceive {
    case IngestionCommands.UnknownDataset =>
      whoStartedMe.foreach(_ ! IngestionErr("Ingestion actors shut down, check error logs"))

    case t: Throwable =>
        whoStartedMe.foreach(_ ! IngestionErr(t.getMessage, Some(t)))

    case e: ErrorResponse =>
        whoStartedMe.foreach(_ ! IngestionErr(e.toString))
  }

  def reading: Receive = (LoggingReceive {
    case GetMoreRows if batchIterator.hasNext => sendRows()
    case GetMoreRows =>
      if (outstanding.isEmpty)  { finish() }

    case IngestionCommands.Ack(lastSequenceNo) =>
      handleAck(lastSequenceNo)
      scheduledTask.foreach(_.cancel)
      if (outstanding.nonEmpty) schedule(ackTimeout, AckTimeout)
      // Keep ingestion going if below half of maxOutstanding items
      if (outstanding.size < (maxUnackedBatches / 2)) self ! GetMoreRows

    case IngestionCommands.Nack(seqNo) =>
      goToWaiting()

    case AckTimeout =>
      logger.warn(s" ==> (${self.path.name}) No Acks received for last $ackTimeout")
      goToWaiting()
  }) orElse errorCatcher

  private def goToWaiting(): Unit = {
    logger.info(s" ==> (${self.path.name}) waiting: outstanding seqIds = ${outstanding.keys}")
    schedule(waitingPeriod, CheckCanIngest)
    context.become(waiting)
  }

  def waitingAck: Receive = LoggingReceive {
    case IngestionCommands.Ack(lastSequenceNo) =>
      handleAck(lastSequenceNo)
      if (outstanding.isEmpty) {
        logger.info(s" ==> (${self.path.name}) reading, all unacked messages acked")
        self ! GetMoreRows
        scheduledTask.foreach(_.cancel)
        context.become(reading)
      }
  }

  def waiting: Receive = waitingAck orElse replay orElse errorCatcher

  private def checkIngest(): Unit = {
    logger.debug(s"Checking if dataset $dataset can ingest...")
    coordinatorActor ! IngestionCommands.CheckCanIngest(dataset, version)
  }

  val replay: Receive = LoggingReceive {
    case GetMoreRows =>

    case IngestionCommands.ResumeIngest => checkIngest()
    case CheckCanIngest                 => checkIngest()

    case IngestionCommands.CanIngest(can) =>
      if (can) {
        logger.debug(s" ==> (${self.path.name}) Replaying unacked messages with ids=${outstanding.keys}")
        outstanding.foreach { case (seqId, batch) =>
          coordinatorActor ! IngestionCommands.IngestRows(dataset, version, batch, seqId)
          rowsReplayed.increment(batch.length)
        }
      }
      schedule(waitingPeriod, CheckCanIngest)
  }

  // Gets the next batch of data from the batchIterator, then determine if we can get more rows
  // or need to wait.
  def sendRows(): Unit = {
    val (nextSeqId, nextBatch) = batchIterator.next
    outstanding(nextSeqId) = nextBatch
    rowsIngested.increment(nextBatch.length)
    coordinatorActor ! IngestionCommands.IngestRows(dataset, version, nextBatch, nextSeqId)
    if (scheduledTask.isEmpty || scheduledTask.get.isCancelled) schedule(ackTimeout, AckTimeout)
    // Go get more rows
    if (outstanding.size < maxUnackedBatches) self ! GetMoreRows
  }

  private var scheduledTask: Option[Cancellable] = None
  def schedule(delay: FiniteDuration, msg: Any): Unit = {
    scheduledTask.foreach(_.cancel)
    val task = context.system.scheduler.scheduleOnce(delay, self, msg)
    scheduledTask = Some(task)
  }

  def finish(): Unit = {
    logger.info(s"(${self.path.name}) Ingestion is all done")
    allDoneAndGood()
    whoStartedMe.foreach(_ ! AllDone)
    scheduledTask.foreach(_.cancel)
    // context.stop() stops remaining incoming messages, ensuring that we won't call finish() here multiple
    // times
    context.stop(self)
  }

  val receive = start
}

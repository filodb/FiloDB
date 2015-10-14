package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.Config
import org.velvia.filo.RowReader
import scala.concurrent.Future

import filodb.core.metadata.{Column, Dataset, Projection}
import filodb.core.columnstore.ColumnStore
import filodb.core.reprojector.{MemTable, Reprojector}

object DatasetCoordinatorActor {
  import filodb.core.Types._

  /**
   * One-time setup of dataset ingestion.
   * @param columns the schema of Columns to ingest. Each row should have the same schema.
   * @param defaultPartitionKey if Some(key), a null value in partitioning column will cause key to be used.
   *                            if None, then NullPartitionValue will be thrown when null value
   *                            is encountered in a partitioning column.
   * @return see return values in NodeCoordinatorActor
   */
  case class Setup(replyTo: ActorRef,
                   columns: Seq[Column],
                   defaultPartitionKey: Option[PartitionKey] = None)

  /**
   * Ingests a bunch of rows into this dataset, version's memtable.
   * Automatically checks at the end if the memtable should be flushed, and sends a StartFlush
   * message to itself to initiate a flush.  This is done before the Ack is sent back.
   * @return Ack(seqNo)
   */
  case class NewRows(ackTo: ActorRef, rows: Seq[RowReader], seqNo: Long)

  /**
   * Initiates a memtable flush to the columnStore if one is not already in progress.
   * Can also be used to listen to / wait for when a reprojection is finished.
   * @param replyTo optionally, add to the list of folks to get a message back when flush/reprojection
   *                finishes.
   */
  case class StartFlush(replyTo: Option[ActorRef] = None)

  /**
   * Clears all data from the projection.  Waits for existing flush to finish first.
   */
  case class ClearProjection(replyTo: ActorRef, projection: Projection)

  /**
   * Returns current stats.  Note: numRows* will return -1 if the memtable is not set up yet
   */
  case object GetStats
  case class Stats(flushesStarted: Int,
                   flushesSucceeded: Int,
                   flushesFailed: Int,
                   numRowsActive: Long,
                   numRowsFlushing: Long)

  // Internal messages
  case class FlushDone(result: Seq[String])
  case class FlushFailed(t: Throwable)

  def props(datasetObj: Dataset,
            version: Int,
            columnStore: ColumnStore,
            reprojector: Reprojector,
            config: Config,
            memTable: MemTable): Props =
    Props(classOf[DatasetCoordinatorActor], datasetObj, version, columnStore,
          reprojector, config, memTable)
}

/**
 * The DatasetCoordinatorActor coordinates row ingestion and scheduling reprojections to the columnstore.
 *
 * One per (dataset, version) per node.
 *
 * Responsible for:
 * - Owning and setting up the MemTable
 * - Feeding rows into the MemTable
 * - Forwarding acks from MemTable back to client
 * - Scheduling memtable flushes, “flipping” MemTables, and
 *   calling Reprojector to convert MemTable rows to Segments
 * - Calling the ColumnStore to append Segments to disk
 *
 * Scheduling is reactive and not done on a schedule right now.  Flushes are checked and initiated after
 * rows are inserted and after flushes are done.
 *
 * ==Configuration==
 * {{{
 *   memtable {
 *     flush-trigger-rows = 50000    # No of rows above which memtable flush might be triggered
 *   }
 * }}}
 */
class DatasetCoordinatorActor(datasetObj: Dataset,
                              version: Int,
                              columnStore: ColumnStore,
                              reprojector: Reprojector,
                              config: Config,
                              memTable: MemTable) extends BaseActor {
  import DatasetCoordinatorActor._
  import context.dispatcher

  val flushTriggerRows = config.getLong("memtable.flush-trigger-rows")

  def activeRows: Option[Long] = memTable.numRows(datasetObj.name, version, MemTable.Active)
  def flushingRows: Option[Long] = memTable.numRows(datasetObj.name, version, MemTable.Locked)

  var curReprojection: Option[Future[Seq[String]]] = None
  var flushesStarted = 0
  var flushesSucceeded = 0
  var flushesFailed = 0

  private def reportStats(): Unit = {
    logger.info(s"MemTable active table rows: $activeRows")
    logger.info(s"MemTable flushing rows: $flushingRows")
  }

  var flushedCallbacks: List[ActorRef] = Nil

  private def ingestRows(ackTo: ActorRef, rows: Seq[RowReader], seqNo: Long): Unit = {
    // TODO: backpressure based on max # of rows in memtable
    val msg = memTable.ingestRows(datasetObj.name, version, rows) match {
      case MemTable.NoSuchDatasetVersion => Some(NodeCoordinatorActor.UnknownDataset)
      case MemTable.Ingested             => Some(NodeCoordinatorActor.Ack(seqNo))
      case MemTable.PleaseWait           =>
        logger.info(s"MemTable full or low on memory, try rows again later...")
        None
    }

    // Now, check how full the memtable is, and if it needs to be flipped, and a flush started
    if (shouldFlush) self ! StartFlush()

    msg.foreach(ackTo.!)
  }

  private def startFlush(): Unit = {
    logger.info(s"Starting new flush cycle for (${datasetObj.name}/$version)...")
    reportStats()
    if (memTable.flipBuffers(datasetObj.name, version) != MemTable.Flipped) {
      logger.warn("This should not happen, unless Scheduler is running concurrently!")
      return
    }
    val newTaskFuture = reprojector.newTask(memTable, datasetObj.name, version)
    curReprojection = Some(newTaskFuture)
    flushesStarted += 1
    newTaskFuture.map { results =>
      self ! FlushDone(results)
    }
    newTaskFuture.recover {
      case t: Throwable => self ! FlushFailed(t)
    }
  }

  private def shouldFlush: Boolean =
    activeRows.map { numRows =>
      (numRows >= flushTriggerRows && curReprojection == None)
    }.getOrElse(false)

  private def handleFlushDone(): Unit = {
    curReprojection = None
    flushedCallbacks.foreach { ref => ref ! NodeCoordinatorActor.Flushed }
    flushedCallbacks = Nil
    flushesSucceeded += 1
    // See if another flush needs to be initiated
    if (shouldFlush) self ! StartFlush()
  }

  private def handleFlushErr(t: Throwable): Unit = {
    // TODO: let everyone know task failed?  Or not until retries are all up?
    flushedCallbacks.foreach { ref => ref ! FlushFailed(t) }
    flushedCallbacks = Nil
    flushesFailed += 1
    // TODO: retry?
    // Don't delete reprojection.  Just let everything suspend?
  }

  private def clearProjection(originator: ActorRef, projection: Projection): Unit = {
    for { flushResult <- curReprojection.getOrElse(Future.successful(Nil))
          resp <- columnStore.clearProjectionData(projection) }
    { originator ! NodeCoordinatorActor.ProjectionTruncated }
  }

  def receive: Receive = {
    case Setup(replyTo, columns, defaultPartKey) =>
      memTable.setupIngestion(datasetObj, columns, version, defaultPartKey) match {
        case MemTable.SetupDone      => replyTo ! NodeCoordinatorActor.IngestionReady
        // If the table is already set up, that's fine!
        case MemTable.AlreadySetup   => replyTo ! NodeCoordinatorActor.IngestionReady
        case MemTable.BadSchema(msg) => replyTo ! NodeCoordinatorActor.BadSchema(msg)
      }

    case NewRows(ackTo, rows, seqNo) =>
      ingestRows(ackTo, rows, seqNo)

    case StartFlush(originator) =>
      originator.foreach { callbackRef => flushedCallbacks = flushedCallbacks :+ callbackRef }
      if (!curReprojection.isDefined) { startFlush() }
      else { logger.debug(s"Ignoring StartFlush, reprojection already in progress...") }

    case ClearProjection(replyTo, projection) =>
      clearProjection(replyTo, projection)

    case GetStats =>
      sender ! Stats(flushesStarted, flushesSucceeded, flushesFailed,
                     activeRows.getOrElse(-1L), flushingRows.getOrElse(-1L))

    case FlushDone(results) =>
      logger.info(s"Reprojection task (${datasetObj.name}, $version) succeeded: ${results.toList}")
      reportStats()
      handleFlushDone()

    case FlushFailed(t) =>
      logger.error(s"Error in reprojection task (${datasetObj.name}, $version)", t)
      reportStats()
      handleFlushErr(t)
  }
}

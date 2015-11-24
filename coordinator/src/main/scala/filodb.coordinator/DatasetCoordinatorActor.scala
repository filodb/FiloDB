package filodb.coordinator

import akka.actor.{Actor, ActorRef, Props}
import com.typesafe.config.Config
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.concurrent.Future

import filodb.core.metadata.{Column, Dataset, Projection, RichProjection}
import filodb.core.columnstore.ColumnStore
import filodb.core.reprojector.{MemTable, FiloMemTable, Reprojector}

object DatasetCoordinatorActor {
  import filodb.core.Types._

  sealed trait DSCoordinatorMessage

  /**
   * Ingests a bunch of rows into this dataset, version's memtable.
   * Automatically checks at the end if the memtable should be flushed, and sends a StartFlush
   * message to itself to initiate a flush.  This is done before the Ack is sent back.
   * @param rows the rows to ingest.  Must have the same schema, for now, as the columns passed in
   *        when the ingestion was set up.
   * @return Ack(seqNo)
   */
  case class NewRows(ackTo: ActorRef, rows: Seq[RowReader], seqNo: Long) extends DSCoordinatorMessage

  // The default minimum amt of memory in MB to allow ingesting more data
  val DefaultMinFreeMb = 512

  /**
   * Initiates a memtable flush to the columnStore if one is not already in progress.
   * Can also be used to listen to / wait for when a reprojection is finished.
   * @param replyTo optionally, add to the list of folks to get a message back when flush/reprojection
   *                finishes.
   */
  case class StartFlush(replyTo: Option[ActorRef] = None) extends DSCoordinatorMessage

  // Checks if memtable is ready to accept more rows.  Common reason why not is due to lack of memory
  // or the row limit has been reached
  case object CanIngest extends DSCoordinatorMessage

  /**
   * Clears all data from the projection.  Waits for existing flush to finish first.
   */
  case class ClearProjection(replyTo: ActorRef, projection: Projection) extends DSCoordinatorMessage

  /**
   * Returns current stats.  Note: numRows* will return -1 if the memtable is not set up yet
   */
  case object GetStats extends DSCoordinatorMessage
  case class Stats(flushesStarted: Int,
                   flushesSucceeded: Int,
                   flushesFailed: Int,
                   numRowsActive: Int,
                   numRowsFlushing: Int) extends DSCoordinatorMessage

  // Internal messages
  case class FlushDone(result: Seq[String]) extends DSCoordinatorMessage
  case class FlushFailed(t: Throwable) extends DSCoordinatorMessage

  def props[K](projection: RichProjection[K],
               version: Int,
               columnStore: ColumnStore,
               reprojector: Reprojector,
               config: Config): Props =
    Props(classOf[DatasetCoordinatorActor[K]], projection, version, columnStore, reprojector, config)
}

/**
 * The DatasetCoordinatorActor coordinates row ingestion and scheduling reprojections to the columnstore.
 *
 * One per (dataset, version) per node.
 *
 * Responsible for:
 * - Owning and setting up MemTables for ingestion
 * - Feeding rows into the MemTable, and knowing when not to feed the memTable
 * - Forwarding acks from MemTable back to client
 * - Scheduling memtable flushes, “flipping” MemTables, and
 *   calling Reprojector to convert MemTable rows to Segments and flush them to disk
 *
 * Scheduling is reactive and not done on a schedule right now.  Flushes are checked and initiated after
 * rows are inserted and after flushes are done.
 *
 * There are usually two MemTables held by this actor.  One, the activeMemTable, ingests incoming rows.
 * The other, the flushingMemTable, is immutable and holds rows for flushing.  The two are switched.
 *
 * ==Configuration==
 * {{{
 *   memtable {
 *     flush-trigger-rows = 50000    # No of rows above which memtable flush might be triggered
 *     max-rows-per-table = 1000000
 *     min-free-mb = 512
 *   }
 * }}}
 */
private[filodb] class DatasetCoordinatorActor[K](projection: RichProjection[K],
                                 version: Int,
                                 columnStore: ColumnStore,
                                 reprojector: Reprojector,
                                 config: Config) extends BaseActor {
  import DatasetCoordinatorActor._
  import context.dispatcher

  val datasetName = projection.dataset.name
  val nameVer = s"$datasetName/$version"
  // TODO: consider passing in a DatasetCoordinatorSettings class, so config doesn't have to be reparsed
  val flushTriggerRows = config.getLong("memtable.flush-trigger-rows")
  val maxRowsPerTable = config.getInt("memtable.max-rows-per-table")
  val minFreeMb = config.as[Option[Int]]("memtable.min-free-mb").getOrElse(DefaultMinFreeMb)

  def activeRows: Int = activeTable.numRows
  def flushingRows: Int = flushingTable.map(_.numRows).getOrElse(-1)

  var activeTable: MemTable[K] = makeNewTable
  var flushingTable: Option[MemTable[K]] = None

  var curReprojection: Option[Future[Seq[String]]] = None
  var flushesStarted = 0
  var flushesSucceeded = 0
  var flushesFailed = 0

  def makeNewTable(): MemTable[K] = new FiloMemTable(projection, config)

  private def reportStats(): Unit = {
    logger.info(s"MemTable active table rows: $activeRows")
    logger.info(s"MemTable flushing rows: $flushingRows")
  }

  var flushedCallbacks: List[ActorRef] = Nil

  private def ingestRows(ackTo: ActorRef, rows: Seq[RowReader], seqNo: Long): Unit = {
    if (canIngest) {
      // MemTable will take callback function and call back to ack rows
      activeTable.ingestRows(rows) { ackTo ! NodeCoordinatorActor.Ack(seqNo) }

      // Now, check how full the memtable is, and if it needs to be flipped, and a flush started
      if (shouldFlush) self ! StartFlush()
    }
  }

  private def canIngest: Boolean = {
    // TODO: gate on total rows in both active and flushing tables, not just active table?
    if (activeRows >= maxRowsPerTable) {
      logger.debug(s"MemTable ($nameVer) is at $activeRows rows, cannot accept writes...")
      false
    } else {
      val freeMB = getRealFreeMb
      if (freeMB < minFreeMb) {
        logger.info(s"Only $freeMB MB memory left, cannot accept more writes...")
        logger.info(s"Active table ($nameVer) has $activeRows rows")
        sys.runtime.gc()
        false
      }
      true
    }
  }

  private def getRealFreeMb: Int =
    ((sys.runtime.maxMemory - (sys.runtime.totalMemory - sys.runtime.freeMemory)) / (1024 * 1024)).toInt

  private def startFlush(): Unit = {
    logger.info(s"Starting new flush cycle for ($nameVer)...")
    reportStats()
    if (curReprojection.isDefined || flushingTable.isDefined) {
      logger.warn("This should not happen. Not starting flush; $curReprojection; $flushingTable")
      return
    }
    activeTable.forceCommit()
    flushingTable = Some(activeTable)
    activeTable = makeNewTable()
    val newTaskFuture = reprojector.reproject(flushingTable.get, version)
    curReprojection = Some(newTaskFuture)
    flushesStarted += 1
    for { results <- newTaskFuture } self ! FlushDone(results)
    newTaskFuture.recover {
      case t: Throwable => self ! FlushFailed(t)
    }
  }

  private def shouldFlush: Boolean =
    activeRows >= flushTriggerRows && curReprojection == None

  private def handleFlushDone(): Unit = {
    curReprojection = None
    // Close the flushing table and mark it as None
    flushingTable.foreach(_.close())
    flushingTable = None
    for { ref <- flushedCallbacks } ref ! NodeCoordinatorActor.Flushed
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
    case NewRows(ackTo, rows, seqNo) =>
      ingestRows(ackTo, rows, seqNo)

    case StartFlush(originator) =>
      originator.foreach { callbackRef => flushedCallbacks = flushedCallbacks :+ callbackRef }
      if (!curReprojection.isDefined) { startFlush() }
      else { logger.debug(s"Ignoring StartFlush, reprojection already in progress...") }

    case ClearProjection(replyTo, projection) =>
      clearProjection(replyTo, projection)

    case CanIngest =>
      sender ! NodeCoordinatorActor.CanIngest(canIngest)

    case GetStats =>
      sender ! Stats(flushesStarted, flushesSucceeded, flushesFailed,
                     activeRows, flushingRows)

    case FlushDone(results) =>
      logger.info(s"Reprojection task ($nameVer) succeeded: ${results.toList}")
      reportStats()
      handleFlushDone()

    case FlushFailed(t) =>
      logger.error(s"Error in reprojection task ($nameVer)", t)
      reportStats()
      handleFlushErr(t)
  }
}

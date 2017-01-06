package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.cluster.Cluster
import akka.event.LoggingReceive
import com.typesafe.config.Config
import filodb.coordinator.IngestionCommands.DCAReady
import kamon.Kamon
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader

import scala.collection.mutable.{ArrayBuffer, HashSet}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future
import filodb.core.metadata.{Column, Dataset, Projection, RichProjection}
import filodb.core.store.{ColumnStore, SegmentInfo}
import filodb.core.reprojector.{FiloMemTable, MemTable, Reprojector}

object DatasetCoordinatorActor {
  import filodb.core.Types._

  sealed trait DSCoordinatorMessage

  /**
   * Ingests a bunch of rows into this dataset, version's memtable.
   * Automatically checks at the end if the memtable should be flushed, and sends a StartFlush
   * message to itself to initiate a flush.  This is done before the Ack is sent back.
   *
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
   *
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
   * TODO: Deprecate?  We now have Kamon, though that doesn't bring all stats back to driver
   */
  case object GetStats extends DSCoordinatorMessage
  case class Stats(flushesStarted: Int,
                   flushesSucceeded: Int,
                   flushesFailed: Int,
                   numRowsActive: Int,
                   numRowsFlushing: Int,
                   memtableRowsIngested: Long) extends DSCoordinatorMessage

  // Internal messages
  case object MemTableWrite extends DSCoordinatorMessage
  case class FlushDone(result: Seq[SegmentInfo[_, _]]) extends DSCoordinatorMessage
  case class FlushFailed(t: Throwable) extends DSCoordinatorMessage

  /**
    * Creates new Memtable using WAL file
    */
  final case class InitIngestion(replyTo: ActorRef) extends DSCoordinatorMessage

  def props(projection: RichProjection,
            version: Int,
            columnStore: ColumnStore,
            reprojector: Reprojector,
            config: Config,
            reloadFlag: Boolean = false): Props =
    Props(classOf[DatasetCoordinatorActor], projection, version, columnStore, reprojector, config, reloadFlag)
}

/**
 * The DatasetCoordinatorActor coordinates row ingestion and scheduling reprojections to the columnstore.
 *
 * One per (dataset, version) per node.
 *
 * Responsible for:
 * - Owning and setting up MemTables for ingestion
 * - Feeding rows into the MemTable, and knowing when not to feed the memTable
 * - Forwarding acks and Nacks back to client
 * - Scheduling memtable flushes, “flipping” MemTables, and
 *   calling Reprojector to convert MemTable rows to Segments and flush them to disk
 *
 * Flushes are done when memtable is full or when noActivityFlushInterval has elapsed with no writes.
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
 *     filo.chunksize = 1000   # The number of rows per Filo chunk
 *     flush.interval = 1 s    # If less than chunksize rows ingested before this interval, then
 *                             # rows will be written into the WAL and MemTable. NOTE: this is not the
 *                             # columnar store flush interval.
 *   }
 * }}}
*/
private[filodb] class DatasetCoordinatorActor(projection: RichProjection,
                                              version: Int,
                                              columnStore: ColumnStore,
                                              reprojector: Reprojector,
                                              config: Config,
                                              var reloadFlag: Boolean = false) extends BaseActor {
  import DatasetCoordinatorActor._
  import context.dispatcher

  val datasetName = projection.datasetName
  val nameVer = s"$datasetName/$version"
  // TODO: consider passing in a DatasetCoordinatorSettings class, so config doesn't have to be reparsed
  val maxRowsPerTable = config.getInt("memtable.max-rows-per-table")
  val flushTriggerRows = Math.min(config.getLong("memtable.flush-trigger-rows"), maxRowsPerTable.toLong)
  val minFreeMb = config.as[Option[Int]]("memtable.min-free-mb").getOrElse(DefaultMinFreeMb)
  val chunkSize = config.as[Option[Int]]("memtable.filo.chunksize").getOrElse(1000)
  val mTableWriteInterval = config.as[FiniteDuration]("memtable.write.interval")
  val mTableNoActivityFlushInterval = config.as[FiniteDuration]("memtable.noactivity.flush.interval")

  def activeRows: Int = activeTable.numRows
  def flushingRows: Int = flushingTable.map(_.numRows).getOrElse(-1)

  var activeTable: MemTable = makeNewTable
  var flushingTable: Option[MemTable] = None

  var curReprojection: Option[Future[Seq[SegmentInfo[_, _]]]] = None
  var flushesStarted = 0
  var flushesSucceeded = 0
  var flushesFailed = 0
  var rowsIngested = 0L

  // ==== Kamon metrics ====
  private val kamonTags = Map("dataset" -> datasetName, "version" -> version.toString)
  val kamonRowsIngested = Kamon.metrics.counter("dca-rows-ingested", kamonTags)
  val kamonActiveRows   = Kamon.metrics.gauge("dca-active-rows", kamonTags) { activeRows.toLong }
  val kamonFlushingRows = Kamon.metrics.gauge("dca-flushing-rows", kamonTags) {
    Math.max(flushingRows, 0).toLong
  }
  val kamonFlushesDone  = Kamon.metrics.counter("dca-flushes-done", kamonTags)
  val kamonFlushesFailed = Kamon.metrics.counter("dca-flushes-failed", kamonTags)
  val kamonOOMCount     = Kamon.metrics.counter("dca-out-of-memory", kamonTags)
  val kamonMemtableFull = Kamon.metrics.counter("dca-memtable-full", kamonTags)

  // Holds temporary rows before being flushed to MemTable
  val tempRows = new ArrayBuffer[RowReader]
  val ackTos = new ArrayBuffer[(ActorRef, Long)]
  val cannotIngest = new HashSet[ActorRef]
  var mTableWriteTask: Option[Cancellable] = None
  var mTableFlushTask: Option[Cancellable] = None

  val clusterSelfAddr = Cluster(context.system).selfAddress
  val actorPath = clusterSelfAddr.host.getOrElse("None")

  def makeNewTable(): MemTable = new FiloMemTable(projection, config, actorPath, version, reloadFlag)

  private def reportStats(): Unit = {
    logger.info(s"MemTable active table rows: $activeRows")
    logger.info(s"MemTable flushing rows: $flushingRows")
  }

  var flushedCallbacks: List[ActorRef] = Nil

  private def ingestRows(ackTo: ActorRef, rows: Seq[RowReader], seqNo: Long): Unit = {
    if (canIngest && !cannotIngest.contains(ackTo)) {
      mTableFlushTask.foreach(_.cancel)
      mTableFlushTask = Some(context.system.scheduler.scheduleOnce
        (mTableNoActivityFlushInterval,self, StartFlush(None)))

      tempRows ++= rows
      ackTos.append((ackTo, seqNo))
      if (tempRows.length >= chunkSize) {
        mTableWriteTask.foreach(_.cancel)
        writeToMemTable()
      } else if (!mTableWriteTask.isDefined) {
        // schedule future write to memtable, don't have enough rows yet
        mTableWriteTask = Some(context.system.scheduler.scheduleOnce(mTableWriteInterval, self, MemTableWrite))
      }
    } else {
      ackTo ! IngestionCommands.Nack(seqNo)
      logger.debug(s"Cannot ingest rows with seqNo=$seqNo")
      cannotIngest += ackTo
    }
  }

  private def writeToMemTable(): Unit = if (tempRows.nonEmpty) {
    logger.debug(s"Flushing ${tempRows.length} rows to MemTable...")
    rowsIngested += tempRows.length
    kamonRowsIngested.increment(tempRows.length)
    activeTable.ingestRows(tempRows)
    ackTos.foreach { case (ackTo, seqNo) => ackTo ! IngestionCommands.Ack(seqNo) }
    tempRows.clear()
    ackTos.clear()
    mTableWriteTask = None

    // Now, check how full the memtable is, and if it needs to be flipped, and a flush started
    // Note: running startFlush right away helps avoid situation where some ingest messages come in
    // between and get rejected prematurely while new memtable becomes available
    if (shouldFlush) startFlush()
  }

  private def canIngest: Boolean = {
    // TODO: gate on total rows in both active and flushing tables, not just active table?
    if (activeRows >= maxRowsPerTable) {
      logger.debug(s"MemTable ($nameVer) is at $activeRows rows, cannot accept writes...")
      kamonMemtableFull.increment
      false
    } else {
      val freeMB = getRealFreeMb
      if (freeMB < minFreeMb) {
        logger.info(s"Only $freeMB MB memory left, cannot accept more writes...")
        logger.info(s"Active table ($nameVer) has $activeRows rows")
        kamonOOMCount.increment
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

    if (curReprojection.isDefined || flushingTable.isDefined) {
      logger.warn(s"This should not happen. Not starting flush; $curReprojection; $flushingTable")
      return
    }
    writeToMemTable()
    reportStats()

    // Cancel any outstanding scheduled flushes
    mTableFlushTask.foreach(_.cancel)
    mTableFlushTask = None

    flushingTable = Some(activeTable)
    // Reset reload flag to create new WAL file whenever new Memtable is initialised.
    reloadFlag = false
    activeTable = makeNewTable()
    val newTaskFuture = reprojector.reproject(flushingTable.get, version)
    curReprojection = Some(newTaskFuture)
    flushesStarted += 1
    for { results <- newTaskFuture } self ! FlushDone(results)
    newTaskFuture.recover {
      case t: Throwable => self ! FlushFailed(t)
    }
    // Important: Tell RowSource/ingesters there is a new memtable for them to ingest
    for { ingester <- cannotIngest } ingester ! IngestionCommands.ResumeIngest
    logger.info(s" flush cycle for ($nameVer)... is complete")
  }

  private def shouldFlush: Boolean =
    activeRows >= flushTriggerRows && curReprojection == None

  private def handleFlushDone(): Unit = {
    curReprojection = None
    // delete wal files after flush completed
    flushingTable.foreach(_.deleteWalFiles())
    // Close the flushing table and mark it as None
    flushingTable.foreach(_.close())
    flushingTable = None
    for { ref <- flushedCallbacks } ref ! IngestionCommands.Flushed
    flushedCallbacks = Nil
    flushesSucceeded += 1
    kamonFlushesDone.increment
    // See if another flush needs to be initiated
    if (shouldFlush) self ! StartFlush()
  }

  private def handleFlushErr(t: Throwable): Unit = {
    flushedCallbacks.foreach { ref => ref ! FlushFailed(t) }
    flushedCallbacks = Nil
    flushesFailed += 1
    kamonFlushesFailed.increment
    // At this point, we can attempt a couple different things.
    //  1. Retry the segments that did not succeed writing (how do we know what they are?)
    //     This only makes sense for certain failures.
    //  2. Skip the segments in error.
    //  3. Fail and throw an exception, and let our supervisor actor deal with it.  This most likely
    //     means a loss of state and messages.
    // For now, just throw an error, and assume we can't proceed.
    // TODO: implement retry.
    throw t
  }

  private def clearProjection(originator: ActorRef, projection: Projection): Unit = {
    for { flushResult <- curReprojection.getOrElse(Future.successful(Nil))
          resp <- columnStore.clearProjectionData(projection) }
    { originator ! DatasetCommands.ProjectionTruncated }
  }

  def receive: Receive = LoggingReceive {
    case NewRows(ackTo, rows, seqNo) =>
      ingestRows(ackTo, rows, seqNo)

    case StartFlush(originator) =>
      originator.foreach { callbackRef => flushedCallbacks = flushedCallbacks :+ callbackRef }
      if (!curReprojection.isDefined) { startFlush() }
      else {
        logger.debug(s"Ignoring StartFlush, reprojection already in progress...")
        originator.foreach { _ ! IngestionCommands.FlushIgnored }
      }

    case ClearProjection(replyTo, projection) =>
      clearProjection(replyTo, projection)

    case CanIngest =>
      val can = canIngest
      // Make it look like this reply came from the NodeCoordinator
      sender.tell(IngestionCommands.CanIngest(can), context.parent)
      if (can) cannotIngest.remove(sender)

    case GetStats =>
      sender.tell(Stats(flushesStarted, flushesSucceeded, flushesFailed,
                        activeRows, flushingRows, rowsIngested), context.parent)

    case MemTableWrite =>
      writeToMemTable()

    case FlushDone(results) =>
      val resultStr = if (results.length < 32) { results.mkString(", ") }
                      else { s"${results.length} segments: [${results.take(3)} ... ${results.takeRight(3)}]" }
      logger.info(s"Reprojection task ($nameVer) succeeded: $resultStr")
      reportStats()
      handleFlushDone()

    case FlushFailed(t) =>
      logger.error(s"Error in reprojection task ($nameVer)", t)
      reportStats()
      handleFlushErr(t)

    case InitIngestion(replyTo) =>
      try {
        activeTable.reloadMemTable()
        replyTo ! DCAReady
      } catch {
        case ne: java.nio.file.NoSuchFileException =>
          logger.info("There are no WAL files exist for this dataset and will proceed to start ingestion")
          // Send DCAReady message When no WAL files exist for this dataset
          replyTo ! DCAReady
      }
  }
}

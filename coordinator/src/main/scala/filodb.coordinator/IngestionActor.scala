package filodb.coordinator

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler, UncaughtExceptionReporter}
import monix.execution.schedulers.SchedulerService
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.core.{DatasetRef, GlobalConfig, Iterators}
import filodb.core.downsample.{DownsampleConfig, DownsampledTimeSeriesStore}
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.metrics.FilodbMetrics
import filodb.core.store.StoreConfig
import filodb.memory.data.Shutdown

object IngestionActor {
  final case class IngestRows(ackTo: ActorRef, shard: Int, records: SomeData)

  case object GetStatus

  final case class IngestionStatus(rowsIngested: Long)

  /**
   * Calculates the recovery progress percentage based on the current recursion depth, max recursion depth,
   * recovered offset, start offset and end offset.
   * For Example: if recursionDepth = 1 and maxRecursionDepth = 3, then the progress percentage will be calculated as:
   * If recursionDepth == 1, then recovery would be between [0, 33]
   * If recursionDepth == 2, then recovery would be between (33, 66]
   * If recursionDepth == 3, then recovery would be between (66, 100]
   * @return the recovery progress percentage
   */
  def getRecoveryProgressPercentage(currentRecursionDepth: Int, maxRecursionDepth: Int,
                                    recoveredOffset: Long, startOffset: Long, endOffset: Long): Int = {
    val maxProgressPerIteration = 100 / maxRecursionDepth
    if (recoveredOffset >= endOffset && currentRecursionDepth == maxRecursionDepth) { 100 }
    else if ( (recoveredOffset >= endOffset) || (endOffset == startOffset) ) {
      currentRecursionDepth * maxProgressPerIteration
    }
    else (((recoveredOffset - startOffset) * maxProgressPerIteration) / (endOffset - startOffset)).toInt +
      (currentRecursionDepth - 1) * maxProgressPerIteration
  }

  def props(ref: DatasetRef,
            schemas: Schemas,
            memStore: TimeSeriesStore,
            source: NodeClusterActor.IngestionSource,
            downsample: DownsampleConfig,
            storeConfig: StoreConfig,
            numShards: Int,
            statusActor: ActorRef): Props =
    Props(new IngestionActor(ref, schemas, memStore, source, downsample, storeConfig, numShards, statusActor))
}

/**
 * Parameters for the IngestionActor's recovery process.
 * @param currentRecursionDepth the current depth of recursion
 * @param maxRecursionDepth the maximum depth of recursion
 * @param shard the shard number to start recovery on
 * @param startOffset the starting offset to begin recovery
 * @param maxCheckpointOffset the max offset among the last flushed checkpoints for the shard
 * @param checkpoints the existing checkpoints for the shard
 */
case class RecoveryParams(currentRecursionDepth: Int,
                          maxRecursionDepth: Int,
                          shard: Int,
                          startOffset: Long,
                          maxCheckpointOffset: Long,
                          checkpoints: Map[Int, Long])

/**
  * Oversees ingestion and recovery process for a single dataset.  The overall process for a single shard:
  * 1. Shard state command is received and start() called
  * 2. MemStore.setup() is called for that shard
  * 3. IF no checkpoint data is found, THEN normal ingestion is started
  * 4. IF checkpoints are found, then recovery is started from the minimum checkpoint offset
  *    and goes until the maximum checkpoint offset.  These offsets are per subgroup of the shard.
  *    Progress will be sent at regular intervals
  * 5. Once the recovery has proceeded beyond the end checkpoint then normal ingestion is started
  *
  * ERROR HANDLING: currently any error in ingestion stream or memstore ingestion wll stop the ingestion
  *
  * @param storeConfig IngestionConfig.storeConfig, the store section of the source configuration
  * @param statusActor the actor to which to forward ShardEvents for status updates
  */
private[filodb] final class IngestionActor(ref: DatasetRef,
                                           schemas: Schemas,
                                           tsStore: TimeSeriesStore,
                                           source: NodeClusterActor.IngestionSource,
                                           downsample: DownsampleConfig,
                                           storeConfig: StoreConfig,
                                           numShards: Int,
                                           statusActor: ActorRef) extends BaseActor {

  import IngestionActor._

  final val streamSubscriptions = new ConcurrentHashMap[Int, CancelableFuture[Unit]].asScala
  final val streams = new ConcurrentHashMap[Int, IngestionStream].asScala
  final val nodeCoord = context.parent
  var shardStateVersion: Long = 0

  // Params for creating the default memStore flush scheduler
  private final val numGroups = storeConfig.groupsPerShard

  val actorDispatcher = context.dispatcher

  // The flush task has very little work -- pretty much none. Looking at doFlushSteps, you can see that
  // all the heavy lifting -- including chunk encoding, forming the (potentially big) index time-bucket blobs --
  // is all done in the ingestion thread. Even the futures used to do I/O will not be done in this flush thread...
  // they are allocated by the implicit ExecutionScheduler that Futures use and/or what C* etc. uses.
  // The only thing that flushSched really does is tie up all these Futures together.
  val flushSched = Scheduler.computation(
    name = FiloSchedulers.FlushSchedName,
    reporter = UncaughtExceptionReporter(logger.error("Uncaught Exception in Flush Scheduler", _)))

  // TODO: add and remove per-shard ingestion sources?
  // For now just start it up one time and kill the actor if it fails
  val ctor = Class.forName(source.streamFactoryClass).getConstructors.head
  val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]
  logger.info(s"Using stream factory $streamFactory with config ${source.config}, storeConfig $storeConfig")

  val shutdownAfterStop = source.config.as[Option[Boolean]]("shutdown-ingest-after-stopped").getOrElse(true)

  override def postStop(): Unit = {
    super.postStop() // <- logs shutting down
    logger.info(s"Cancelling all streams and calling teardown for dataset=$ref")
    streamSubscriptions.keys.foreach(stopIngestion)
  }

  def receive: Receive = LoggingReceive {
    case GetStatus               => status(sender())
    case e: IngestRows           => ingest(e)
    case e: ShardIngestionState  => resync(e, sender())
    case e: IngestionStopped     => ingestionStopped(e.ref, e.shard)
  }

  /**
    * Compares the given shard mapper snapshot to the current set of shards being ingested and
    * reconciles any differences. It does so by stopping ingestion for shards that aren't mapped
    * to this node, and it starts ingestion for those that are.
    */
  // scalastyle:off method.length
  private def resync(state: ShardIngestionState, origin: ActorRef): Unit = {
    if (invalid(state.ref)) {
      logger.error(s"$state is invalid for this ingester '$ref'.")
      return
    }

    if (state.version != 0 && state.version <= shardStateVersion) {
      logger.info(s"Ignoring old ShardIngestionState version: ${state.version} <= $shardStateVersion " +
        s"for dataset=$ref")
      return
    }

    // Start with the full set of all shards being ingested, and remove shards from this set
    // which must continue being ingested.
    val shardsToStop = mutable.HashSet() ++ streams.keySet

    for (shard <- 0 until state.map.numShards) {
      if (state.map.coordForShard(shard) == context.parent) {
        if (state.map.isAnIngestionState(shard) || !shutdownAfterStop) {
          if (shardsToStop.contains(shard)) {
            // Is aready ingesting, and it must not be stopped.
            shardsToStop.remove(shard)
          } else {
            try {
              // Isn't ingesting, so start it.
              startIngestion(shard)
            } catch {
              case t: Throwable =>
                logger.error(s"Error occurred during initialization of ingestion for " +
                  s"dataset=$ref shard=${shard}", t)
                handleError(ref, shard, t)
            }
          }
        } else {
          val status = state.map.statuses(shard)
          if (shardsToStop.contains(shard)) {
            logger.info(s"Will stop ingestion for dataset=$ref shard=$shard due to status ${status}")
          } else {
            // Already stopped. Send the message again in case it got dropped.
            logger.info(s"Stopping ingestion again for dataset=$ref shard=$shard due to status ${status}")
            sendStopMessage(shard)
          }
        }
      }
    }

    // Stop ingesting the rest.
    for (shard <- shardsToStop) {
      stopIngestion(shard)
    }

    if (state.version != 0) {
      shardStateVersion = state.version
    }
  }

  // scalastyle:off method.length
  private def startIngestion(shard: Int): Unit = {
    try tsStore.setup(ref, schemas, shard, storeConfig, numShards, downsample) catch {
      case ShardAlreadySetup(ds, s) =>
        logger.warn(s"dataset=$ds shard=$s already setup, skipping....")
        return
    }

    implicit val futureMapDispatcher: ExecutionContext = actorDispatcher
    val ingestion = if (tsStore.isDownsampleStore) {
      logger.info(s"Initiating shard startup on read-only memstore for dataset=$ref shard=$shard")
      for {
        _ <- tsStore.recoverIndex(ref, shard)
      } yield {
        // bring shard to active state by sending this message - this code path won't invoke normalIngestion
        statusActor ! IngestionStarted(ref, shard, nodeCoord)
        streamSubscriptions(shard) = CancelableFuture.never // simulate ingestion happens continuously
        streams(shard) = IngestionStream(Observable.never)
      }
    } else {
      logger.info(s"Initiating ingestion for dataset=$ref shard=$shard")
      logger.info(s"Metastore is ${tsStore.metastore}")
      for {
        _ <- tsStore.recoverIndex(ref, shard)
        checkpoints <- tsStore.metastore.readCheckpoints(ref, shard)
      } yield {
        if (checkpoints.isEmpty) {
          logger.info(s"No checkpoints were found for dataset=$ref shard=$shard -- skipping kafka recovery")
          // Start normal ingestion with no recovery checkpoint and flush group 0 first
          normalIngestion(shard, None, 0)
        } else {
          // Figure out recovery end watermark and intervals.  The reportingInterval is the interval at which
          // offsets come back from the MemStore for us to report progress.
          val startRecoveryWatermark = checkpoints.values.min + 1
          val endRecoveryWatermark = checkpoints.values.max
          val lastFlushedGroup = checkpoints.find(_._2 == endRecoveryWatermark).get._1
          logger.info(s"Checkpoints=$checkpoints for dataset=$ref shard=$shard! lastFlushedGroup=$lastFlushedGroup")
          GlobalConfig.indexRecoveryMaxDepth.isDefined match {
            case true => {
              val maxRecursionDepth = GlobalConfig.indexRecoveryMaxDepth.get
              logger.info(s"[RecoverIndex] Using recursive strategy to fetch endOffset from IngestionStream!" +
                s" dataset=$ref shard=$shard startOffset=$startRecoveryWatermark maxRecursionDepth=$maxRecursionDepth")

              // NOTE: Scheduling the ingestion stream recovery task on the ingestion thread
              val ingestionScheduler = tsStore.asInstanceOf[TimeSeriesMemStore].getShardE(ref, shard).ingestSched
              // start with first recursion depth
              val params = RecoveryParams(
                currentRecursionDepth = 1, maxRecursionDepth = maxRecursionDepth, shard = shard,
                startOffset = startRecoveryWatermark, maxCheckpointOffset = endRecoveryWatermark,
                checkpoints = checkpoints)
              for {
                lastOffset <- doRecoveryWithShardRecoveryLatencyTracking(params, ingestionScheduler)
              } yield {
                // NOTE: if the future failed, yield block will not be executed and the shard state will remain in error
                // Start reading past last offset for normal records; start flushes one group past last group
                normalIngestion(shard, Some(lastOffset.getOrElse(endRecoveryWatermark) + 1),
                  (lastFlushedGroup + 1) % numGroups)
              }
            }
            case false => {
              logger.info(s"[RecoverIndex] Using legacy strategy to start recovery for dataset=$ref " +
                s"shard=$shard from $startRecoveryWatermark to $endRecoveryWatermark")
              val reportingInterval = Math.max((endRecoveryWatermark - startRecoveryWatermark) / 20, 1L)
              for {
                lastOffset <- doRecovery(shard, startRecoveryWatermark, endRecoveryWatermark, reportingInterval,
                  checkpoints)
              }
              yield {
                // Start reading past last offset for normal records; start flushes one group past last group
                normalIngestion(shard, Some(lastOffset.getOrElse(endRecoveryWatermark) + 1),
                  (lastFlushedGroup + 1) % numGroups)
              }
            }
          }
        }
      }
    }

    ingestion.recover {
      case NonFatal(t) =>
        logger.error(s"Error occurred during initialization/execution of ingestion for " +
          s"dataset=$ref shard=$shard", t)
        handleError(ref, shard, t)
    }
  }

  /**
   * Initiates post-recovery ingestion and regular flushing from the source.
   * startingGroupNo and offset would be defined for recovery scenarios.
   * @param shard the shard number to start ingestion
   * @param offset optionally the offset to start ingestion at
   * @param startingGroupNo the group number to start flushes at
   */
  private def normalIngestion(shard: Int,
                              offset: Option[Long],
                              startingGroupNo: Int): Unit = {
    create(shard, offset) map { ingestionStream =>
      val stream = ingestionStream.get
      logger.info(s"Starting normal/active ingestion for dataset=$ref shard=$shard at offset $offset")
      statusActor ! IngestionStarted(ref, shard, nodeCoord)

      // Define a cancel task to run when ingestion is stopped.
      val onCancel = Task {
        logger.info(s"Ingestion cancel task invoked for dataset=$ref shard=$shard")
        sendStopMessage(shard)
      }

      val shardIngestionEnd = tsStore.startIngestion(ref,
        shard,
        stream,
        flushSched,
        onCancel)
      // On completion of the future, send IngestionStopped
      // except for noOpSource, which would stop right away, and is used for sending in tons of data
      // also: small chance for race condition here due to remove call in stop() method
      shardIngestionEnd.onComplete {
        case Failure(x) =>
          handleError(ref, shard, x)
        case Success(_) =>
          logger.info(s"IngestStream onComplete.Success invoked for dataset=$ref shard=$shard")
          // We don't release resources when finite ingestion ends normally.
          // Kafka ingestion is usually infinite and does not end unless canceled.
          // Cancel operation is already releasing after cancel is done.
          // We also have some tests that validate after finite ingestion is complete
          if (source != NodeClusterActor.noOpSource) statusActor ! IngestionStopped(ref, shard)
      }(actorDispatcher)
      streamSubscriptions(shard) = shardIngestionEnd
    } recover { case t: Throwable =>
      logger.error(s"Error occurred when setting up ingestion pipeline for dataset=$ref shard=$shard ", t)
      handleError(ref, shard, t)
    }
  }

  private def sendStopMessage(shard: Int): Unit = {
    val stopped = IngestionStopped(ref, shard)
    self ! stopped
    statusActor ! stopped
  }

  import Iterators._

  /**
   * Initiates the recovery process for the given shard and tracks the total latency of the recovery process.
   * @param params the parameters for the recovery process
   * @param ingestionScheduler the scheduler to run the recovery process
   * @return the last offset read during recovery process
   */
  private def doRecoveryWithShardRecoveryLatencyTracking(params: RecoveryParams,
                                                         ingestionScheduler: SchedulerService): Future[Option[Long]] = {
    implicit val futureMapDispatcher: ExecutionContext = ingestionScheduler
    val ingestionRecoveryLatency = FilodbMetrics.timeHistogram("ingestion-recovery-latency", TimeUnit.MILLISECONDS,
      Map("dataset" -> ref.dataset, "shard" -> params.shard.toString))
    val recoveryStart = System.currentTimeMillis()
    statusActor ! RecoveryInProgress(ref, params.shard, nodeCoord, 0)
    val fut = doRecoveryRecursive(params, ingestionScheduler)
    // Make sure we track the ingestion recovery latency on both success and failure scenario
    fut.onComplete(_ => ingestionRecoveryLatency.record(System.currentTimeMillis() - recoveryStart))
    fut
  }

  /**
   * Recursively fetch the last offset from the ingestion-stream and ingest data until the offset until the
   *    maxRecursionDepth is reached. The max depth is configurable and is used to prevent infinite recursion.

   * NOTE: on sucess path, we force the kafka consumer to be closed before the next recursive call happens. This ensures
   * proper clean up of kafka resources, before the next recovery iteration starts.
   *
   * @param params the parameters for the recovery process
   * @param ingestionScheduler the scheduler to run the recovery process
   * @return the last offset read during recovery process
   */
  private def doRecoveryRecursive(params: RecoveryParams,
                                  ingestionScheduler: SchedulerService): Future[Option[Long]] = {
    implicit val futureMapDispatcher: ExecutionContext = ingestionScheduler
    logger.info(s"[RecoverIndex] doRecoveryRecursive called for dataset=$ref shard=${params.shard} " +
      s"startOffset=${params.startOffset} currentRecursionDepth=${params.currentRecursionDepth}")
    // NOTE: Why are we creating the ingestion stream for each iteration ? This is because the monix KCO object
    // internally issues a cancel call during the end of the iteration when the last offset is reached during
    // createDataRecoveryObservable. This results in `IllegalStateException` if we try to reuse the same stream. Hence,
    // we are re-creating the stream for each iteration.
    val futTry = create(params.shard, Some(params.startOffset)) map { ingestionStream =>
      doRecoveryWithIngestionStreamEndOffset(params, ingestionStream, ingestionScheduler)
        .andThen {
          case Success(_) =>
            ingestionStream.teardown(isForced = true)
            streams.remove(params.shard)
        case Failure(ex) =>
          handleError(ref, params.shard, ex)
        }
        .flatMap {
          // NOTE: Avoid recursing if the lastOffset was not greater than startOffset, i.e. end of stream is reached
          case Some(lastOffset) =>
            if (params.currentRecursionDepth < params.maxRecursionDepth && lastOffset > params.startOffset) {
            // lastOffset is already ingested. Reading from the next offset i.e. lastOffset + 1
            val updatedParamsForNextIteration = params.copy(
              startOffset = lastOffset + 1, currentRecursionDepth = params.currentRecursionDepth + 1)
            doRecoveryRecursive(updatedParamsForNextIteration, ingestionScheduler)
          } else {
            statusActor ! RecoveryInProgress(ref, params.shard, nodeCoord, 100)
            logger.info(s"[RecoverIndex] Recovery completed for dataset=$ref shard=${params.shard} " +
              s". last-offset-read=$lastOffset ")
            Future.successful(Some(lastOffset))
          }
          case None =>
            // This should never happen in normal flow. But if it does, we should fail the recovery process and
            // let shard transition into error state
            logger.error(s"[RecoverIndex] doRecoveryWithIngestionStreamEndOffset returned None! Recovery failed for" +
              s" dataset=$ref shard=${params.shard}. currentRecursionDepth=${params.currentRecursionDepth}")
            val ex = new IllegalStateException(
              "[RecoverIndex] Recovery failed with None offset from IngestionStream. " +
                s"currentRecursionDepth=${params.currentRecursionDepth} dataset=$ref shard=${params.shard}")
            handleError(ref, params.shard, ex)
            Future.failed(ex)
        }
    }
    futTry.recover { case NonFatal(t) =>
      handleError(ref, params.shard, t)
      Future.failed(t)
    }
    futTry.get
  }

  /**
   * Fetches the current endOffset from the ingestion stream and starts the ingestion of data until the endOffset
   * is reached. On failure or completion, the ingestion stream is removed from the shard state, for next iteration.
   * @return the last offset read during recovery process
   */
  private def doRecoveryWithIngestionStreamEndOffset(params: RecoveryParams, ingestionStream: IngestionStream,
                                                     ingestionScheduler: SchedulerService): Future[Option[Long]] = {
    val stream = ingestionStream.get
    // getting the current last offset from the ingestion stream
    // if we are unable to get the last offset, default to the maxCheckpointOffset and recover until it
    val endStreamOffsetOption = ingestionStream.endOffset
    val endStreamOffset = endStreamOffsetOption match {
      case None =>
        logger.error(s"[RecoverIndex] Unable to get end offset from stream for dataset=$ref shard=${params.shard}")
        // NOTE: Since this call is made recursively, we need to make sure we are always looking at the greater value
        Math.max(params.maxCheckpointOffset, params.startOffset)
      case Some(off) => off
    }
    if (endStreamOffset <= params.startOffset) {
      logger.info(s"[RecoverIndex] Already reached the endOffset=$endStreamOffset! dataset=$ref shard=${params.shard}")
      Future.successful(Some(endStreamOffset))
    }
    else {
      val reportingInterval = Math.max((endStreamOffset - params.startOffset) / 20, 1L)
      logger.info(s"[RecoverIndex] Starting recovery for dataset=$ref shard=${params.shard} " +
        s"from ${params.startOffset} to $endStreamOffset with interval $reportingInterval")

      tsStore.createDataRecoveryObservable(
          ref, params.shard, stream, params.startOffset, endStreamOffset, params.checkpoints, reportingInterval)
        .map { off =>
          val progressPct = getRecoveryProgressPercentage(params.currentRecursionDepth, params.maxRecursionDepth,
            off, params.startOffset, endStreamOffset)

          logger.info(s"[RecoverIndex]Recovery of dataset=$ref shard=${params.shard} at " +
            s"$progressPct % - offset $off (target $endStreamOffset); " +
            s"${(off - params.startOffset)/(endStreamOffset - params.startOffset)}% of iteration=" +
            s"${params.currentRecursionDepth + 1 / params.maxRecursionDepth}")

          statusActor ! RecoveryInProgress(ref, params.shard, nodeCoord, progressPct)
          off
        }
        .until(_ >= endStreamOffset)
        .lastOptionL.runToFuture(ingestionScheduler)
    }
  }

  /**
   * Starts the recovery stream; returns the last offset read during recovery process
   * Periodically (every interval offsets) reports recovery progress
   * This stream is optimized for recovery; no flushes or other write I/O is performed.
   * @param shard the shard number to start recovery on
   * @param startOffset the starting offset to begin recovery
   * @param endOffset the offset past which recovery should stop (approximately)
   * @param interval the interval of reporting progress
   */
  private def doRecovery(shard: Int, startOffset: Long, endOffset: Long, interval: Long,
                         checkpoints: Map[Int, Long]): Future[Option[Long]] = {
    val futTry = create(shard, Some(startOffset)) map { ingestionStream =>

      val ingestionRecoveryLatency = FilodbMetrics.timeHistogram("ingestion-recovery-latency", TimeUnit.MILLISECONDS,
                                                  Map("dataset" -> ref.dataset, "shard" -> shard.toString))

      val recoveryStart = System.currentTimeMillis()
      val stream = ingestionStream.get
      statusActor ! RecoveryInProgress(ref, shard, nodeCoord, 0)
      val shardInstance = tsStore.asInstanceOf[TimeSeriesMemStore].getShardE(ref, shard)
      val fut = tsStore.createDataRecoveryObservable(
          ref, shard, stream, startOffset, endOffset, checkpoints, interval)
        .map { off =>
          val progressPct = if (endOffset - startOffset == 0) 100
                            else (off - startOffset) * 100 / (endOffset - startOffset)
          logger.info(s"Recovery of dataset=$ref shard=$shard at " +
            s"$progressPct % - offset $off (target $endOffset)")
          statusActor ! RecoveryInProgress(ref, shard, nodeCoord, progressPct.toInt)
          off }
        .until(_ >= endOffset)
        // TODO: move this code to TimeSeriesShard itself.  Shard should control the thread
        .lastOptionL.runToFuture(shardInstance.ingestSched)
      fut.onComplete {
        case Success(_) =>
          logger.info(s"Finished recovery for dataset=$ref shard=$shard")
          ingestionStream.teardown()
          streams.remove(shard)
          ingestionRecoveryLatency.record(System.currentTimeMillis() - recoveryStart)
        case Failure(ex) =>
          logger.error(s"Recovery failed for dataset=$ref shard=$shard", ex)
          handleError(ref, shard, ex)
          ingestionRecoveryLatency.record(System.currentTimeMillis() - recoveryStart)
      }(actorDispatcher)
      fut
    }
    futTry.recover { case NonFatal(t) =>
      handleError(ref, shard, t)
      Future.failed(t)
    }
    futTry.get
  }

  /** [[filodb.coordinator.IngestionStreamFactory.create]] can raise IllegalArgumentException
    * if the shard is not 0. This will notify versus throw so the sender can handle the
    * problem, which is internal.
    * NOTE: this method will synchronously retry so it make take a long time.
    */
  private def create(shard: Int, offset: Option[Long],
                     retries: Int = storeConfig.failureRetries): Try[IngestionStream] =
    Try {
      val ingestStream = streamFactory.create(source.config, schemas, shard, offset)
      streams(shard) = ingestStream
      logger.info(s"Ingestion stream $ingestStream set up for dataset=$ref shard=$shard")
      ingestStream
    }.recoverWith {
      case e: Exception if retries > 0 =>
        logger.warn(s"IngestionStream creation got [$e], $retries retries left.  Waiting then retrying", e)
        Thread sleep storeConfig.retryDelay.toMillis
        create(shard, offset, retries - 1)
      case e: Exception =>
        logger.error(s"IngestionStream creation got [$e], out of retries, shard will stop", e)
        Failure(e)
    }

  private def ingest(e: IngestRows): Unit = {
    tsStore.ingest(ref, e.shard, e.records)
    if (!e.records.records.isEmpty) {
      e.ackTo ! client.IngestionCommands.Ack(e.records.offset)
    }
  }

  private def status(origin: ActorRef): Unit =
    origin ! IngestionStatus(tsStore.numRowsIngested(ref))

  private def stopIngestion(shard: Int): Unit = {
    // When the future is canceled, the onCancel task installed earlier should run.
    logger.info(s"stopIngestion called for dataset=$ref shard=$shard")
    streamSubscriptions.get(shard).foreach(_.cancel())
  }

  private def ingestionStopped(ref: DatasetRef, shard: Int): Unit = {
    removeAndReleaseResources(ref, shard)
    logger.info(s"stopIngestion handler done. Ingestion success for dataset=$ref shard=$shard")
  }

  private def invalid(dsRef: DatasetRef): Boolean = dsRef != ref

  private def handleError(ref: DatasetRef, shard: Int, err: Throwable): Unit = {
    logger.error(s"Exception thrown during ingestion stream for dataset=$ref shard=$shard." +
      s" Stopping ingestion", err)
    removeAndReleaseResources(ref, shard)
    statusActor ! IngestionError(ref, shard, err)
    logger.error(s"Stopped dataset=$ref shard=$shard after error was thrown")
    // This is uncommon. Instead of having other shards be reassigned to other nodes, fail fast and shutdown
    // the node and force all shards to be reassigned to new nodes of the cluster.
    Shutdown.haltAndCatchFire(err)
  }

  private def removeAndReleaseResources(ref: DatasetRef, shardNum: Int): Unit = {
    // TODO: Wait for all the queries to stop
    streamSubscriptions.remove(shardNum).foreach(_.cancel)
    streams.remove(shardNum).foreach(_.teardown())
    // Release memory for shard in MemStore

    tsStore match {
      case ro: DownsampledTimeSeriesStore => ro.getShard(ref, shardNum)
                                            .foreach { shard =>
                                              shard.shutdown()
                                              ro.removeShard(ref, shardNum, shard)
                                            }

      case m: TimeSeriesMemStore => m.getShard(ref, shardNum)
                                      .foreach { shard =>
                                        shard.shutdown()
                                        m.removeShard(ref, shardNum, shard)
                                      }
    }
    logger.info(s"Released resources for dataset=$ref shard=$shardNum")
  }
}

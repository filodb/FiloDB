package filodb.coordinator

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import kamon.Kamon
import monix.eval.Task
import monix.execution.{CancelableFuture, Scheduler, UncaughtExceptionReporter}
import net.ceedubs.ficus.Ficus._

import filodb.core.{DatasetRef, Iterators}
import filodb.core.downsample.{DownsampleConfig, DownsampledTimeSeriesStore}
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.store.StoreConfig

object IngestionActor {
  final case class IngestRows(ackTo: ActorRef, shard: Int, records: SomeData)

  case object GetStatus

  final case class IngestionStatus(rowsIngested: Long)

  def props(ref: DatasetRef,
            schemas: Schemas,
            memStore: MemStore,
            source: NodeClusterActor.IngestionSource,
            downsample: DownsampleConfig,
            storeConfig: StoreConfig,
            statusActor: ActorRef): Props =
    Props(new IngestionActor(ref, schemas, memStore, source, downsample, storeConfig, statusActor))
}

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
                                           memStore: MemStore,
                                           source: NodeClusterActor.IngestionSource,
                                           downsample: DownsampleConfig,
                                           storeConfig: StoreConfig,
                                           statusActor: ActorRef) extends BaseActor {

  import IngestionActor._

  final val streamSubscriptions = (new ConcurrentHashMap[Int, CancelableFuture[Unit]]).asScala
  final val streams = (new ConcurrentHashMap[Int, IngestionStream]).asScala
  final val nodeCoord = context.parent
  var shardStateVersion: Long = 0

  // Params for creating the default memStore flush scheduler
  private final val numGroups = storeConfig.groupsPerShard

  val actorDispatcher = context.dispatcher

  // The flush task has very little work -- pretty much none. Looking at doFlushSteps, you can see that
  // all of the heavy lifting -- including chunk encoding, forming the (potentially big) index timebucket blobs --
  // is all done in the ingestion thread. Even the futures used to do I/O will not be done in this flush thread...
  // they are allocated by the implicit ExecutionScheduler that Futures use and/or what C* etc uses.
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
    streamSubscriptions.keys.foreach(stopIngestion(_))
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
    val shardsToStop = HashSet() ++ streams.keySet

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
          logger.info(s"Will stop ingestion of for dataset=$ref shard=$shard due to status ${status}")
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
    try memStore.setup(ref, schemas, shard, storeConfig, downsample) catch {
      case ShardAlreadySetup(ds, shard) =>
        logger.warn(s"dataset=$ds shard=$shard already setup, skipping....")
        return
    }

    logger.info(s"Initiating ingestion for dataset=$ref shard=${shard}")
    logger.info(s"Metastore is ${memStore.metastore}")
    implicit val futureMapDispatcher = actorDispatcher
    val ingestion = if (memStore.isReadOnly) {
      for {
        _ <- memStore.recoverIndex(ref, shard)
      } yield {
        streamSubscriptions(shard) = CancelableFuture.never // simulate ingestion happens continuously
      }
    } else {
      for {
        _ <- memStore.recoverIndex(ref, shard)
        checkpoints <- memStore.metastore.readCheckpoints(ref, shard)
      } yield {
        if (checkpoints.isEmpty) {
          logger.info(s"No checkpoints were found for dataset=$ref shard=${shard} -- skipping kafka recovery")
          // Start normal ingestion with no recovery checkpoint and flush group 0 first
          normalIngestion(shard, None, 0)
        } else {
          // Figure out recovery end watermark and intervals.  The reportingInterval is the interval at which
          // offsets come back from the MemStore for us to report progress.
          val startRecoveryWatermark = checkpoints.values.min + 1
          val endRecoveryWatermark = checkpoints.values.max
          val lastFlushedGroup = checkpoints.find(_._2 == endRecoveryWatermark).get._1
          val reportingInterval = Math.max((endRecoveryWatermark - startRecoveryWatermark) / 20, 1L)
          logger.info(s"Starting recovery for dataset=$ref " +
            s"shard=${shard} from $startRecoveryWatermark to $endRecoveryWatermark ; " +
            s"last flushed group $lastFlushedGroup")
          logger.info(s"Checkpoints for dataset=$ref shard=${shard} were $checkpoints")
          for {lastOffset <- doRecovery(shard, startRecoveryWatermark, endRecoveryWatermark, reportingInterval,
            checkpoints)}
            yield {
              // Start reading past last offset for normal records; start flushes one group past last group
              normalIngestion(shard, Some(lastOffset.getOrElse(endRecoveryWatermark) + 1),
                (lastFlushedGroup + 1) % numGroups)
            }
        }
      }
    }

    ingestion.recover {
      case NonFatal(t) =>
        logger.error(s"Error occurred during initialization/execution of ingestion for " +
          s"dataset=$ref shard=${shard}", t)
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
        logger.info(s"Ingstion cancel task invoked for dataset=$ref shard=$shard")
        val stopped = IngestionStopped(ref, shard)
        self ! stopped
        statusActor ! stopped
      }

      val shardIngestionEnd = memStore.ingestStream(ref,
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
          // We dont release resources when finite ingestion ends normally.
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

  import Iterators._

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
      val recoveryTrace = Kamon.buildSpan("ingestion-recovery-trace")
                               .withTag("shard", shard.toString)
                               .withTag("dataset", ref.toString).start()
      val stream = ingestionStream.get
      statusActor ! RecoveryInProgress(ref, shard, nodeCoord, 0)

      val shardInstance = memStore.asInstanceOf[TimeSeriesMemStore].getShardE(ref, shard)
      val fut = memStore.recoverStream(ref, shard, stream, startOffset, endOffset, checkpoints, interval)
        .map { off =>
          val progressPct = if (endOffset - startOffset == 0) 100
                            else (off - startOffset) * 100 / (endOffset - startOffset)
          logger.info(s"Recovery of dataset=$ref shard=$shard at " +
            s"$progressPct % - offset $off (target $endOffset)")
          statusActor ! RecoveryInProgress(ref, shard, nodeCoord, progressPct.toInt)
          off }
        .until(_ >= endOffset)
        // TODO: move this code to TimeSeriesShard itself.  Shard should control the thread
        .lastOptionL.runAsync(shardInstance.ingestSched)
      fut.onComplete {
        case Success(_) =>
          logger.info(s"Finished recovery for dataset=$ref shard=$shard")
          ingestionStream.teardown()
          streams.remove(shard)
          recoveryTrace.finish()
        case Failure(ex) =>
          recoveryTrace.addError(s"Recovery failed for dataset=$ref shard=$shard", ex)
          logger.error(s"Recovery failed for dataset=$ref shard=$shard", ex)
          handleError(ref, shard, ex)
          recoveryTrace.finish()
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
    memStore.ingest(ref, e.shard, e.records)
    if (!e.records.records.isEmpty) {
      e.ackTo ! client.IngestionCommands.Ack(e.records.offset)
    }
  }

  private def status(origin: ActorRef): Unit =
    origin ! IngestionStatus(memStore.numRowsIngested(ref))

  private def stopIngestion(shard: Int): Unit = {
    // When the future is canceled, the onCancel task installed earlier should run.
    logger.info(s"stopIngestion called for dataset=$ref shard=$shard")
    streamSubscriptions.get(shard).foreach(_.cancel())
  }

  private def ingestionStopped(ref: DatasetRef, shard: Int): Unit = {
    removeAndReleaseResources(ref, shard)
    logger.info(s"stopIngestion handler done. Ingestion success for dataset=${ref} shard=$shard")
  }

  private def invalid(dsRef: DatasetRef): Boolean = dsRef != ref

  private def handleError(ref: DatasetRef, shard: Int, err: Throwable): Unit = {
    logger.error(s"Exception thrown during ingestion stream for dataset=$ref shard=$shard." +
      s" Stopping ingestion", err)
    removeAndReleaseResources(ref, shard)
    statusActor ! IngestionError(ref, shard, err)
    logger.error(s"Stopped dataset=$ref shard=$shard after error was thrown")
  }

  private def removeAndReleaseResources(ref: DatasetRef, shardNum: Int): Unit = {
    // TODO: Wait for all the queries to stop
    streamSubscriptions.remove(shardNum).foreach(_.cancel)
    streams.remove(shardNum).foreach(_.teardown())
    // Release memory for shard in MemStore

    memStore match {
      case ro: DownsampledTimeSeriesStore => ro.getShard(ref, shardNum)
                                            .foreach { shard =>
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

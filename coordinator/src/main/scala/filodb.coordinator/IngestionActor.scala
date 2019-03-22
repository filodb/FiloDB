package filodb.coordinator

import scala.collection.mutable.{HashMap, HashSet}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import akka.actor.{ActorRef, Props}
import akka.event.LoggingReceive
import kamon.Kamon
import monix.execution.CancelableFuture
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._

import filodb.core.{DatasetRef, GlobalScheduler, Iterators}
import filodb.core.downsample.DownsampleConfig
import filodb.core.memstore._
import filodb.core.metadata.Dataset
import filodb.core.store.StoreConfig

object IngestionActor {
  final case class IngestRows(ackTo: ActorRef, shard: Int, records: SomeData)

  case object GetStatus

  final case class IngestionStatus(rowsIngested: Long)

  def props(dataset: Dataset,
            memStore: MemStore,
            source: NodeClusterActor.IngestionSource,
            downsample: DownsampleConfig,
            storeConfig: StoreConfig,
            statusActor: ActorRef): Props =
    Props(new IngestionActor(dataset, memStore, source, downsample, storeConfig, statusActor))
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
private[filodb] final class IngestionActor(dataset: Dataset,
                                           memStore: MemStore,
                                           source: NodeClusterActor.IngestionSource,
                                           downsample: DownsampleConfig,
                                           storeConfig: StoreConfig,
                                           statusActor: ActorRef) extends BaseActor {

  import IngestionActor._

  final val streamSubscriptions = new HashMap[Int, CancelableFuture[Unit]]
  final val streams = new HashMap[Int, IngestionStream]
  private var shardStateVersion: Long = 0

  // Params for creating the default memStore flush scheduler
  private final val numGroups = storeConfig.groupsPerShard

  // The flush task has very little work -- pretty much none. Looking at doFlushSteps, you can see that
  // all of the heavy lifting -- including chunk encoding, forming the (potentially big) index timebucket blobs --
  // is all done in the ingestion thread. Even the futures used to do I/O will not be done in this flush thread...
  // they are allocated by the implicit ExecutionScheduler that Futures use and/or what C* etc uses.
  // The only thing that flushSched really does is tie up all these Futures together.  Thus we use the global one.
  // TODO: re-examine doFlushSteps and thread usage in flush tasks.
  import context.dispatcher
  val flushSched = GlobalScheduler.globalImplicitScheduler

  // TODO: add and remove per-shard ingestion sources?
  // For now just start it up one time and kill the actor if it fails
  val ctor = Class.forName(source.streamFactoryClass).getConstructors.head
  val streamFactory = ctor.newInstance().asInstanceOf[IngestionStreamFactory]
  logger.info(s"Using stream factory $streamFactory with config ${source.config}, storeConfig $storeConfig")

  override def postStop(): Unit = {
    super.postStop() // <- logs shutting down
    logger.info("Cancelling all streams and calling teardown")
    streamSubscriptions.keys.foreach(stopIngestion(_))
  }

  def receive: Receive = LoggingReceive {
    case GetStatus               => status(sender())
    case e: IngestRows           => ingest(e)
    case e: ShardIngestionState  => resync(e, sender())
  }

  /**
    * Compares the given shard mapper snapshot to the current set of shards being ingested and
    * reconciles any differences. It does so by stopping ingestion for shards that aren't mapped
    * to this node, and it starts ingestion for those that are.
    */
  private def resync(state: ShardIngestionState, origin: ActorRef): Unit = {
    if (invalid(state.ref)) {
      logger.error(s"$state is invalid for this ingester '${dataset.ref}'.")
      return
    }

    if (state.version != 0 && state.version <= shardStateVersion) {
      logger.info(s"Ignoring old ShardIngestionState version: ${state.version} <= $shardStateVersion")
      return
    }

    // Start with the full set of all shards being ingested, and remove shards from this set
    // which must continue being ingested.
    val shardsToStop = HashSet() ++ streams.keySet

    for (shard <- 0 until state.map.numShards) {
      if (state.map.coordForShard(shard) == context.parent) {
        // Must be ingesting from the shard.
        if (shardsToStop.contains(shard)) {
          // Is aready ingesting, and it must not be stopped.
          shardsToStop.remove(shard)
        } else {
          // Isn't ingesting, so start it.
          startIngestion(shard)
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

  private def startIngestion(shard: Int): Unit = {
    try memStore.setup(dataset, shard, storeConfig, downsample) catch {
      case ShardAlreadySetup(ds, shard) =>
        logger.warn(s"dataset=$ds shard=$shard already setup, skipping....")
        return
    }

    val ingestion = for {
      _ <- memStore.recoverIndex(dataset.ref, shard)
      checkpoints <- memStore.metastore.readCheckpoints(dataset.ref, shard) }
    yield {
      if (checkpoints.isEmpty) {
        // Start normal ingestion with no recovery checkpoint and flush group 0 first
        normalIngestion(shard, None, 0, storeConfig.diskTTLSeconds)
      } else {
        // Figure out recovery end watermark and intervals.  The reportingInterval is the interval at which
        // offsets come back from the MemStore for us to report progress.
        val startRecoveryWatermark = checkpoints.values.min + 1
        val endRecoveryWatermark = checkpoints.values.max
        val lastFlushedGroup = checkpoints.find(_._2 == endRecoveryWatermark).get._1
        val reportingInterval = Math.max((endRecoveryWatermark - startRecoveryWatermark) / 20, 1L)
        logger.info(s"Starting recovery for dataset=${dataset.ref} " +
          s"shard=${shard}: from $startRecoveryWatermark to $endRecoveryWatermark; " +
          s"last flushed group $lastFlushedGroup")
        logger.info(s"Checkpoints for dataset=${dataset.ref} shard=${shard}: $checkpoints")
        for { lastOffset <- doRecovery(shard, startRecoveryWatermark, endRecoveryWatermark, reportingInterval,
                                       checkpoints) }
        yield {
          // Start reading past last offset for normal records; start flushes one group past last group
          normalIngestion(shard, Some(lastOffset + 1), (lastFlushedGroup + 1) % numGroups,
                          storeConfig.diskTTLSeconds)
        }
      }
    }

    ingestion.recover {
      case NonFatal(t) =>
        logger.error(s"Error occurred during initialization/execution of ingestion for " +
          s"dataset=${dataset.ref} shard=${shard}", t)
        handleError(dataset.ref, shard, t)
    }
  }

  private def flushStream(startGroupNo: Int = 0): Observable[FlushCommand] = {
    if (source.config.as[Option[Boolean]]("noflush").getOrElse(false)) {
      FlushStream.empty
    } else {
      FlushStream.interval(numGroups, storeConfig.flushInterval / numGroups, startGroupNo)
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
                              startingGroupNo: Int,
                              diskTimeToLiveSeconds: Int): Unit = {
    create(shard, offset) map { ingestionStream =>
      val stream = ingestionStream.get
      logger.info(s"Starting normal/active ingestion for dataset=${dataset.ref} shard=$shard at offset $offset")
      statusActor ! IngestionStarted(dataset.ref, shard, context.parent)

      streamSubscriptions(shard) = memStore.ingestStream(dataset.ref,
        shard,
        stream,
        flushSched,
        flushStream(startingGroupNo),
        diskTimeToLiveSeconds)
      // On completion of the future, send IngestionStopped
      // except for noOpSource, which would stop right away, and is used for sending in tons of data
      // also: small chance for race condition here due to remove call in stop() method
      streamSubscriptions(shard).onComplete {
        case Failure(x) =>
          handleError(dataset.ref, shard, x)
        case Success(_) =>
          // We dont release resources when fitite ingestion ends normally.
          // Kafka ingestion is usually infinite and does not end unless canceled.
          // Cancel operation is already releasing after cancel is done.
          // We also have some tests that validate after finite ingestion is complete
          if (source != NodeClusterActor.noOpSource) statusActor ! IngestionStopped(dataset.ref, shard)
      }
    } recover { case t: Throwable =>
      logger.error(s"Error occurred when setting up ingestion pipeline for dataset=${dataset.ref} shard=$shard ", t)
      handleError(dataset.ref, shard, t)
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
                         checkpoints: Map[Int, Long]): Future[Long] = {
    val futTry = create(shard, Some(startOffset)) map { ingestionStream =>
      val recoveryTrace = Kamon.buildSpan("ingestion-recovery-trace")
                               .withTag("shard", shard.toString)
                               .withTag("dataset", dataset.ref.toString).start()
      val stream = ingestionStream.get
      statusActor ! RecoveryInProgress(dataset.ref, shard, context.parent, 0)

      val shardInstance = memStore.asInstanceOf[TimeSeriesMemStore].getShardE(dataset.ref, shard)
      val fut = memStore.recoverStream(dataset.ref, shard, stream, checkpoints, interval)
        .map { off =>
          val progressPct = if (endOffset - startOffset == 0) 100
                            else (off - startOffset) * 100 / (endOffset - startOffset)
          logger.info(s"Recovery of dataset=${dataset.ref} shard=$shard at " +
            s"$progressPct % - offset $off (target $endOffset)")
          statusActor ! RecoveryInProgress(dataset.ref, shard, context.parent, progressPct.toInt)
          off }
        .until(_ >= endOffset)
        // TODO: move this code to TimeSeriesShard itself.  Shard should control the thread
        .lastL.runAsync(shardInstance.ingestSched)
      fut.onComplete {
        case Success(_) =>
          ingestionStream.teardown()
          streams.remove(shard)
          recoveryTrace.finish()
        case Failure(ex) =>
          recoveryTrace.addError(s"Recovery failed for dataset=${dataset.ref} shard=$shard", ex)
          logger.error(s"Recovery failed for dataset=${dataset.ref} shard=$shard", ex)
          handleError(dataset.ref, shard, ex)
          recoveryTrace.finish()
      }
      fut
    }
    futTry.recover { case NonFatal(t) =>
      handleError(dataset.ref, shard, t)
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
      val ingestStream = streamFactory.create(source.config, dataset, shard, offset)
      streams(shard) = ingestStream
      logger.info(s"Ingestion stream $ingestStream set up for dataset=${dataset.ref} shard=$shard")
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
    memStore.ingest(dataset.ref, e.shard, e.records)
    if (!e.records.records.isEmpty) {
      e.ackTo ! client.IngestionCommands.Ack(e.records.offset)
    }
  }

  private def status(origin: ActorRef): Unit =
    origin ! IngestionStatus(memStore.numRowsIngested(dataset.ref))

  private def stopIngestion(shard: Int): Unit = {
    streamSubscriptions.get(shard).foreach { s =>
      s.onComplete {
        case Success(_) =>
          // release resources when stop is invoked explicitly, not when ingestion ends in non-kafka environments
          removeAndReleaseResources(dataset.ref, shard)
          // ingestion stopped event is already handled in the normalIngestion method
          logger.info(s"Stopped streaming ingestion for dataset=${dataset.ref} shard=$shard and released resources")
        case Failure(_) =>
          // release of resources on failure is already handled in the normalIngestion method
      }
    }
    streamSubscriptions.get(shard).foreach(_.cancel())
  }

  private def invalid(ref: DatasetRef): Boolean = ref != dataset.ref

  private def handleError(ref: DatasetRef, shard: Int, err: Throwable): Unit = {
    logger.error(s"Exception thrown during ingestion stream for dataset=${dataset.ref} shard=$shard." +
      s" Stopping ingestion", err)
    removeAndReleaseResources(ref, shard)
    statusActor ! IngestionError(ref, shard, err)
    logger.error(s"Stopped dataset=${dataset.ref} shard=$shard after error was thrown")
  }

  private def removeAndReleaseResources(ref: DatasetRef, shard: Int): Unit = {
    if (streamSubscriptions.contains(shard)) {
      // TODO: Wait for all the queries to stop
      streamSubscriptions.remove(shard).foreach(_.cancel)
      streams.remove(shard).foreach(_.teardown())
      // Release memory for shard in MemStore
      memStore.asInstanceOf[TimeSeriesMemStore].getShard(ref, shard)
        .foreach { shard =>
          shard.shutdown()
        }
    }
  }
}

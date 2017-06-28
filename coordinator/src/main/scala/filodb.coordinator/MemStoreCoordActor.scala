package filodb.coordinator

import akka.actor.{Actor, ActorRef, Address, Props}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import monix.execution.{Cancelable, Scheduler}
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap

import filodb.core.memstore._
import filodb.core.metadata.RichProjection

object MemStoreCoordActor {
  final case class IngestRows(ackTo: ActorRef, shard: Int, records: Seq[IngestRecord])
  case object GetStatus

  // TODO: do we need a more specific name?
  final case class Status(rowsIngested: Long, lastError: Option[Throwable])

  def props(projection: RichProjection,
            memStore: MemStore,
            selfAddress: Address,
            source: NodeClusterActor.IngestionSource)(implicit sched: Scheduler): Props =
    Props(classOf[MemStoreCoordActor], projection, memStore, selfAddress, source, sched)
}

/**
 * Simply a wrapper for ingesting new records into a MemStore
 * Also starts up an IngestStream streaming directly into MemStore.
 *
 * ERROR HANDLING: currently any error in ingestion stream or memstore ingestion wll stop the ingestion
 *
 * @param sched a Scheduler for running ingestion stream Observables
 */
private[filodb] final class MemStoreCoordActor(projection: RichProjection,
                                               memStore: MemStore,
                                               selfAddress: Address,
                                               source: NodeClusterActor.IngestionSource)
                                              (implicit sched: Scheduler) extends BaseActor {
  import MemStoreCoordActor._

  private var lastErr: Option[Throwable] = None
  private final val streamSubscriptions = new HashMap[Int, Cancelable]
  private final val streams = new HashMap[Int, IngestStream]

  // TODO: add and remove per-shard ingestion sources?
  // For now just start it up one time and kill the actor if it fails
  val ctor = Class.forName(source.streamFactoryClass).getConstructors.head
  val streamFactory = ctor.newInstance().asInstanceOf[IngestStreamFactory]
  logger.info(s"Using stream factory $streamFactory with config ${source.config}")

  private def setupShard(shard: Int): Unit = {
    try {
      memStore.setup(projection, shard)
    } catch {
      case ex @ DatasetAlreadySetup(ds) =>
        logger.warn(s"Dataset $ds already setup, projections might differ!   $ex")
      case ShardAlreadySetup =>
        logger.warn(s"Shard already setup, projection might differ")
    }

    val ingestStream = streamFactory.create(source.config, projection, shard)
    streams(shard) = ingestStream
    logger.info(s"Ingestion stream $ingestStream set up for shard $shard")

    // TODO(velvia): user-configurable error handling?  Should we stop?  Should we restart?
    val stream = ingestStream.get
    stream.onErrorRecover { case ex: Exception => handleErr(ex) }
    streamSubscriptions(shard) = memStore.ingestStream(projection.datasetRef, shard, stream) {
                                   ex => handleErr(ex) }
    // TODO: send status update on stream completion
  }

  private def stopShardIngestion(shard: Int): Unit = {
    streamSubscriptions.remove(shard).foreach(_.cancel)
    streams.remove(shard).foreach(_.teardown())
    // TODO: release memory for shard in MemStore
    logger.info(s"Stopped streaming ingestion for shard $shard and released resources...")
  }

  private def handleErr(ex: Throwable): Unit = {
    lastErr = Some(ex)
    // TODO: send status update on error
    logger.error(s"Exception thrown during ingestion stream", ex)
  }

  def receive: Receive = LoggingReceive {
    case IngestRows(ackTo, shard, records) =>
      memStore.ingest(projection.datasetRef, shard, records)
      if (records.nonEmpty) {
        ackTo ! IngestionCommands.Ack(records.last.offset)
      }

    case u @ NodeClusterActor.ShardMapUpdate(ref, newMap) =>
      // For now figure out what shards to add or remove from the new ShardMap.
      // TODO: in future, replace with status event subscription
      logger.debug(s"Got new shardMap for ref $ref = $newMap")
      val ourShards = (newMap.shardsForAddress(selfAddress) ++
                       newMap.shardsForAddress(self.path.address)).distinct
      val additions = ourShards.toSet -- streams.keys
      val subtractions = streams.keySet -- ourShards
      logger.debug(s"selfAddr=$selfAddress ourShards=$ourShards additions=$additions subt=$subtractions")
      additions.foreach(setupShard)
      subtractions.foreach(stopShardIngestion)

    case GetStatus =>
      sender ! Status(memStore.numRowsIngested(projection.datasetRef), lastErr)
  }
}
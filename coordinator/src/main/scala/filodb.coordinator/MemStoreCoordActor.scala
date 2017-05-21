package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import org.velvia.filo.RowReader

import filodb.core.memstore._
import filodb.core.metadata.RichProjection

object MemStoreCoordActor {
  def props(projection: RichProjection,
            memStore: MemStore,
            source: NodeClusterActor.IngestionSource): Props =
    Props(classOf[MemStoreCoordActor], projection, memStore, source)
}

/**
 * POC POC POC
 * Simply a wrapper for ingesting new records into a MemStore
 * Also manages RowSource for pulling new records.  Thus it supports both push and pull.
 */
private[filodb] class MemStoreCoordActor(projection: RichProjection,
                                         memStore: MemStore,
                                         source: NodeClusterActor.IngestionSource) extends BaseActor {
  import DatasetCoordinatorActor._

  try {
    memStore.setup(projection)
  } catch {
    case ex @ DatasetAlreadySetup(ds) =>
      logger.warn(s"Dataset $ds already setup, projections might differ!   $ex")
  }

  logger.info(s"Set up ${projection.datasetRef} in memstore $memStore")

  // TODO: add and remove per-shard ingestion sources?
  // For now just start it up one time and kill the actor if it fails
  val ctor = Class.forName(source.rowSourceFactoryClass).getConstructors.head
  val sourceFactory = ctor.newInstance().asInstanceOf[RowSourceFactory]
  val config = context.system.settings.config.getConfig("filodb")
  val sourceProps = sourceFactory.create(source.config, projection, memStore)
  logger.info(s"Setting up source $source...")
  val sourceActor = context.actorOf(sourceProps, s"source-${projection.datasetRef}")

  def receive: Receive = LoggingReceive {
    case NewRows(ackTo, rows, seqNo) =>
      val withOffsets = rows.map(RowWithOffset(_, seqNo))
      memStore.ingest(projection.datasetRef, withOffsets)
      ackTo ! IngestionCommands.Ack(seqNo)

    case StartFlush(originator) =>
      originator.foreach(_ ! IngestionCommands.Flushed)

    case ClearProjection(replyTo, projection) =>
    case CanIngest =>
      sender.tell(IngestionCommands.CanIngest(true), context.parent)

    case u @ NodeClusterActor.ShardMapUpdate(ref, newMap) =>
      sourceActor ! u

    case RowSource.AllDone =>
      logger.info(s"Ingestion for ${projection.datasetRef} has stopped")
      // TODO: kill myself and remove references in NodeCoordinator?

    case RowSource.IngestionErr(msg, cause) =>
      logger.error(s"Ingestion error!  $msg")
      cause.foreach(e => throw e)

    case GetStats =>
    case InitIngestion(replyTo) =>
  }
}
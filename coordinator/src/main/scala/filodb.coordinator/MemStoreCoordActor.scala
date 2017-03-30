package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import org.velvia.filo.RowReader

import filodb.core.memstore._
import filodb.core.metadata.RichProjection

object MemStoreCoordActor {
  def props(projection: RichProjection,
            memStore: MemStore): Props =
    Props(classOf[MemStoreCoordActor], projection, memStore)
}

/**
 * POC POC POC
 * Simply a wrapper for ingesting new records into a MemStore
 */
private[filodb] class MemStoreCoordActor(projection: RichProjection,
                                         memStore: MemStore) extends BaseActor {
  import DatasetCoordinatorActor._

  try {
    memStore.setup(projection)
  } catch {
    case ex @ DatasetAlreadySetup(ds) =>
      logger.warn(s"Dataset $ds already setup, projections might differ!   $ex")
  }

  logger.info(s"Set up ${projection.datasetRef} in memstore $memStore")

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

    case GetStats =>
    case InitIngestion(replyTo) =>
  }
}
package filodb.coordinator

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.velvia.filo.RowReader

import filodb.core.memstore._
import filodb.core.metadata.RichProjection

object MemStoreCoordActor {
  final case class IngestRows(ackTo: ActorRef, records: Seq[IngestRecord])
  case object GetStatus

  // TODO: do we need a more specific name?
  final case class Status(rowsIngested: Int, lastError: Option[Throwable])

  def props(projection: RichProjection,
            memStore: MemStore,
            source: NodeClusterActor.IngestionSource)(implicit sched: Scheduler): Props =
    Props(classOf[MemStoreCoordActor], projection, memStore, source, sched)
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
                                               source: NodeClusterActor.IngestionSource)
                                              (implicit sched: Scheduler) extends BaseActor {
  import MemStoreCoordActor._

  private var rowsIngested = 0
  private var lastErr: Option[Throwable] = None

  try {
    memStore.setup(projection)
  } catch {
    case ex @ DatasetAlreadySetup(ds) =>
      logger.warn(s"Dataset $ds already setup, projections might differ!   $ex")
  }

  logger.info(s"Set up ${projection.datasetRef} in memstore $memStore")

  // TODO: add and remove per-shard ingestion sources?
  // For now just start it up one time and kill the actor if it fails
  val ctor = Class.forName(source.streamFactoryClass).getConstructors.head
  val streamFactory = ctor.newInstance().asInstanceOf[IngestStreamFactory]
  val ingestStream = streamFactory.create(source.config, projection)
  logger.info(s"Created ingestion stream from factory $streamFactory using config ${source.config}")

  // TODO(velvia): user-configurable error handling?  Should we stop?  Should we restart?
  ingestStream.onErrorRecover { case ex: Exception => handleErr(ex) }
  ingestStream.foreach { records =>
                memStore.ingest(projection.datasetRef, records)
                rowsIngested += records.length
              }.recover { case ex: Exception => handleErr(ex) }
  // TODO: send status update on error
  // TODO: send status update on stream completion

  private def handleErr(ex: Exception): Unit = {
    lastErr = Some(ex)
    logger.error(s"Exception thrown during ingestion stream after $rowsIngested records", ex)
  }

  def receive: Receive = LoggingReceive {
    case IngestRows(ackTo, records) =>
      memStore.ingest(projection.datasetRef, records)
      rowsIngested += records.length
      if (records.nonEmpty) {
        ackTo ! IngestionCommands.Ack(records.last.offset)
      }

    case u @ NodeClusterActor.ShardMapUpdate(ref, newMap) =>

    case GetStatus =>
      sender ! Status(rowsIngested, lastErr)
  }
}
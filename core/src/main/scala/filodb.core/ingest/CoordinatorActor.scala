package filodb.core.ingest

import akka.actor.{Actor, ActorRef, Props}
import org.velvia.filo.RowIngestSupport
import scala.collection.mutable.HashMap

import filodb.core.BaseActor
import filodb.core.messages._

/**
 * The CoordinatorActor is the common API entry point for all FiloDB ingestion operations.
 * It is a singleton - there should be exactly one such actor per node/JVM process.
 * It is responsible for:
 * - spinning up an Ingester and RowIngester actor per ingestion stream
 * - validating dataset, schema, etc.
 * - monitoring timeouts and failure recovery from (Row)IngesterActor deaths/exceptions
 * - keeping away too many ingestion requests
 *
 * It is called by local (eg HTTP) as well as remote (eg Spark ETL) processes.
 *
 * See doc/ingestion.md and the ingestion flow diagram for more details about the entire ingestion flow.
 */
object CoordinatorActor {
  // //////////// Commands

  /**
   * Tells the CoordinatorActor to spin up the actor pipeline for high volume ingestion of a
   * single dataset.  Should be sent from an actor, which will get back a RowIngestionReady
   * with an ActorRef to start pushing rows to.   It will get Acks back from the IngesterActor.
   *
   * @return RowIngestionReady, or NotFound, CannotLockPartition, UndefinedColumns etc.
   */
  case class StartRowIngestion[R](dataset: String,
                                  partition: String,
                                  columns: Seq[String],
                                  initialVersion: Int,
                                  rowIngestSupport: RowIngestSupport[R])


  /**
   * Explicitly stop ingestion.  Always use this when possible to help keep resources low.
   */
  case class StopIngestion(streamId: Int)

  // ////////// Responses

  case class RowIngestionReady(streamId: Int, rowIngestActor: ActorRef) extends Response
  case object CannotLockPartition extends ErrorResponse
  case object NoDatasetColumns extends ErrorResponse
  case class UndefinedColumns(undefined: Seq[String]) extends ErrorResponse

  /*
   * This may be sent back for several reasons:
   * - in response to an explicit StopIngestion
   * - Idle / lack of activity or new input for that dataset/partition
   *
   * Either case, it means that the entire pipeline (Ingester, RowIngester) has been shut down, and
   * their resources released.  Any intermediate data would have been flushed.
   */
  case class IngestionStopped(streamId: Int) extends Response


  def props(metadataActor: ActorRef, dataWriterActor: ActorRef): Props =
    Props(classOf[CoordinatorActor], metadataActor, dataWriterActor)
}

class CoordinatorActor(metadataActor: ActorRef, dataWriterActor: ActorRef) extends BaseActor {
  import CoordinatorActor._

  val streamIds = new HashMap[Int, (String, String)]
  val ingesterActors = new HashMap[Int, ActorRef]
  val rowIngesterActors = new HashMap[Int, ActorRef]
  var nextStreamId = 0

  def receive: Receive = {
    case StartRowIngestion(dataset, partition, columns, initVersion, rowIngestSupport) =>

      // TODO: check that there aren't too many ingestion streams already

      // Create a IngestVerifyActor to verify dataset, schema, partition lock, etc.
      val originator = sender     // good practice, in case we use it in a future later
      val verifier = context.actorOf(IngestVerifyActor.props(
                       originator, nextStreamId, dataset, partition, columns,
                       initVersion, metadataActor, rowIngestSupport))
      nextStreamId += 1

    case IngestVerifyActor.Verified(streamId, originator, partObj, schema, ingestSupport) =>
      val ingester = context.actorOf(
        IngesterActor.props(partObj, schema, metadataActor, dataWriterActor, originator),
        s"ingester-$partObj")
      val rowIngester = context.actorOf(
        RowIngesterActor.props(ingester, schema, partObj, ingestSupport),
        s"rowIngester-$partObj-$ingestSupport")

      streamIds += streamId -> (partObj.dataset -> partObj.partition)
      ingesterActors += streamId -> ingester
      rowIngesterActors += streamId -> rowIngester
      logger.info(s"Set up ingestion pipeline for $partObj, streamId=$streamId")
      originator ! RowIngestionReady(streamId, rowIngester)

    case IngestVerifyActor.NoDatasetColumns(originator) =>
      originator ! NoDatasetColumns

    case IngestVerifyActor.UndefinedColumns(originator, undefined) =>
      originator ! UndefinedColumns(undefined)

    case IngestVerifyActor.PartitionNotFound(originator) =>
      originator ! NotFound

    case StopIngestion(streamId) =>
      logger.error("Unimplemented!")
      ???

    // TODO: implement error recovery and watch actors for termination
    // Consider restarting everything as a group?
    // case Terminated(actorRef) =>
  }
}
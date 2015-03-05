package filodb.core.ingest

import akka.actor.{Actor, ActorRef, Props}
import scala.collection.mutable.HashMap

import filodb.core.BaseActor
import filodb.core.messages._

/**
 * The CoordinatorActor is the common API entry point for all FiloDB ingestion operations.
 * It is a singleton - there should be exactly one such actor per node/JVM process.
 * It is responsible for:
 * - spinning up an Ingester and RowIngester actor per dataset/partition
 * - keeping track of Ingester readiness / backpressure
 * - monitoring timeouts and failure recovery from (Row)IngesterActor deaths/exceptions
 * - keeping away too many ingestion requests
 *
 * See doc/ingestion.md and the ingestion flow diagram for more details about the entire ingestion flow.
 */
object CoordinatorActor {
  // //////////// Commands

  /**
   * Tells the CoordinatorActor to spin up the actor pipeline for high volume ingestion of a
   * single dataset.
   * @return IngestionReady, or NotFound, CannotLockPartition, UndefinedColumns etc.
   */
  case class StartIngestion(dataset: String, partition: String, columns: Seq[String])


  /**
   * Explicitly stop ingestion.  Always use this when possible to help keep resources low.
   */
  case class StopIngestion(dataset: String, partition: String)

  // ////////// Responses

  case object CannotLockPartition extends ErrorResponse
  case class UndefinedColumns(undefined: Seq[String]) extends ErrorResponse

  /*
   * This may be sent back for several reasons:
   * - in response to an explicit StopIngestion
   * - Idle / lack of activity or new input for that dataset/partition
   *
   * Either case, it means that the entire pipeline (Ingester, RowIngester) has been shut down, and
   * their resources released.  Any intermediate data would have been flushed.
   */
  case class IngestionStopped(dataset: String, partition: String) extends Response


  def props(metadataActor: ActorRef, dataWriterActor: ActorRef): Props =
    Props(classOf[CoordinatorActor], metadataActor, dataWriterActor)
}

class CoordinatorActor(metadataActor: ActorRef, dataWriterActor: ActorRef) extends BaseActor {
  import CoordinatorActor._

  val ingesterActors = new HashMap[(String, String), ActorRef]
  val rowIngesterActors = new HashMap[(String, String), ActorRef]
  val ready = new HashMap[(String, String), Boolean]

  def receive: Receive = {
    case s @ StartIngestion(dataset, partition, columns) =>
      logger.info(s"$s...")
  }
}
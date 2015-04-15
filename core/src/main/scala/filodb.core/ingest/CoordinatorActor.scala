package filodb.core.ingest

import akka.actor.{Actor, ActorRef, Props}
import org.velvia.filo.RowIngestSupport
import scala.collection.mutable.HashMap
import scala.concurrent.Future

import filodb.core.BaseActor
import filodb.core.datastore.Datastore
import filodb.core.messages._
import filodb.core.metadata.{Partition, Column}

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

  // /////////// Internal messaging
  case class GoodToGo(originator: ActorRef, streamId: Int, ingester: ActorRef,
                      rowIngester: ActorRef, partition: Partition)

  def invalidColumns(columns: Seq[String], schema: Column.Schema): Seq[String] =
    (columns.toSet -- schema.keys).toSeq

  def props(datastore: Datastore): Props =
    Props(classOf[CoordinatorActor], datastore)
}

class CoordinatorActor(datastore: Datastore) extends BaseActor {
  import CoordinatorActor._
  import context.dispatcher

  val streamIds = new HashMap[Int, (String, String)]
  val ingesterActors = new HashMap[Int, ActorRef]
  val rowIngesterActors = new HashMap[Int, ActorRef]
  var nextStreamId = 0

  private def verifySchema(originator: ActorRef, dataset: String, version: Int, columns: Seq[String]):
      Future[Option[Column.Schema]] = {
    datastore.getSchema(dataset, version).collect {
      case Datastore.TheSchema(schema) =>
        val undefinedCols = invalidColumns(columns, schema)
        if (schema.isEmpty) {
          logger.info(s"Either no columns defined or no dataset $dataset")
          originator ! NoDatasetColumns
          None
        } else if (undefinedCols.nonEmpty) {
          logger.info(s"Undefined columns $undefinedCols for dataset $dataset with schema $schema")
          originator ! UndefinedColumns(undefinedCols.toSeq)
          None
        } else {
          Some(schema)
        }
      case e: ErrorResponse =>
        originator ! e
        None
    }.recover {
      case t: Throwable =>
        originator ! MetadataException(t)
        None
    }
  }

  private def getPartition(originator: ActorRef, dataset: String, partitionName: String):
      Future[Option[Partition]] = {
    datastore.getPartition(dataset, partitionName).collect {
      case Datastore.ThePartition(partObj) =>
        Some(partObj)
      case e: ErrorResponse                =>
        originator ! e
        None
    }.recover {
      case t: Throwable =>
        originator ! MetadataException(t)
        None
    }
  }

  def receive: Receive = {
    case StartRowIngestion(dataset, partition, columns, initVersion, rowIngestSupport) =>

      // TODO: check that there aren't too many ingestion streams already

      val originator = sender     // capture mutable sender for async response
      val streamId = nextStreamId
      nextStreamId += 1
      for { schema <- verifySchema(originator, dataset, initVersion, columns) if schema.isDefined
            partOpt <- getPartition(originator, dataset, partition) if partOpt.isDefined }
      {
        val columnSeq = columns.map(schema.get(_))
        val partObj = partOpt.get

        val ingester = context.actorOf(
          IngesterActor.props(partObj, columnSeq, datastore, originator),
          s"ingester-$partObj")
        val rowIngester = context.actorOf(
          RowIngesterActor.props(ingester, columnSeq, partObj, rowIngestSupport),
          s"rowIngester-$partObj-$rowIngestSupport")

        // Send message to myself to modify state, don't do it in async future callback
        self ! GoodToGo(originator, streamId, ingester, rowIngester, partObj)
      }

    case GoodToGo(originator, streamId, ingester, rowIngester, partition) =>
      streamIds += streamId -> (partition.dataset -> partition.partition)
      ingesterActors += streamId -> ingester
      rowIngesterActors += streamId -> rowIngester
      logger.info(s"Set up ingestion pipeline for $partition, streamId=$streamId")
      originator ! RowIngestionReady(streamId, rowIngester)

    case StopIngestion(streamId) =>
      logger.error("Unimplemented!")
      ???

    // TODO: implement error recovery and watch actors for termination
    // Consider restarting everything as a group?
    // case Terminated(actorRef) =>
  }
}
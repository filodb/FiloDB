package filodb.core.ingest

import akka.actor.{Actor, ActorRef, PoisonPill, Props, FSM}
import org.velvia.filo.RowIngestSupport

import filodb.core.BaseActor
import filodb.core.datastore.Datastore
import filodb.core.messages._
import filodb.core.metadata.{Partition, Column}

object IngestVerifyActor {
  // ////////// Responses
  case class Verified[R](streamId: Int, originator: ActorRef,
                         partition: Partition, schema: Seq[Column],
                         ingestSupport: RowIngestSupport[R])

  case class NoDatasetColumns(originator: ActorRef)
  case class UndefinedColumns(originator: ActorRef, undefined: Seq[String])
  case class PartitionNotFound(originator: ActorRef)

  // /////////// States

  sealed trait VerifyState
  case object GetLock extends VerifyState
  case object GetSchema extends VerifyState
  case object GetPartition extends VerifyState

  // /////////// Data
  // /
  sealed trait Data
  case object Uninitialized extends Data
  case class GotSchema(schema: Column.Schema) extends Data

  // /////////// Core functions
  //
  def invalidColumns(columns: Seq[String], schema: Column.Schema): Seq[String] =
    (columns.toSet -- schema.keys).toSeq

  def props[R](originator: ActorRef,
            streamId: Int,
            dataset: String,
            partition: String,
            columns: Seq[String],
            initVersion: Int,
            metadataActor: ActorRef,
            datastore: Datastore,
            ingestSupport: RowIngestSupport[R]): Props =
    Props(classOf[IngestVerifyActor[_]], originator, streamId, dataset, partition, columns,
          initVersion, metadataActor, datastore, ingestSupport)
}

import IngestVerifyActor._

/**
 * A state machine / FSM to verify a dataset's partition and schema information.
 * While this is fine, a simpler implementation would be just to use future chaining in
 * the CoordinatorActor itself.  Much less code I think.
 */
class IngestVerifyActor[R](originator: ActorRef,
                           streamId: Int,
                           dataset: String,
                           partitionName: String,
                           columns: Seq[String],
                           initVersion: Int,
                           metadataActor: ActorRef,
                           datastore: Datastore,
                           ingestSupport: RowIngestSupport[R]) extends BaseActor with FSM[VerifyState, Data] {
  // If partition locking was implemented, we would do something like this:
  // metadataActor ! GetPartitonLock(dataset, partitionName)
  // startWith(GetLock, Uninitialized)

  import context.dispatcher

  startWith(GetSchema, Uninitialized)

  // TODO: what to do about versions?
  metadataActor ! Column.GetSchema(dataset, initVersion)

  def killMyself(msg: Any): State = {
    context.parent ! msg
    self ! PoisonPill
    stay using Uninitialized
  }

  when(GetSchema) {
    case Event(Column.TheSchema(schema), Uninitialized) =>
      val undefinedCols = invalidColumns(columns, schema)
      if (schema.isEmpty) {
        logger.info(s"Either no columns defined or no dataset $dataset")
        killMyself(NoDatasetColumns(originator))
      } else if (undefinedCols.nonEmpty) {
        logger.info(s"Undefined columns $undefinedCols for dataset $dataset with schema $schema")
        killMyself(UndefinedColumns(originator, undefinedCols.toSeq))
      } else {
        datastore.getPartition(dataset, partitionName)
          .foreach { resp => self ! resp}
        goto(GetPartition) using GotSchema(schema)
      }
  }

  when(GetPartition) {
    case Event(Datastore.ThePartition(partObj), GotSchema(schema)) =>
      // invalidColumns() above guarantees the schema will have all requested columns
      val columnSeq = columns.map(schema(_))
      killMyself(Verified(streamId, originator, partObj, columnSeq, ingestSupport))
    case Event(NotFound, GotSchema(schema)) =>
      logger.info(s"Partition $partitionName for dataset $dataset not found")
      killMyself(PartitionNotFound(originator))
  }

  whenUnhandled {
    case Event(e: ErrorResponse, s) =>
      // Probably an error of some kind
      logger.error(s"received error $e in state $s")
      killMyself(e)
  }
}
package filodb.coordinator.client

import filodb.core._
import filodb.core.memstore.SomeData
import filodb.core.metadata.Dataset

// NOTE: need to inherit java.io.Serializable to ensure Kryo will serialize subclasses
trait QueryResponse extends NodeResponse with java.io.Serializable

object DatasetCommands {
  /**
   * Creates a new dataset with columns and a default projection.
   * @param dataset the Dataset object
   * @param database optionally, the database/keyspace to create the dataset in
   */
  final case class CreateDataset(numShards: Int,
                                 dataset: Dataset,
                                 database: Option[String] = None) extends NodeCommand

  case object DatasetCreated extends Response with NodeResponse
  case object DatasetAlreadyExists extends Response with NodeResponse
  final case class DatasetError(msg: String) extends ErrorResponse with NodeResponse

  /**
   * Truncates all data from the memStore and any underlying ChunkSink/persistent store for a dataset
   */
  final case class TruncateDataset(dataset: DatasetRef, numShards: Int) extends NodeCommand
  case object DatasetTruncated extends NodeResponse
  final case class ServerError(errorObject: Any) extends NodeResponse
}

object MiscCommands {
  /**
   * Asks for the NodeClusterActor ActorRef.  Sends back Option[ActorRef].
   */
  case object GetClusterActor extends NodeCommand
}

object IngestionCommands {

  /**
   * Ingests a new set of rows for a given dataset and version.
   * The partitioning column and sort column are set up in the dataset.
   *
   * @return Ack(seqNo) returned when the set of rows has been committed to the MemTable.
   */
  final case class IngestRows(dataset: DatasetRef,
                              shard: Int,
                              data: SomeData) extends NodeCommand

  final case class Ack(seqNo: Long) extends NodeResponse
  case object UnknownDataset extends NodeResponse with ErrorResponse
  // If ingestion cannot proceed, Nack is sent, then ResumeIngest is sent if things open up.
  final case class Nack(seqNo: Long) extends NodeResponse
  case object ResumeIngest extends NodeResponse

  /**
   * Initiates a flush of the remaining MemTable rows of the given dataset and version.
   * Usually used when at the end of ingesting some large blob of data.
   * @return Flushed when the flush cycle has finished successfully, committing data to columnstore.
   */
  final case class Flush(dataset: DatasetRef) extends NodeCommand
  case object Flushed extends NodeResponse
  case object FlushIgnored extends NodeResponse

  /**
   * Gets the latest ingestion stats from the IngestionActor
   */
  final case class GetIngestionStats(dataset: DatasetRef) extends NodeCommand
}


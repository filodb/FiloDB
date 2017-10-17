package filodb.coordinator

import filodb.core._
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.Dataset

// Public, external Actor/Akka API for NodeCoordinatorActor, so every incoming command should be a NodeCommand
sealed trait NodeCommand
sealed trait NodeResponse
trait QueryCommand extends NodeCommand {
  def dataset: DatasetRef
}
trait QueryResponse extends NodeResponse

object DatasetCommands {
  /**
   * Creates a new dataset with columns and a default projection.
   * @param dataset the Dataset object
   * @param database optionally, the database/keyspace to create the dataset in
   */
  final case class CreateDataset(dataset: Dataset,
                                 database: Option[String] = None) extends NodeCommand

  case object DatasetCreated extends Response with NodeResponse
  case object DatasetAlreadyExists extends Response with NodeResponse
  final case class DatasetError(msg: String) extends ErrorResponse with NodeResponse

  /**
   * Truncates all data from the memStore and any underlying ChunkSink/persistent store for a dataset
   */
  final case class TruncateDataset(dataset: DatasetRef) extends NodeCommand
  case object DatasetTruncated extends NodeResponse
}

object MiscCommands {
  /**
   * Asks for the NodeClusterActor ActorRef.  Sends back Option[ActorRef].
   */
  case object GetClusterActor extends NodeCommand
}

object IngestionCommands {
  import NodeClusterActor._

  /**
   * Sets up ingestion and querying for a given dataset and version.
   * The dataset and columns must have been previously defined.
   * Internally sets up IngestionActors and QueryActors and starts the ingestion stream.
   * It is the IngestionStream's job to translate incoming schema to the Dataset's dataColumns schema.
   * NOTE: this is not meant for external APIs but an internal one.  It is sent by the NodeClusterActor
   * after verifying the dataset.
   *
   * @param compactDatasetStr Dataset.asCompactString serialized representation of dataset
   * @param source the IngestionSource on each node.  Use noOpSource to not start ingestion and
   *               manually push records into NodeCoordinator.
   * @return no response. Instead the ClusterActor will get an update to the node status when ingestion
   *                      and querying are ready.
   */
  final case class DatasetSetup(compactDatasetStr: String,
                                source: IngestionSource = noOpSource) extends NodeCommand

  /**
   * Ingests a new set of rows for a given dataset and version.
   * The partitioning column and sort column are set up in the dataset.
   *
   * @return Ack(seqNo) returned when the set of rows has been committed to the MemTable.
   */
  final case class IngestRows(dataset: DatasetRef,
                              shard: Int,
                              rows: Seq[IngestRecord]) extends NodeCommand

  final case class Ack(seqNo: Long) extends NodeResponse
  case object UnknownDataset extends NodeResponse with ErrorResponse
  // If ingestion cannot proceed, Nack is sent, then ResumeIngest is sent if things open up.
  final case class Nack(seqNo: Long) extends NodeResponse
  case object ResumeIngest extends NodeResponse

  /**
   * Initiates a flush of the remaining MemTable rows of the given dataset and version.
   * Usually used when at the end of ingesting some large blob of data.
   * @return Flushed when the flush cycle has finished successfully, commiting data to columnstore.
   */
  final case class Flush(dataset: DatasetRef) extends NodeCommand
  case object Flushed extends NodeResponse
  case object FlushIgnored extends NodeResponse

  /**
   * Gets the latest ingestion stats from the IngestionActor
   */
  final case class GetIngestionStats(dataset: DatasetRef) extends NodeCommand
}


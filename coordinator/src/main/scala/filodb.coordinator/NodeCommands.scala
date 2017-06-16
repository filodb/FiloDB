package filodb.coordinator

import akka.actor.ActorRef
import org.velvia.filo.RowReader

import filodb.core._
import filodb.core.memstore.IngestRecord
import filodb.core.metadata.{DataColumn, Dataset, Projection}

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
   * @param columns DataColumns to create for that dataset.  Must include partition and row key columns, at a
   *          minimum.  Computed columns can be left out.
   * @param database optionally, the database/keyspace to create the dataset in
   */
  final case class CreateDataset(dataset: Dataset,
                                 columns: Seq[DataColumn],
                                 database: Option[String] = None) extends NodeCommand

  case object DatasetCreated extends Response with NodeResponse
  case object DatasetAlreadyExists extends Response with NodeResponse
  final case class DatasetError(msg: String) extends ErrorResponse with NodeResponse

  /**
   * Truncates all data from a projection of a dataset.  Waits for any pending flushes from said
   * dataset to finish first, and also clears the columnStore cache for that dataset.
   */
  final case class TruncateProjection(projection: Projection, version: Int) extends NodeCommand
  case object ProjectionTruncated extends NodeResponse

  /**
   * Drops all versions/projections of a dataset from both the column store and metastore.
   */
  final case class DropDataset(dataset: DatasetRef) extends NodeCommand
  case object DatasetDropped extends NodeResponse
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
   * Sets up ingestion and querying for a given dataset, version, and schema of columns.
   * The dataset and columns must have been previously defined.
   * Internally creates the projection and sets up DatasetCoordinatorActors and QueryActors
   * and also starts up a RowSource to start ingestion for current dataset.
   * NOTE: this is not meant for external APIs but an internal one.  It is sent by the NodeClusterActor
   * after verifying the dataset.
   *
   * @param encodedColumns a list of strings as returned by Column.toString (encoded string of all fields)
   * @param source the IngestionSource on each node.  Use noOpSource to not start ingestion and
   *               manually push records into NodeCoordinator.
   * @return no response. Instead the ClusterActor will get an update to the node status when ingestion
   *                      and querying are ready.
   */
  final case class DatasetSetup(dataset: Dataset,
                                encodedColumns: Seq[String],
                                version: Int,
                                source: IngestionSource = noOpSource) extends NodeCommand

  /**
   * Ingests a new set of rows for a given dataset and version.
   * The partitioning column and sort column are set up in the dataset.
   *
   * @return Ack(seqNo) returned when the set of rows has been committed to the MemTable.
   */
  final case class IngestRows(dataset: DatasetRef,
                              version: Int,
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
  final case class Flush(dataset: DatasetRef, version: Int) extends NodeCommand
  case object Flushed extends NodeResponse
  case object FlushIgnored extends NodeResponse

  /**
   * Checks to see if the DatasetCoordActor is ready to take in more rows.  Usually sent when an actor
   * is in a wait state.
   */
  final case class CheckCanIngest(dataset: DatasetRef, version: Int) extends NodeCommand
  final case class CanIngest(can: Boolean) extends NodeResponse

  /**
   * Gets the latest ingestion stats from the DatasetCoordinatorActor
   */
  final case class GetIngestionStats(dataset: DatasetRef, version: Int) extends NodeCommand

  /**
    * Initializes new Memtable and loads it using WAL file.
    */
  case class ReloadIngestionState(originator: ActorRef,
                                  dataset: DatasetRef,
                                  version: Int) extends NodeCommand

  case object DCAReady extends NodeResponse
}


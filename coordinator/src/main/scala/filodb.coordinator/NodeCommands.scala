package filodb.coordinator

import org.velvia.filo.RowReader

import filodb.core._
import filodb.core.metadata.{Dataset, DataColumn, Projection}

// Public, external Actor/Akka API for NodeCoordinatorActor, so every incoming command should be a NodeCommand
sealed trait NodeCommand
sealed trait NodeResponse

object DatasetCommands {
  /**
   * Creates a new dataset with columns and a default projection.
   * @param dataset the Dataset object
   * @param columns DataColumns to create for that dataset.  Must include partition and row key columns, at a
   *          minimum.  Computed columns can be left out.
   * @param database optionally, the database/keyspace to create the dataset in
   */
  case class CreateDataset(dataset: Dataset,
                           columns: Seq[DataColumn],
                           database: Option[String] = None) extends NodeCommand

  case object DatasetCreated extends Response with NodeResponse
  case object DatasetAlreadyExists extends Response with NodeResponse
  case class DatasetError(msg: String) extends ErrorResponse with NodeResponse

  /**
   * Truncates all data from a projection of a dataset.  Waits for any pending flushes from said
   * dataset to finish first, and also clears the columnStore cache for that dataset.
   */
  case class TruncateProjection(projection: Projection, version: Int) extends NodeCommand
  case object ProjectionTruncated extends NodeResponse

  /**
   * Drops all versions/projections of a dataset from both the column store and metastore.
   */
  case class DropDataset(dataset: DatasetRef) extends NodeCommand
  case object DatasetDropped extends NodeResponse
}

object IngestionCommands {
  /**
   * Sets up ingestion for a given dataset, version, and schema of columns.
   * The dataset and columns must have been previously defined.
   *
   * @return BadSchema if the partition column is unsupported, sort column invalid, etc.
   */
  case class SetupIngestion(dataset: DatasetRef,
                            schema: Seq[String],
                            version: Int) extends NodeCommand

  case object IngestionReady extends NodeResponse
  case object UnknownDataset extends ErrorResponse with NodeResponse
  case class UndefinedColumns(undefined: Set[String]) extends ErrorResponse with NodeResponse
  case class BadSchema(message: String) extends ErrorResponse with NodeResponse

  /**
   * Ingests a new set of rows for a given dataset and version.
   * The partitioning column and sort column are set up in the dataset.
   *
   * @param seqNo the sequence number to be returned for acknowledging the entire set of rows
   * @return Ack(seqNo) returned when the set of rows has been committed to the MemTable.
   */
  case class IngestRows(dataset: DatasetRef,
                        version: Int,
                        rows: Seq[RowReader],
                        seqNo: Long) extends NodeCommand

  case class Ack(seqNo: Long) extends NodeResponse

  /**
   * Initiates a flush of the remaining MemTable rows of the given dataset and version.
   * Usually used when at the end of ingesting some large blob of data.
   * @return Flushed when the flush cycle has finished successfully, commiting data to columnstore.
   */
  case class Flush(dataset: DatasetRef, version: Int) extends NodeCommand
  case object Flushed extends NodeResponse

  /**
   * Checks to see if the DatasetCoordActor is ready to take in more rows.  Usually sent when an actor
   * is in a wait state.
   */
  case class CheckCanIngest(dataset: DatasetRef, version: Int) extends NodeCommand
  case class CanIngest(can: Boolean) extends NodeResponse

  /**
   * Gets the latest ingestion stats from the DatasetCoordinatorActor
   */
  case class GetIngestionStats(dataset: DatasetRef, version: Int) extends NodeCommand
}

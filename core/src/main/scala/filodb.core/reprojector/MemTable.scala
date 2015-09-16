package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.collection.mutable.HashMap

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.RowReader

object MemTable {
  sealed trait SetupResponse
  case object SetupDone extends SetupResponse
  case object AlreadySetup extends SetupResponse
  case object BadSchema extends SetupResponse

  sealed trait IngestionResponse
  case object Ingested extends IngestionResponse
  case object PleaseWait extends IngestionResponse  // Cannot quite ingest yet

  sealed trait FlipResponse
  case object Flipped extends FlipResponse
  case object LockedNotEmpty extends FlipResponse

  case object NoSuchDatasetVersion extends IngestionResponse with FlipResponse

  sealed trait BufferType
  case object Active extends BufferType
  case object Locked extends BufferType

  // TODO: Base this on RichProjection, maybe this is not needed
  case class IngestionSetup(dataset: Dataset,
                            schema: Seq[Column],
                            partitioningFunc: RowReader => PartitionKey,
                            sortColumnNum: Int,
                            keyHelper: SortKeyHelper[Any]) {
    def helper[K]: SortKeyHelper[K] = keyHelper.asInstanceOf[SortKeyHelper[K]]
    def sortColumn: Column = schema(sortColumnNum)
  }
}

/**
 * The MemTable serves these purposes:
 * 1) Holds incoming rows of data before being flushed
 * 2) Can extract rows of data in a given sort order, and remove them
 * 3) Can read rows of data in a given sort order for queries
 *
 * Ingestion must be set up for each (dataset, version) pair with a fixed schema (for now).
 * For each (dataset, version), a MemTable ingests new rows into an Active table.  It can then be
 * flipped into a Locked table, which cannot ingest new rows and is used for flushing/reprojections.
 *
 * A dataset being actively flushed has a Locked table whose number of rows is decreasing towards 0.
 *
 * It definitely must be multithread safe, and very very fast.  Synchronization occurs around setting up
 * new ingestion datasets.
 *
 * Data written to a MemTable should be logged via WAL or some other mechanism so it can be recovered in
 * case of failure.
 */
trait MemTable extends StrictLogging {
  import MemTable._
  import RowReader._

  def close(): Unit

  /**
   * === Dataset ingestion setup ===
   */

  /**
   * Prepares the memtable for ingesting a particular dataset with a schema.  Note: no checking is here
   * done to make sure the schema is valid.  If the schema needs to be changed, use a new version.
   * @dataset a Dataset with valid partitioning key.  projections(0).sortColumn will also be used.
   * @returns BadSchema if cannot determine a partitioningFunc or sort column
   *          AlreadySetup if the dataset has been setup before
   */
  def setupIngestion(dataset: Dataset, schema: Seq[Column], version: Int): SetupResponse = {
    import Column.ColumnType._
    ingestionSetups.synchronized {
      val partitionFunc = getPartitioningFunc(dataset, schema).getOrElse(return BadSchema)

      val sortColNo = schema.indexWhere(_.hasId(dataset.projections.head.sortColumn))
      if (sortColNo < 0) return BadSchema

      val versions = ingestionSetups.getOrElseUpdate(dataset.name, new HashMap[Int, IngestionSetup])
      if (versions contains version) return AlreadySetup

      val helper = (schema(sortColNo).columnType match {
        case LongColumn    => Dataset.sortKeyHelper[Long](dataset.options)
        case other: Column.ColumnType =>
          logger.info(s"Unsupported sort column type $other attempted for dataset $dataset")
          return BadSchema
      }).asInstanceOf[SortKeyHelper[Any]]

      versions(version) = IngestionSetup(dataset, schema, partitionFunc, sortColNo, helper)
      logger.info(s"Set up ingestion for dataset $dataset, version $version with schema $schema")
      SetupDone
    }
  }

  def getIngestionSetup(dataset: TableName, version: Int): Option[IngestionSetup] =
    ingestionSetups.get(dataset).flatMap(_.get(version))

  private val ingestionSetups = new HashMap[TableName, HashMap[Int, IngestionSetup]]

  /**
   * === Row ingest, read, delete operations ===
   */

  /**
   * Ingests a bunch of new rows for a given dataset and version.  Will be ingested into the Active buffer.
   * @param dataset the Dataset to ingest.  Must have been setup using setupIngestion().
   * @param version the version to ingest into.
   * @param rows the rows to ingest
   * @returns Ingested or PleaseWait, if the MemTable is too full.
   */
  def ingestRows(dataset: TableName, version: Int, rows: Seq[RowReader]): IngestionResponse = {
    import Column.ColumnType._

    val setup = getIngestionSetup(dataset, version).getOrElse(return NoSuchDatasetVersion)
    setup.schema(setup.sortColumnNum).columnType match {
      case LongColumn    => ingestRowsInner[Long](setup, version, rows)
      case other: Column.ColumnType => throw new RuntimeException("Illegal sort key type $other")
    }
  }

  def ingestRowsInner[K: TypedFieldExtractor](setup: IngestionSetup,
                                              version: Int,
                                              rows: Seq[RowReader]): IngestionResponse

  /**
   * Reads rows out. Note that inserts may be happening while rows are read, so results are not
   * guaranteed to be stable if you read from the Active buffer.
   */
  def readRows[K: SortKeyHelper](keyRange: KeyRange[K],
                                 version: Int,
                                 buffer: BufferType): Iterator[RowReader]

  /**
   * Reads all rows of the memtable out, from every partition.  Partition ordering is not
   * guaranteed, but all sort keys K within the partition will be ordered.
   */
  def readAllRows[K](dataset: TableName, version: Int, buffer: BufferType):
      Iterator[(PartitionKey, K, RowReader)]

  /**
   * Removes specific rows from a particular keyRange and version.  Can only remove rows
   * from the Locked buffer.
   */
  def removeRows[K: SortKeyHelper](keyRange: KeyRange[K], version: Int): Unit

  /**
   * Flips the active and locked buffers. After this is called, ingestion will immediately
   * proceed to the new active buffer, and the existing active buffer becomes the new Locked
   * buffer.
   * @returns NotEmpty if the locked buffer is not empty
   */
  def flipBuffers(dataset: TableName, version: Int): FlipResponse

  def numRows(dataset: TableName, version: Int, buffer: BufferType): Option[Long]

  /**
   * Yes, this clears everything!  It's meant for testing only.
   */
  def clearAllData(): Unit = {
    ingestionSetups.clear()
    clearAllDataInner()
  }

  def clearAllDataInner(): Unit

  /**
   * == Querying and Stats ==
   */
  def datasets: Set[String] = ingestionSetups.keys.toSet

  def versionsForDataset(dataset: TableName): Option[Set[Int]] =
    ingestionSetups.get(dataset).map(_.keys.toSet)

  /**
   * Returns a list of the number of rows for each (dataset, version) memtable.
   * @param buffer specify whether to look up Active or Locked memtables
   * @param nonZero if true, only return tables that have nonzero # of rows
   */
  def allNumRows(buffer: BufferType, nonZero: Boolean = false): Seq[((TableName, Int), Long)] = {
    val records = {
      for { dataset <- datasets.toSeq
            version <- versionsForDataset(dataset).get.toSeq }
      yield { ((dataset, version), numRows(dataset, version, buffer).get) }
    }
    if (nonZero) records.filter(_._2 > 0) else records
  }

  /**
   * Returns the (dataset, version) and # of rows in the Locked table for all dataset/versions
   * that have non-empty Locked tables, ie that are actively being flushed.
   */
  def flushingDatasets: Seq[((TableName, Int), Long)] = allNumRows(Locked, nonZero = true)

  /**
   * == Common helper funcs ==
   */
  private def getPartitioningFunc(dataset: Dataset, schema: Seq[Column]):
      Option[RowReader => PartitionKey] = {
    if (dataset.partitionColumn == Dataset.DefaultPartitionColumn) {
      Some(row => Dataset.DefaultPartitionKey)
    } else {
      val partitionColNo = schema.indexWhere(_.hasId(dataset.partitionColumn))
      if (partitionColNo < 0) return None
      if (schema(partitionColNo).columnType != Column.ColumnType.StringColumn) return None
      Some(row => row.getString(partitionColNo))
    }
  }
}


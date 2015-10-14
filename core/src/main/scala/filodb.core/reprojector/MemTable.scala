package filodb.core.reprojector

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.velvia.filo.RowReader
import scala.collection.mutable.HashMap

import filodb.core.{KeyRange, SortKeyHelper}
import filodb.core.Types._
import filodb.core.metadata.{Column, Dataset}

object MemTable {
  sealed trait SetupResponse
  case object SetupDone extends SetupResponse
  case object AlreadySetup extends SetupResponse
  case class BadSchema(reason: String) extends SetupResponse

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

  val DefaultMinFreeMb = 512

  // TODO: Base this on RichProjection, maybe this is not needed
  case class IngestionSetup(dataset: Dataset,
                            schema: Seq[Column],
                            partitioningFunc: RowReader => PartitionKey,
                            sortColumnNum: Int,
                            keyHelper: SortKeyHelper[Any]) {
    def helper[K]: SortKeyHelper[K] = keyHelper.asInstanceOf[SortKeyHelper[K]]
    def sortColumn: Column = schema(sortColumnNum)
  }

  case class NullPartitionValue(partCol: String) extends Exception(s"Null partition value for col $partCol")
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
 *
 * ==Backpressure==
 * The idea is for clients to keep writing so long as it keeps getting back Ingested.  If it gets
 * PleaseWait, then it needs to slow down and periodically retry, or call canIngest().
 */
trait MemTable extends StrictLogging {
  import MemTable._
  import RowReader._

  def close(): Unit

  // The minimum amount of free memory for ingestion to continue
  def minFreeMb: Int

  /**
   * === Dataset ingestion setup ===
   */

  /**
   * Prepares the memtable for ingesting a particular dataset with a schema.  Note: no checking is here
   * done to make sure the schema is valid.  If the schema needs to be changed, use a new version.
   * @dataset a Dataset with valid partitioning column.  projections(0).sortColumn will also be used.
   * @defaultPartitionKey if Some(key), a null value in partitioning column will cause key to be used.
   *                      if None, then NullPartitionValue will be thrown when null value
   *                        is encountered in a partitioning column.
   * @return BadSchema if cannot determine a partitioningFunc or sort column
   *          AlreadySetup if the dataset has been setup before
   */
  def setupIngestion(dataset: Dataset,
                     schema: Seq[Column],
                     version: Int,
                     defaultPartitionKey: Option[PartitionKey] = None): SetupResponse = {
    import Column.ColumnType._
    ingestionSetups.synchronized {
      val partitionFunc = getPartitioningFunc(dataset, schema, defaultPartitionKey).getOrElse(
                            return BadSchema(s"Problem with partitioning column ${dataset.partitionColumn}"))

      val sortColName = dataset.projections.head.sortColumn
      val sortColNo = schema.indexWhere(_.hasId(sortColName))
      if (sortColNo < 0) return BadSchema(s"Sort column $sortColName not in schema $schema")

      val versions = ingestionSetups.getOrElseUpdate(dataset.name, new HashMap[Int, IngestionSetup])
      if (versions contains version) return AlreadySetup

      val helper = Dataset.sortKeyHelper[Any](dataset, schema(sortColNo)).getOrElse {
        val msg = s"Unsupported sort column type ${schema(sortColNo).columnType} for dataset $dataset"
        logger.info(msg)
        return BadSchema(msg)
      }

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
   * @return Ingested or PleaseWait, if the MemTable is too full or we are low on memory
   */
  def ingestRows(dataset: TableName, version: Int, rows: Seq[RowReader]): IngestionResponse = {
    import Column.ColumnType._

    val freeMB = sys.runtime.freeMemory / (1024*1024)
    if (freeMB < minFreeMb) {
      logger.info(s"Only $freeMB MB memory left, cannot accept more writes...")
      logger.info(s"MemTable state: ${allNumRows(Active, true)}")
      sys.runtime.gc()
      return PleaseWait
    }

    val setup = getIngestionSetup(dataset, version).getOrElse(return NoSuchDatasetVersion)
    setup.schema(setup.sortColumnNum).columnType match {
      case LongColumn    => ingestRowsInner[Long](setup, version, rows)
      case IntColumn     => ingestRowsInner[Int](setup, version, rows)
      case DoubleColumn  => ingestRowsInner[Double](setup, version, rows)
      case other: Column.ColumnType => throw new RuntimeException("Illegal sort key type $other")
    }
  }

  def ingestRowsInner[K: TypedFieldExtractor](setup: IngestionSetup,
                                              version: Int,
                                              rows: Seq[RowReader]): IngestionResponse

  /**
   * Returns true if the MemTable is capable of ingesting more data.  Designed to be a much
   * lighter weight alternative to ingestRows so that clients can poll the memTable for when
   * conditions are right for ingestion to occur again.
   */
  def canIngest(dataset: TableName, version: Int): Boolean = {
    if (sys.runtime.freeMemory / (1024*1024) < minFreeMb) {
      sys.runtime.gc()
      return false
    }
    canIngestInner(dataset, version)
  }

  // Determines if there is enough room for the specific dataset
  def canIngestInner(dataset: TableName, version: Int): Boolean

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
   * Deletes the entire locked table for a given dataset and version.
   * Note: does not check if it is empty or not.  Should be only called once all reprojections are done.
   */
  def deleteLockedTable(dataset: TableName, version: Int): Unit

  /**
   * Removes specific rows from a particular keyRange and version.  Can only remove rows
   * from the Locked buffer.
   */
  def removeRows[K: SortKeyHelper](keyRange: KeyRange[K], version: Int): Unit

  /**
   * Flips the active and locked buffers. After this is called, ingestion will immediately
   * proceed to the new active buffer, and the existing active buffer becomes the new Locked
   * buffer.
   * @return NotEmpty if the locked buffer is not empty
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
  private def getPartitioningFunc(dataset: Dataset,
                                  schema: Seq[Column],
                                  defaultPartitionKey: Option[PartitionKey]):
      Option[RowReader => PartitionKey] = {
    if (dataset.partitionColumn == Dataset.DefaultPartitionColumn) {
      Some(row => Dataset.DefaultPartitionKey)
    } else {
      val partitionColNo = schema.indexWhere(_.hasId(dataset.partitionColumn))
      if (partitionColNo < 0) return None

      import Column.ColumnType._
      val extractFunc: Option[RowReader => PartitionKey] = schema(partitionColNo).columnType match {
        case StringColumn => Some(row => row.getString(partitionColNo))
        case IntColumn    => Some(row => row.getInt(partitionColNo).toString)
        case LongColumn   => Some(row => row.getLong(partitionColNo).toString)
        case other: Column.ColumnType => None
      }
      extractFunc.map { func =>
        defaultPartitionKey.map { defKey =>
          (row: RowReader) => if (row.notNull(partitionColNo)) func(row) else defKey
        }.getOrElse {
          (row: RowReader) => if (row.notNull(partitionColNo)) { func(row) }
                              else { throw NullPartitionValue(schema(partitionColNo).name) }
        }
      }
    }
  }
}


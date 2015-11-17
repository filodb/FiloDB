package filodb.core.store

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}

import filodb.core.Types._
import filodb.core._
import filodb.core.metadata.{Column, ProjectionInfo}
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object MetaStore {

  case class IllegalColumnChange(reasons: Seq[String]) extends Exception {
    override def getMessage: String = reasons.mkString(", ")
  }

  case class BadSchema(reason: String) extends Exception("BadSchema: " + reason)

  def projectionInfo[R, S](dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): ProjectionInfo[R, S] =
    makeProjection[R, S](dataset, columns, projectionId).get

  def makeProjection[R, S](dataset: Dataset, columns: Seq[Column], projectionId: Int = 0): Try[ProjectionInfo[R, S]] = {
    def fail(reason: String): Try[ProjectionInfo[R, S]] = Failure(BadSchema(reason))

    val normProjection = dataset.projections(projectionId)
    val richColumns = {
      if (normProjection.columns.isEmpty) {
        columns
      } else {
        val columnMap = columns.map { c => c.name -> c }.toMap
        val missing = normProjection.columns.toSet -- columnMap.keySet
        if (missing.nonEmpty) return fail(s"Specified projection columns are missing: $missing")
        normProjection.columns.map(columnMap)
      }
    }

    val sortColNo = richColumns.indexWhere(_.hasId(normProjection.sortColumn))
    if (sortColNo < 0) return fail(s"Sort column ${normProjection.sortColumn} not in columns $richColumns")

    val sortColumn = richColumns(sortColNo)
    if (!(KeyType.ValidSortClasses contains sortColumn.columnType.clazz)) {
      return fail(s"Unsupported sort column type ${sortColumn.columnType}")
    }
    val keyColNo = richColumns.indexWhere(_.hasId(normProjection.keyColumn))
    if (keyColNo < 0) return fail(s"Key column ${normProjection.keyColumn} not in columns $richColumns")

    val keyColumn = richColumns(keyColNo)
    if (!(KeyType.ValidSortClasses contains sortColumn.columnType.clazz)) {
      return fail(s"Unsupported key column type ${sortColumn.columnType}")
    }

    val rowKeyType = Dataset.keyType[R](dataset, keyColumn).get
    val segmentType = Dataset.keyType[S](dataset, sortColumn).get

    for {partitionFunc <- Dataset.getPartitioningFunc(dataset, richColumns)} yield {
      ProjectionInfo[R, S](projectionId, dataset.name, keyColumn, keyColNo, sortColumn, sortColNo,
        normProjection.reverse, richColumns, rowKeyType, segmentType, partitionFunc)
    }
  }
}

/**
 * The MetaStore defines an API to read and write dataset/column/projection metadata.
 * It is not responsible for sharding, partitioning, etc. which is the domain of the ColumnStore.
 */
trait MetaStore {

  /**
   * Clears all dataset and column metadata from the MetaStore.
   */
  def clearAllData(): Future[Boolean]

  /**
   * ** Dataset API ***
   */

  /**
   * Creates a new dataset with the given name, if it doesn't already exist.
   * @param dataset the Dataset to create.  Should have superprojection defined.
   * @return Success, or AlreadyExists, or StorageEngineException
   */
  def newDataset(dataset: Dataset): Future[Boolean]

  /**
   * Retrieves a Dataset object of the given name
   * @param name Name of the dataset to retrieve
   * @return a Dataset
   */
  def getDataset(name: String): Future[Dataset]

  /**
   * Deletes dataset metadata including all projections.  Does not delete column store data.
   * @param name Name of the dataset to delete.
   * @return Success, or MetadataException, or StorageEngineException
   */
  def deleteDataset(name: String): Future[Boolean]

  // TODO: add/delete projections

  /**
   * Get the schema for a version of a dataset.  This scans all defined columns from the first version
   * on up to figure out the changes. Deleted columns are not returned.
   * Implementations should use Column.foldSchema.
   * @param dataset the name of the dataset to return the schema for
   * @param version the version of the dataset to return the schema for
   * @return a Schema, column name -> Column definition, or ErrorResponse
   */
  def getSchema(dataset: String, version: Int): Future[Column.Schema]
}

/**
 * A dataset is a table with a schema.
 * A dataset is partitioned, a partitioning column controls how data is distributed.
 * Data within a dataset consists of multiple projections, including a main "superprojection"
 * that contains all columns.
 */
case class Dataset(name: String,
                   projections: Seq[Projection],

                   /**
                    * Defines the column to be used for partitioning.
                    * If one is not defined, then assume a single global partition, with a special
                    * column name.
                    */
                   partitionColumn: String = Dataset.DefaultPartitionColumn,
                   options: DatasetOptions = Dataset.DefaultOptions)

/**
 * Config options for a table define operational details for the column store and memtable.
 * Every option must have a default!
 */
case class DatasetOptions(chunkSize: Int,
                          segmentSize: String,
                          defaultPartitionKey: Option[PartitionKey] = None) {
  import scala.collection.JavaConverters._
  override def toString: String = {
    val map = Map(
      "chunkSize" -> chunkSize,
      "segmentSize" -> segmentSize
    ) ++
      defaultPartitionKey.map(k => Map("defaultPartitionKey" -> k)).getOrElse(Map.empty)
    val config = ConfigFactory.parseMap(map.asJava)
    config.root.render(ConfigRenderOptions.concise)
  }
}

object DatasetOptions {
  def fromString(s: String): DatasetOptions = {
    val config = ConfigFactory.parseString(s)
    DatasetOptions(chunkSize = config.getInt("chunkSize"),
      segmentSize = config.getString("segmentSize"),
      defaultPartitionKey = config.as[Option[PartitionKey]]("defaultPartitionKey"))
  }
}

/**
 * Contains many helper functions especially pertaining to dataset partitioning.
 */
object Dataset {
  // If a partitioning column is not defined then this refers to a single global partition, and
  // the dataset must fit in one node.
  val DefaultPartitionColumn = ":single"
  val DefaultPartitionKey: PartitionKey = "/0"

  val DefaultOptions = DatasetOptions(chunkSize = 1000,
    segmentSize = "10000")

  /**
   * Creates a new Dataset with a single superprojection with a defined sort order.
   */
  def apply(name: String,
            keyColumn: String,
            sortColumn: String,
            partitionColumn: String): Dataset =
    Dataset(name, Seq(Projection(0, name, keyColumn, sortColumn)), partitionColumn)

  def apply(name: String, keyColumn: String, sortColumn: String): Dataset =
    Dataset(name, Seq(Projection(0, name, keyColumn, sortColumn)), DefaultPartitionColumn)

  /**
   * Returns a SortKeyHelper configured from the DatasetOptions.
   */
  def keyTypeForOptions[K: ClassTag](options: DatasetOptions): KeyType[K] = {
    val StringClass = classOf[String]
    implicitly[ClassTag[K]].runtimeClass match {
      case java.lang.Long.TYPE => new LongKeyType(options.segmentSize.toLong).
        asInstanceOf[KeyType[K]]
      case java.lang.Integer.TYPE => new IntKeyType(options.segmentSize.toInt).
        asInstanceOf[KeyType[K]]
      case java.lang.Double.TYPE => new DoubleKeyType(options.segmentSize.toDouble).
        asInstanceOf[KeyType[K]]
      case StringClass => new StringKeyType(options.segmentSize.toInt).
        asInstanceOf[KeyType[K]]
    }
  }

  def keyTypeForOptionsAndColumn[K](options: DatasetOptions, column: Column): Option[KeyType[K]] = {
    import filodb.core.metadata.Column.ColumnType._
    val helper = column.columnType match {
      case LongColumn => Some(keyTypeForOptions[Long](options))
      case IntColumn => Some(keyTypeForOptions[Int](options))
      case DoubleColumn => Some(keyTypeForOptions[Double](options))
      case StringColumn => Some(keyTypeForOptions[String](options))
      case other: Column.ColumnType => None
    }
    helper.asInstanceOf[Option[KeyType[K]]]
  }

  def keyType[K](dataset: Dataset, column: Column): Option[KeyType[K]] =
    keyTypeForOptionsAndColumn[K](dataset.options, column)

  trait PartitionValueException

  case class BadPartitionColumn(reason: String) extends Exception("BadPartitionColumn: " + reason) with
  PartitionValueException

  case class NullPartitionValue(partCol: String) extends Exception(s"Null partition value for col $partCol")
  with PartitionValueException

  /**
   * Gets a partitioning function to extract the PartitionKey out of a row.  The return
   * results depend on how the Dataset and DatasetOptions are configured.
   *
   * @return a partitioning function RowReader => PartitionKey:
   *         If dataset.partitionColumn is DefaultPartitionColumn, then just returns the DefaultPartitioningKey
   *         If dataset.partitionColumn does not exist in schema or is not supported type, BadPartitionColumn
   *         Otherwise, a function that extracts the partition key out.
   *         If the defaultPartitionKey is defined, then when a null partition column value is found,
   *         it will use that default.  Otherwise a NullPartitionValue is returned.
   */
  def getPartitioningFunc(dataset: Dataset, schema: Seq[Column]): Try[RowReader => PartitionKey] = {
    if (dataset.partitionColumn == DefaultPartitionColumn) {
      Success((row: RowReader) => DefaultPartitionKey)
    } else {
      val partitionColNo = schema.indexWhere(_.hasId(dataset.partitionColumn))
      if (partitionColNo < 0) return Failure(BadPartitionColumn(
        s"Column ${dataset.partitionColumn} not in schema $schema"))

      import filodb.core.metadata.Column.ColumnType._
      val extractFunc: RowReader => PartitionKey = schema(partitionColNo).columnType match {
        case StringColumn => row => row.getString(partitionColNo)
        case IntColumn => row => row.getInt(partitionColNo).toString
        case LongColumn => row => row.getLong(partitionColNo).toString
        case other: Column.ColumnType =>
          return Failure(BadPartitionColumn(s"Unsupported partitioning type $other"))
      }
      val func = dataset.options.defaultPartitionKey.fold {
        (row: RowReader) => if (row.notNull(partitionColNo)) {
          extractFunc(row)
        }
        else {
          throw NullPartitionValue(schema(partitionColNo).name)
        }
      } { defKey =>
        (row: RowReader) => if (row.notNull(partitionColNo)) extractFunc(row) else defKey
      }
      Success(func)
    }
  }
}

/**
 * A Projection defines one particular view of a dataset, designed to be optimized for a particular query.
 * It usually defines a sort order and subset of the columns.
 *
 * By convention, projection 0 is the SuperProjection which consists of all columns from the dataset.
 *
 * The Projection base class is normalized, ie it doesn't have all the information.
 */
case class Projection(id: Int,
                      dataset: TableName,
                      keyColumn: ColumnId,
                      sortColumn: ColumnId,
                      reverse: Boolean = false,
                      // Nil columns means all columns
                      columns: Seq[ColumnId] = Nil,
                      // Probably not necessary in the future
                      segmentSize: String = "10000")

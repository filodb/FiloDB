package filodb.core.metadata

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Try, Success, Failure}

import filodb.core._
import filodb.core.Types
import filodb.core.Types.PartitionKey

/**
 * A dataset is a table with a schema.
 * A dataset is partitioned, a partitioning column controls how data is distributed.
 * Data within a dataset consists of multiple projections, including a main "superprojection"
 * that contains all columns.
 */
case class Dataset(name: String,
                   projections: Seq[Projection],
                   /**
                    *  Defines the column to be used for partitioning.
                    *  If one is not defined, then assume a single global partition, with a special
                    *  column name.
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
            sortColumn: String,
            partitionColumn: String): Dataset =
    Dataset(name, Seq(Projection(0, name, sortColumn)), partitionColumn)

  def apply(name: String, sortColumn: String): Dataset =
    Dataset(name, Seq(Projection(0, name, sortColumn)), DefaultPartitionColumn)

  /**
   * Returns a SortKeyHelper configured from the DatasetOptions.
   */
  def sortKeyHelper[K: ClassTag](options: DatasetOptions): SortKeyHelper[K] = {
    val StringClass = classOf[String]
    implicitly[ClassTag[K]].runtimeClass match {
      case java.lang.Long.TYPE => (new LongKeyHelper(options.segmentSize.toLong)).
                                    asInstanceOf[SortKeyHelper[K]]
      case java.lang.Integer.TYPE => (new IntKeyHelper(options.segmentSize.toInt)).
                                    asInstanceOf[SortKeyHelper[K]]
      case java.lang.Double.TYPE => (new DoubleKeyHelper(options.segmentSize.toDouble)).
                                    asInstanceOf[SortKeyHelper[K]]
      case StringClass         => (new StringKeyHelper(options.segmentSize.toInt)).
                                    asInstanceOf[SortKeyHelper[K]]
    }
  }

  def sortKeyHelper[K](options: DatasetOptions, sortColumn: Column): Option[SortKeyHelper[K]] = {
    import Column.ColumnType._
    val helper = sortColumn.columnType match {
      case LongColumn    => Some(sortKeyHelper[Long](options))
      case IntColumn     => Some(sortKeyHelper[Int](options))
      case DoubleColumn  => Some(sortKeyHelper[Double](options))
      case StringColumn  => Some(sortKeyHelper[String](options))
      case other: Column.ColumnType =>  None
    }
    helper.asInstanceOf[Option[SortKeyHelper[K]]]
  }

  def sortKeyHelper[K](dataset: Dataset, sortColumn: Column): Option[SortKeyHelper[K]] =
    sortKeyHelper[K](dataset.options, sortColumn)

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
   *   If dataset.partitionColumn is DefaultPartitionColumn, then just returns the DefaultPartitioningKey
   *   If dataset.partitionColumn does not exist in schema or is not supported type, BadPartitionColumn
   *   Otherwise, a function that extracts the partition key out.
   *     If the defaultPartitionKey is defined, then when a null partition column value is found,
   *     it will use that default.  Otherwise a NullPartitionValue is returned.
   */
  def getPartitioningFunc(dataset: Dataset, schema: Seq[Column]): Try[RowReader => PartitionKey] = {
    if (dataset.partitionColumn == DefaultPartitionColumn) {
      Success((row: RowReader) => DefaultPartitionKey)
    } else {
      val partitionColNo = schema.indexWhere(_.hasId(dataset.partitionColumn))
      if (partitionColNo < 0) return Failure(BadPartitionColumn(
        s"Column ${dataset.partitionColumn} not in schema $schema"))

      import Column.ColumnType._
      val extractFunc: RowReader => PartitionKey = schema(partitionColNo).columnType match {
        case StringColumn => row => row.getString(partitionColNo)
        case IntColumn    => row => row.getInt(partitionColNo).toString
        case LongColumn   => row => row.getLong(partitionColNo).toString
        case other: Column.ColumnType =>
          return Failure(BadPartitionColumn(s"Unsupported partitioning type $other"))
      }
      val func = dataset.options.defaultPartitionKey.map { defKey =>
        (row: RowReader) => if (row.notNull(partitionColNo)) extractFunc(row) else defKey
      }.getOrElse {
        (row: RowReader) => if (row.notNull(partitionColNo)) { extractFunc(row) }
                            else { throw NullPartitionValue(schema(partitionColNo).name) }
      }
      Success(func)
    }
  }
}

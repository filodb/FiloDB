package filodb.core.metadata

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import filodb.core._
import filodb.core.Types

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
                          segmentSize: String) {
  override def toString: String = {
    val config = ConfigFactory.parseMap(Map(
                   "chunkSize" -> chunkSize,
                   "segmentSize" -> segmentSize
                 ).asJava)
    config.root.render(ConfigRenderOptions.concise)
  }
}

object DatasetOptions {
  def fromString(s: String): DatasetOptions = {
    val config = ConfigFactory.parseString(s)
    DatasetOptions(chunkSize = config.getInt("chunkSize"),
                   segmentSize = config.getString("segmentSize"))
  }
}

object Dataset {
  // If a partitioning column is not defined then this refers to a single global partition, and
  // the dataset must fit in one node.
  val DefaultPartitionColumn = ":single"
  val DefaultPartitionKey: Types.PartitionKey = "/0"

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
    import Column.ColumnType._
    implicitly[ClassTag[K]].runtimeClass match {
      case java.lang.Long.TYPE => (new LongKeyHelper(options.segmentSize.toLong)).
                                    asInstanceOf[SortKeyHelper[K]]
    }
  }
}

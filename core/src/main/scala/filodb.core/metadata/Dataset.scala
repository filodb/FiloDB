package filodb.core.metadata

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import filodb.core._
import filodb.core.Types._

/**
 * A dataset is a table with a schema.
 * A dataset is partitioned, partitioning columns controls how data is distributed.
 * Data within a dataset consists of multiple projections, including a main "superprojection"
 * that contains all columns.
 */
case class Dataset(name: String,
                   projections: Seq[Projection],
                   partitionColumns: Seq[ColumnId] = Seq(Dataset.DefaultPartitionColumn),
                   options: DatasetOptions = Dataset.DefaultOptions) {
  def withDatabase(database: String): Dataset =
    this.copy(projections = projections.map(_.withDatabase(database)))

  def withName(newName: String): Dataset =
    this.copy(name = newName, projections = this.projections.map(_.withName(newName)))
}

/**
 * Config options for a table define operational details for the column store and memtable.
 * Every option must have a default!
 */
case class DatasetOptions(chunkSize: Int,
                          metricColumn: String,
                          valueColumn: String) {
  override def toString: String = {
    val map: Map[String, Any] = Map(
                   "chunkSize" -> chunkSize,
                   "metricColumn" -> metricColumn,
                   "valueColumn" -> valueColumn)
    val config = ConfigFactory.parseMap(map.asJava)
    config.root.render(ConfigRenderOptions.concise)
  }
}

object DatasetOptions {
  def fromString(s: String): DatasetOptions =
    fromConfig(ConfigFactory.parseString(s).withFallback(Dataset.DefaultOptionsConfig))

  def fromConfig(config: Config): DatasetOptions =
    DatasetOptions(chunkSize = config.getInt("chunkSize"),
                   metricColumn = config.getString("metricColumn"),
                   valueColumn = config.getString("valueColumn"))
}

/**
 * Contains many helper functions especially pertaining to dataset partitioning.
 */
object Dataset {
  // If a partitioning column is not defined then this refers to a single global partition, and
  // the dataset must fit in one node.
  val DefaultPartitionKey = "/0"
  val DefaultPartitionColumn = s":string $DefaultPartitionKey"

  val DefaultOptions = DatasetOptions(chunkSize = 5000,
                                      metricColumn = "__name__",
                                      valueColumn = "value")
  val DefaultOptionsConfig = ConfigFactory.parseString(DefaultOptions.toString)

  /**
   * Creates a new Dataset with a single superprojection with a single key column and
   * segment column.
   */
  def apply(name: String, keyColumn: String): Dataset =
    Dataset(name, keyColumn, DefaultPartitionColumn)

  def apply(name: String,
            keyColumn: String,
            partitionColumn: String): Dataset =
    Dataset(name, Seq(keyColumn), Seq(partitionColumn))

  def apply(name: String,
            keyColumns: Seq[String],
            partitionColumns: Seq[String]): Dataset =
    Dataset(DatasetRef(name), keyColumns, partitionColumns)

  def apply(ref: DatasetRef,
            keyColumns: Seq[String],
            partitionColumns: Seq[String]): Dataset =
    Dataset(ref.dataset,
            Seq(Projection(0, ref, keyColumns)),
            partitionColumns)
}

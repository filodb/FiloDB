package filodb.core.metadata

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import net.ceedubs.ficus.Ficus._
import org.velvia.filo.RowReader
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.{Try, Success, Failure}

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
case class DatasetOptions(chunkSize: Int) {
  override def toString: String = {
    val map = Map(
                   "chunkSize" -> chunkSize
                 )
    val config = ConfigFactory.parseMap(map.asJava)
    config.root.render(ConfigRenderOptions.concise)
  }
}

object DatasetOptions {
  def fromString(s: String): DatasetOptions = {
    val config = ConfigFactory.parseString(s)
    DatasetOptions(chunkSize = config.getInt("chunkSize"))
  }
}

/**
 * Contains many helper functions especially pertaining to dataset partitioning.
 */
object Dataset {
  // If a partitioning column is not defined then this refers to a single global partition, and
  // the dataset must fit in one node.
  val DefaultPartitionKey = "/0"
  val DefaultPartitionColumn = s":string $DefaultPartitionKey"
  val DefaultSegment = ":string 0"

  val DefaultOptions = DatasetOptions(chunkSize = 5000)

  /**
   * Creates a new Dataset with a single superprojection with a single key column and
   * segment column.
   */
  def apply(name: String,
            keyColumn: String,
            segmentColumn: String,
            partitionColumn: String): Dataset =
    Dataset(name, Seq(keyColumn), segmentColumn, Seq(partitionColumn))

  def apply(name: String,
            keyColumns: Seq[String],
            segmentColumn: String,
            partitionColumns: Seq[String]): Dataset =
    Dataset(DatasetRef(name), keyColumns, segmentColumn, partitionColumns)

  def apply(ref: DatasetRef,
            keyColumns: Seq[String],
            segmentColumn: String,
            partitionColumns: Seq[String]): Dataset =
    Dataset(ref.dataset,
            Seq(Projection(0, ref, keyColumns, segmentColumn)),
            partitionColumns)

  def apply(ref: DatasetRef,
            keyColumns: Seq[String],
            partitionColumns: Seq[String]): Dataset =
    Dataset(ref.dataset,
            Seq(Projection(0, ref, keyColumns, DefaultSegment)),
            partitionColumns)

  def apply(name: String, keyColumn: String, segmentColumn: String): Dataset =
    Dataset(name, keyColumn, segmentColumn, DefaultPartitionColumn)
}

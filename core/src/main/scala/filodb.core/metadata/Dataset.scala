package filodb.core.metadata

import filodb.core.datastore2.Types

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
case class DatasetOptions(chunkSize: Int)

object Dataset {
  // If a partitioning column is not defined then this refers to a single global partition, and
  // the dataset must fit in one node.
  val DefaultPartitionColumn = ":single"
  val DefaultPartitionKey: Types.PartitionKey = "/0"

  val DefaultOptions = DatasetOptions(chunkSize = 1000)

  /**
   * Creates a new Dataset with a single superprojection with a defined sort order.
   */
  def apply(name: String,
            sortColumn: String,
            partitionColumn: String = DefaultPartitionColumn,
            options: DatasetOptions = DefaultOptions): Dataset =
    Dataset(name, Seq(Projection(0, sortColumn)), partitionColumn, options)
}

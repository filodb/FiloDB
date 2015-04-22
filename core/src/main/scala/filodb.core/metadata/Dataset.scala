package filodb.core.metadata

import filodb.core.messages.{Command, Response}

/**
 * A dataset is a table with a schema.
 * A dataset is partitioned into independent partitions.
 */
case class Dataset(name: String,
                   partitions: Set[String])

object Dataset {
  val DefaultPartitionName = "0"
  /**
   * Creates a Dataset case class with the name and a default partition.
   * Note: this does not create a dataset on disk.
   */
  def apply(name: String): Dataset = Dataset(name, Set(DefaultPartitionName))
}

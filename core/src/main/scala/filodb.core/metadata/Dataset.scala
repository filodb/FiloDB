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

  /**
   * Set of low-level dataset commands to send to a datastore actor for I/O
   */

  /**
   * Creates a new dataset in FiloDB if one doesn't exist already.
   * NOTE: does not create partitions.
   * @param name Dataset name to create
   * @returns Success if it succeeds, or AlreadyExists
   */
  case class NewDataset(dataset: String) extends Command

  /**
   * Returns all the information about a dataset, which right now is simply
   * all the partitions.
   * @param name Name of dataset to return
   * @returns Result(dataset) if it succeeds, or NotFound
   */
  case class GetDataset(dataset: String) extends Command

  case class Result(dataset: Dataset) extends Response

  /**
   * Removes a dataset and all its data.  This is a dangerous operation!
   * @param name Dataset name to remove.
   * @returns Success if it succeeds
   */
  case class DeleteDataset(name: String) extends Command
}

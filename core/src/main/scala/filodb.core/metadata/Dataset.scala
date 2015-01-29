package filodb.core.metadata

/**
 * A dataset is a table with a schema.
 * A dataset is partitioned into independent partitions.
 */
case class Dataset(name: String,
                   partitions: Seq[String])

object Dataset {
  val DefaultPartitionName = "0"
  /**
   * Creates a Dataset case class with the name and a default partition.
   * Note: this does not create a dataset on disk.
   */
  def apply(name: String): Dataset = Dataset(name, Seq(DefaultPartitionName))

  /**
   * Set of low-level dataset commands to send to a datastore actor for I/O
   */
  object Command {
    /**
     * Creates a new dataset in FiloDB if one doesn't exist already.
     * NOTE: does not create partitions.
     * @param dataset Dataset to create
     * @returns Created if it succeeds, or AlreadyExists
     */
    case class NewDataset(dataset: Dataset)

    /**
     * Removes a dataset and all its data.  This is a dangerous operation!
     * @param name Dataset name to remove.
     * @returns Deleted if it succeeds
     */
    case class DeleteDataset(name: String)
  }

  /**
   * Set of responses from dataset commands
   */
  object Response {
    case object AlreadyExists
    case object NoSuchDataset
    case object Created
    case object Deleted
  }
}

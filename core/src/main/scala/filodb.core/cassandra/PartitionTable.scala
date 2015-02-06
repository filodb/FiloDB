package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import scala.concurrent.Future

import filodb.core.metadata.{Partition, ShardingStrategy}

/**
 * Represents the "partitions" Cassandra table tracking each partition and its shards
 *
 * NOTE: the hashcode of a Partition object is used for atomic updates and to ensure that
 * the state of a Partition stays intact.
 */
sealed class PartitionTable extends CassandraTable[PartitionTable, Partition] {
  object dataset extends StringColumn(this) with PartitionKey[String]
  object partition extends StringColumn(this) with PartitionKey[String]
  object shardingStrategy extends StringColumn(this)
  object firstRowId extends ListColumn[PartitionTable, Partition, Long](this)
  // NOTE: versionRange is for a _range_ of versions, from one Int to another Int.  It's just
  // encoded as a Long so that we can easily query for a version range together and write it
  // together.
  object versionRange extends ListColumn[PartitionTable, Partition, Long](this)
  object chunkSize extends IntColumn(this)
  object hash extends IntColumn(this)

  // May throw IllegalArgumentException if cannot deserialize shardingStrategy from string
  override def fromRow(row: Row): Partition =
    Partition(dataset(row),
              partition(row),
              ShardingStrategy.deserialize(shardingStrategy(row)),
              firstRowId(row),
              versionRange(row).map(l => PartitionTable.long2ints(l)),
              chunkSize(row))
}

/**
 * Asynchronous methods to operate on partitions.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object PartitionTable extends PartitionTable with SimpleCassandraConnector {
  override val tableName = "partitions"

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  val keySpace = "test"

  import Util._
  import filodb.core.messages._

  def ints2long(x: Int, y: Int): Long = (x.toLong << 32) | (y & 0xffffffffL)
  def long2ints(l: Long): (Int, Int) = ((l >> 32).toInt, l.toInt)

  /**
   * Creates a new partition if it doesn't already exist.
   * @param partition a Partition, with a name unique within the dataset.  It should be empty.
   * @return Success, or AlreadyExists, or NotEmpty/NotValid
   */
  def newPartition(partition: Partition): Future[Response] = {
    if (!partition.isEmpty) return Future(Partition.NotEmpty)
    if (!partition.isValid) return Future(Partition.NotValid)
    insert.value(_.dataset, partition.dataset)
          .value(_.partition, partition.partition)
          .value(_.shardingStrategy, partition.shardingStrategy.serialize())
          .value(_.chunkSize, partition.chunkSize)
          .value(_.hash, partition.hashCode)
          .ifNotExists
          .future().toResponse(AlreadyExists)
  }

  /**
   * Reads the entire state including all shards of a Partition.
   * @param dataset the name of the dataset
   * @param name the name of the partition
   * @return ThePartition, or NotFound
   */
  def getPartition(dataset: String, partition: String): Future[Response] =
    select.where(_.dataset eqs dataset).and(_.partition eqs partition).one()
      .map(opt => opt.map(Partition.ThePartition(_)).getOrElse(NotFound))
      .handleErrors

  /**
   * Adds a shard to an existing Partition, validating the updated partition and also
   * doing a compare-and-write on the hashcode to ensure partition state is consistent
   * @param partition the Partition object to update
   * @param firstRowId the first rowID of the new shard
   * @param versionRange the range of version numbers for the new shard
   * @return Success, NotValid if the new shard is not valid, InconsistentState
   */
  def addShard(partition: Partition,
               firstRowId: Long,
               versionRange: (Int, Int)): Future[Response] = {
    partition.addShard(firstRowId, versionRange) match {
      case None          => Future(Partition.NotValid)
      case Some(newPart) =>
        update.where(_.dataset eqs newPart.dataset).and(_.partition eqs newPart.partition)
              .modify(_.firstRowId append firstRowId)
              .and(_.versionRange append ints2long(versionRange._1, versionRange._2))
              .and(_.hash setTo newPart.hashCode)
              .onlyIf(_.hash eqs partition.hashCode)
              .future().toResponse(InconsistentState)
    }
  }

  // Partial function mapping commands to functions executing them
  val commandMapper: PartialFunction[Command, Future[Response]] = {
    case Partition.NewPartition(partition)          => newPartition(partition)
    case Partition.GetPartition(dataset, partition) => getPartition(dataset, partition)
    case Partition.AddShard(partition, firstRowId, versions) => addShard(partition, firstRowId, versions)
    // case Partition.UpdatePartition(partition) => ???
    // case Partition.DeletePartition(dataset, name) => ???
  }
}
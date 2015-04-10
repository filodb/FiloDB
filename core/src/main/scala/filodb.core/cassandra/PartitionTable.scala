package filodb.core.cassandra

import com.datastax.driver.core.Row
import com.websudos.phantom.Implicits._
import com.websudos.phantom.zookeeper.{SimpleCassandraConnector, DefaultCassandraManager}
import scala.concurrent.Future

import filodb.core.datastore.{Datastore, PartitionApi}
import filodb.core.metadata.{Partition, ShardingStrategy}

/**
 * Represents the "partitions" Cassandra table tracking each partition and its shards
 *
 * NOTE: the hashcode of a Partition object is used for atomic updates and to ensure that
 * the state of a Partition stays intact.
 */
sealed class PartitionTable extends CassandraTable[PartitionTable, Partition] {
  // scalastyle:off
  object dataset extends StringColumn(this) with PartitionKey[String]
  object partition extends StringColumn(this) with PartitionKey[String]
  object shardingStrategy extends StringColumn(this)
  // NOTE: the value of shardVersions is a _range_ of versions, from one Int to another Int.  It's just
  // encoded as a Long so that we can easily query for a version range together and write it together.
  object shardVersions extends MapColumn[PartitionTable, Partition, Long, Long](this)
  object chunkSize extends IntColumn(this)
  object hash extends IntColumn(this)
  // scalastyle:on

  // May throw IllegalArgumentException if cannot deserialize shardingStrategy from string
  override def fromRow(row: Row): Partition =
    Partition(dataset(row),
              partition(row),
              ShardingStrategy.deserialize(shardingStrategy(row)),
              shardVersions(row).mapValues(l => PartitionTable.long2ints(l)),
              chunkSize(row))
}

/**
 * Asynchronous methods to operate on partitions.  All normal errors and exceptions are returned
 * through ErrorResponse types.
 */
object PartitionTable extends PartitionTable with SimpleCassandraConnector with PartitionApi {
  override val tableName = "partitions"

  // TODO: add in Config-based initialization code to find the keyspace, cluster, etc.
  val keySpace = "test"

  import Util._
  import filodb.core.messages._
  import Datastore._

  def ints2long(x: Int, y: Int): Long = (x.toLong << 32) | (y & 0xffffffffL)
  def long2ints(l: Long): (Int, Int) = ((l >> 32).toInt, l.toInt)

  /**
   * Creates a new partition if it doesn't already exist.
   * @param partition a Partition, with a name unique within the dataset.  It should be empty.
   * @return Success, or AlreadyExists, or NotEmpty/NotValid
   */
  def newPartition(partition: Partition): Future[Response] = {
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
      .map(opt => opt.map(ThePartition(_)).getOrElse(NotFound))
      .handleErrors

  /**
   * Adds a shard and version to an existing Partition, validating the updated partition and also
   * doing a compare-and-write on the hashcode to ensure partition state is consistent
   * NOTE: Suggest calling partition.contains() first to avoid unnecessary updates
   * @param partition the Partition object to update
   * @param firstRowId the first rowID of the new shard
   * @param version the version number to add
   * @return Success, InconsistentState
   */
  def addShardVersion(partition: Partition,
                      firstRowId: Long,
                      version: Int): Future[Response] = {
    val newPart = partition.addShardVersion(firstRowId, version)
    val (minVer, maxVer) = newPart.shardVersions(firstRowId)
    update.where(_.dataset eqs newPart.dataset).and(_.partition eqs newPart.partition)
          .modify(_.shardVersions put (firstRowId -> ints2long(minVer, maxVer)))
          .and(_.hash setTo newPart.hashCode)
          .onlyIf(_.hash eqs partition.hashCode)
          .future().toResponse(InconsistentState)
  }

  def getPartitionLock(dataset: String, partition: String, owner: String): Future[Response] = ???
  def releasePartitionLock(dataset: String, partition: String): Future[Response] = ???
  def deletePartition(dataset: String, partition: String): Future[Response] = ???
}
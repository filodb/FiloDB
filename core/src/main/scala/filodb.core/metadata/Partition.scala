package filodb.core.metadata

import filodb.core.messages.{Command, ErrorResponse, Response}

/**
 * A dataset is divided into partitions.  There is one writer per partition
 * and partitions are internally sharded as well by rowID.
 * Partitions keeps state about the shards, each of which should own a non-overlapping range of rowIDs.
 *
 * ==Partition semantics==
 *
 * Each partition and shard stores columns of data in chunks of rows.  The chunks
 * are kept in multiples of chunkSize, and the rowId is the first row number of the chunk.
 * For example, for columns First, Last, and Age, and chunksize of 100, chunks might be
 * stored thus in a shard:
 *   First-0 First-100 First-200  Last-0 Last-100 Last-200  Age-0 Age-100 Age-200
 *
 * Multiple versions coexist within a partition.  Shard definitions do not vary across versions.
 * In fact one common pattern is new versions = new events/rows = new shards.
 *
 * A bigger chunksize results in more efficient space usage, at a cost of higher memory
 * usage, and more expensive individual row replace/deletes.
 *
 * Row-level operations get translated into chunk reads and writes.
 *   appends (row # > lastRowId)      row # on chunk boundary -> new chunk
 *                                    new version -> new chunk
 *                                    otherwise -> replace last chunk
 *   replace (row # < lastRowId)      replace chunk
 *   delete                           replace chunk or delete chunk
 *
 * Rows are addressed by row # or ID.  Custom primary key support will be provided through
 * a PK-to-row-# index.
 *
 * Row #'s never change once a row is appended.
 *
 * ==Partition objects==
 *
 * Partition objects should help answer the following questions:
 * 1. If I'm writing, which shard should I write to?
 * 2. For reading, given a set of versions, which shards should I read from? (or shard-version pairs)
 *
 * TODO: make Partition class only cache some shards in memory so it can scale to millions of shards.
 * This current design will scale to a billion rows per partition, which is probably fine for a while.
 * Amongst other changes: hash currently assumes knowledge of all shards; PartitionTable would need to
 * load shard info not in memory back into memory; maybe shardVersions needs to be a cacheMap etc.
 */
case class Partition(dataset: String,
                     partition: String,
                     shardingStrategy: ShardingStrategy = ShardingStrategy.DefaultStrategy,
                     shardVersions: Map[Long, (Int, Int)] = Map.empty,
                     chunkSize: Int = Partition.DefaultChunkSize) {
  def isEmpty: Boolean = shardVersions.isEmpty

  def isValid: Boolean = chunkSize > 0

  /**
   * Returns true if this Partition contains the shard starting at firstRowId and version.
   */
  def contains(firstRowId: Long, version: Int): Boolean =
    (shardVersions contains firstRowId) &&
    (version >= shardVersions(firstRowId)._1 && version <= shardVersions(firstRowId)._2)

  /**
   * Adds a shard and a version to the partition. Does no validation of shard and version -- that is
   * the job of the shardingStrategy.
   * NOTE: don't call this directly, instead send AddShardVersion message to MetadataActor, which takes
   * care of scenarios where not all shards are in memory.
   * @return a new Partition instance with the shard and version info added.
   */
  def addShardVersion(firstRowId: Long, version: Int): Partition = {
    if (shardVersions contains firstRowId) {
      val (minVersion, maxVersion) = shardVersions(firstRowId)
      this.copy(shardVersions = this.shardVersions +
                  (firstRowId -> (Math.min(version, minVersion) -> Math.max(version, maxVersion))))
    } else {
      this.copy(shardVersions = this.shardVersions + (firstRowId -> (version -> version)))
    }
  }

  override def toString: String = s"$dataset-$partition"
}

object Partition {
  val DefaultChunkSize = 1000
}
package filodb.core.store

import java.nio.ByteBuffer

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core._
import filodb.core.memstore.TimeSeriesShard
import filodb.core.metadata.Dataset
import filodb.core.query._


/**
 * RawChunkSource is the base trait for a source of chunks given a `PartitionScanMethod` and a
 * `ChunkScanMethod`.  It is the basis for querying and reading out of raw chunks.  Most ChunkSources should
 * implement this trait instead of the more advanced ChunkSource API.
 *
 * Besides the basic methods here, see `package.scala` for derivative methods including aggregate
 * and row iterator based methods (intended for things like Spark)
 */
trait RawChunkSource {
  def stats: ChunkSourceStats

  /**
   * Determines how to split the scanning of a dataset across a columnstore.
   * Used only for something like Spark that has to distribute scans across a cluster.
   * @param dataset the name of the dataset to determine splits for
   * @param splitsPerNode the number of splits to target per node.  May not actually be possible.
   * @return a Seq[ScanSplit]
   */
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit]

  /**
   * Reads and returns raw chunk data according to the method. ChunkSources implementing this method can use
   * any degree of parallelism/async under the covers to get the job done efficiently.
   * @param dataset the Dataset to read from
   * @param columnIDs the set of column IDs to read back.  Not used for the memstore, but affects what columns are
   *                  read back from persistent store.
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return an Observable of RawPartDatas
   */
  /**
   * Implemented by lower-level persistent ChunkSources to return "raw" partition data
   */
  def readRawPartitions(dataset: Dataset,
                        columnIDs: Seq[Types.ColumnId],
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData]
}

/**
 * Raw data, used for RawChunkSource, not yet loaded into offheap memory
 * @param infoBytes the raw bytes from a ChunkSetInfo
 * @param vectors ByteBuffers for each chunk vector, in order from data column 0 on up.  Size == # of data columns
 */
final case class RawChunkSet(infoBytes: Array[Byte], vectors: Array[ByteBuffer])

/**
 * Raw data for a partition, with one RawChunkSet per ID read
 */
final case class RawPartData(partitionKey: Array[Byte], chunkSets: Seq[RawChunkSet])

trait ChunkSource extends RawChunkSource {
  /**
   * Scans and returns data in partitions according to the method.  The partitions are ready to be queried.
   * FiloPartitions contains chunks in offheap memory.
   * This is a higher level method that builds off of RawChunkSource/readRawPartitions, but must handle moving
   * memory to offheap and returning partitions.
   * @param dataset the Dataset to read from
   * @param columnIDs the set of column IDs to read back.  Not used for the memstore, but affects what columns are
   *                  read back from persistent store.
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return an Observable over ReadablePartition
   */
  def scanPartitions(dataset: Dataset,
                     columnIDs: Seq[Types.ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod = AllChunkScan): Observable[ReadablePartition]

  // internal method to find # of groups in a dataset
  def groupsInDataset(dataset: Dataset): Int

  /**
   * Returns a stream of RangeVectors's.  Good for per-partition (or time series) processing.
   *
   * @param dataset the Dataset to read from
   * @param columnIDs the set of column IDs to read back.  Order determines the order of columns read back
   *                in each row.  These are the IDs from the Column instances.
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return an Observable of RangeVectors
   */
  def rangeVectors(dataset: Dataset,
                   columnIDs: Seq[Types.ColumnId],
                   partMethod: PartitionScanMethod,
                   chunkMethod: ChunkScanMethod): Observable[RangeVector] = {
    val ids = columnIDs.toArray
    val partCols = dataset.infosFromIDs(dataset.partitionColumns.map(_.id))
    val numGroups = groupsInDataset(dataset)
    scanPartitions(dataset, columnIDs, partMethod, chunkMethod)
      .filter(_.hasChunks(chunkMethod))
      .map { partition =>
        stats.incrReadPartitions(1)
        val subgroup = TimeSeriesShard.partKeyGroup(dataset.partKeySchema, partition.partKeyBase,
                                                    partition.partKeyOffset, numGroups)
        val key = new PartitionRangeVectorKey(partition.partKeyBase, partition.partKeyOffset,
                                              dataset.partKeySchema, partCols, partition.shard, subgroup)
        RawDataRangeVector(key, partition, chunkMethod, ids)
      }
  }
}

final case class PartKeyTimeBucketSegment(segmentId: Int, segment: ByteBuffer)

/**
 * Responsible for uploading RawPartDatas to offheap memory and creating a queryable ReadablePartition
 */
trait RawToPartitionMaker {
  def populateRawChunks(rawPartition: RawPartData): Task[ReadablePartition]
}

/**
 * Just an example of how a RawChunkSource could be mixed with RawToPartitionMaker to create a complete ChunkSource.
 * TODO: actually make this a complete one
 */
trait DefaultChunkSource extends ChunkSource {
  /**
   * Should return a shard-specific RawToPartitionMaker
   */
  def partMaker(dataset: Dataset, shard: Int): RawToPartitionMaker

  val singleThreadPool = Scheduler.singleThread("make-partition")

  // Default implementation takes every RawPartData and turns it into a regular TSPartition
  def scanPartitions(dataset: Dataset,
                     columnIDs: Seq[Types.ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod = AllChunkScan): Observable[ReadablePartition] = {
    readRawPartitions(dataset, columnIDs, partMethod, chunkMethod)
      // NOTE: this executes the partMaker single threaded.  Needed for now due to concurrency constraints.
      // In the future optimize this if needed.
      .mapAsync { rawPart => partMaker(dataset, partMethod.shard).populateRawChunks(rawPart)
                               .executeOn(singleThreadPool) }
  }
}

/**
 * Statistics for a ChunkSource.  Some of this is used by unit tests.
 */
class ChunkSourceStats {
  private val readPartitionsCtr  = Kamon.counter("read-partitions")
  private val readChunksetsCtr   = Kamon.counter("read-chunksets")
  private val chunkNoInfoCtr     = Kamon.counter("read-chunks-with-no-info")
  var readChunkSets: Int = 0
  var readPartitions: Int = 0

  def incrReadPartitions(numPartitions: Int): Unit = {
    readPartitionsCtr.increment(numPartitions)
    readPartitions += numPartitions
  }

  def incrReadChunksets(): Unit = {
    readChunksetsCtr.increment
    readChunkSets += 1
  }

  def incrChunkWithNoInfo(): Unit = { chunkNoInfoCtr.increment }
}

final case class SingleChunkInfo(id: Types.ChunkID, colNo: Int, bytes: ByteBuffer)

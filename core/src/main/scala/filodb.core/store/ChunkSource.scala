package filodb.core.store

import java.nio.ByteBuffer

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.reactive.Observable

import filodb.core._
import filodb.core.memstore.{PartLookupResult, SchemaMismatch, TimeSeriesShard}
import filodb.core.memstore.ratelimit.CardinalityRecord
import filodb.core.metadata.{Schema, Schemas}
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
   * @param ref the DatasetRef to read chunks from
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return an Observable of RawPartDatas
   */
  /**
   * Implemented by lower-level persistent ChunkSources to return "raw" partition data
   */
  def readRawPartitions(ref: DatasetRef,
                        maxChunkTime: Long,
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
final case class RawPartData(partitionKey: Array[Byte], chunkSetsTimeOrdered: Seq[RawChunkSet])

trait ChunkSource extends RawChunkSource with StrictLogging {

  /**
    * True if this store is in the mode of serving downsampled data.
    * This is used to switch ingestion and query behaviors for downsample cluster.
    */
  def isDownsampleStore: Boolean


  /**
   * Scans and returns data in partitions according to the method.  The partitions are ready to be queried.
   * FiloPartitions contains chunks in offheap memory.
   * This is a higher level method that builds off of RawChunkSource/readRawPartitions, but must handle moving
   * memory to offheap and returning partitions.
   * @param ref the DatasetRef to read from
   * @param columnIDs the set of column IDs to read back.  Not used for the memstore, but affects what columns are
   *                  read back from persistent store.
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return an Observable over ReadablePartition
   */
  def scanPartitions(ref: DatasetRef,
                     columnIDs: Seq[Types.ColumnId],
                     partMethod: PartitionScanMethod,
                     chunkMethod: ChunkScanMethod = AllChunkScan,
                     querySession: QuerySession): Observable[ReadablePartition] = {
    logger.debug(s"scanPartitions dataset=$ref shard=${partMethod.shard} " +
      s"partMethod=$partMethod chunkMethod=$chunkMethod")
    scanPartitions(ref, lookupPartitions(ref, partMethod, chunkMethod, querySession), columnIDs, querySession)
  }


  // Internal API that needs to actually be implemented
  def scanPartitions(ref: DatasetRef,
                     lookupRes: PartLookupResult,
                     colIds: Seq[Types.ColumnId],
                     querySession: QuerySession): Observable[ReadablePartition]

  // internal method to find # of groups in a dataset
  def groupsInDataset(ref: DatasetRef): Int

  /**
   * Returns the schemas registered for a given dataset.
   */
  def schemas(ref: DatasetRef): Option[Schemas]

  /**
   * Acquire shared lock to shard(s) for given dataset/partMethod. Acquisition of this lock
   * indicates that readers are reading chunks from the TimeSeriesStore. Writers wait for readers
   * to complete before performing writes to the store.
   *
   * Acquired lock is placed in querySession. It is the job of the client to release the lock.
   *
   * @throws QueryTimeoutException if shared lock was not acquired within timeoutMs
   */
  def acquireSharedLock(ref: DatasetRef,
                        shardNum: Int,
                        querySession: QuerySession): Unit

  /**
   * Check if shard is ready for query.
   *
   * If not and if partial results allowed, it allows query to continue
   * by marking the session for partial results.
   *
   * If not and if partial results are not allowed, ServiceUnavailableException is thrown
   */
  def checkReadyForQuery(ref: DatasetRef,
                         shard: Int,
                         querySession: QuerySession): Unit

  /**
   * Looks up TSPartitions from filters.
   * Also used to discover the first possible schema ID for schema discovery.
   *
   * @param ref the DatasetRef to read from
   * @param partMethod which partitions to scan
   * @param chunkMethod which chunks within a partition to scan
   * @return an PartLookupResult
   */
  def lookupPartitions(ref: DatasetRef,
                       partMethod: PartitionScanMethod,
                       chunkMethod: ChunkScanMethod,
                       querySession: QuerySession): PartLookupResult

  /**
   * Returns a stream of RangeVectors's.  Good for per-partition (or time series) processing.
   *
   * @param lookupRes: PartLookupResult from lookupPartitions()
   * @param schema the Schema to read data for
   * @param filterSchemas if true, partitions are filtered only for the desired schema.
   *                      if false, then every partition is checked to ensure it is that schema, otherwise an error
   *                      is returned.  Set to false for discovering schemas dynamically using lookupPartitions()
   * @return an Observable of RangeVectors
   */
  def rangeVectors(ref: DatasetRef,
                   lookupRes: PartLookupResult,
                   columnIDs: Seq[Types.ColumnId],
                   schema: Schema,
                   filterSchemas: Boolean,
                   querySession: QuerySession): Observable[RangeVector] = {
    val ids = columnIDs.toArray
    val partCols = schema.infosFromIDs(schema.partition.columns.map(_.id))
    val numGroups = groupsInDataset(ref)

    val filteredParts = if (filterSchemas) {
      scanPartitions(ref, lookupRes, columnIDs, querySession)
        .filter { p => p.schema.schemaHash == schema.schemaHash && p.hasChunks(lookupRes.chunkMethod) }
    } else {
      lookupRes.firstSchemaId match {
        case Some(reqSchemaId) =>
          scanPartitions(ref, lookupRes, columnIDs, querySession).filter { p =>
            if (!Utils.doesSchemaMatchOrBackCompatibleHistograms(
                  p.schema.name, p.schema.schemaHash, // current partition
                  Schemas.global.schemaName(reqSchemaId), reqSchemaId) // requested schema
            ) {
              throw SchemaMismatch(Schemas.global.schemaName(reqSchemaId), p.schema.name, getClass.getSimpleName)
            }
            p.hasChunks(lookupRes.chunkMethod)
          }
        case None =>
          Observable.empty
      }
    }

    filteredParts.map { partition =>
      stats.incrReadPartitions(1)
      val subgroup = TimeSeriesShard.partKeyGroup(schema.partKeySchema, partition.partKeyBase,
                                                  partition.partKeyOffset, numGroups)
      val key = PartitionRangeVectorKey(Left(partition),
                                        schema.partKeySchema, partCols, partition.shard,
                                        subgroup, partition.partID, schema.name)
      RawDataRangeVector(
        key, partition, lookupRes.chunkMethod, ids, lookupRes.dataBytesScannedCtr,
        querySession.qContext.plannerParams.enforcedLimits.rawScannedBytes, querySession.qContext.queryId
      )
    }
  }

  val FILODB_PARTITION_KEY = "filodb.partition"

  /**
   * Additional check to ensure the query is for the given filodb partition
   *
   * @param queryContext QueryContext containing traceInfo of the originating partition
   * @return
   */
  def isCorrectPartitionForCardinalityQuery(queryContext: QueryContext, filodbPartition: String): Boolean = {
    if (
      queryContext.traceInfo.contains(FILODB_PARTITION_KEY) &&
        (!queryContext.traceInfo.get(FILODB_PARTITION_KEY).get.isEmpty) &&
        (queryContext.traceInfo.get(FILODB_PARTITION_KEY).get != filodbPartition)
    ) {
      return false
    }
    true
  }

  def scanTsCardinalities(queryContext: QueryContext, ref: DatasetRef, shard: Seq[Int],
                          shardKeyPrefix: Seq[String], depth: Int): Seq[CardinalityRecord]

}

/**
 * Responsible for uploading RawPartDatas to offheap memory and creating a queryable ReadablePartition
 */
trait RawToPartitionMaker {
  def populateRawChunks(rawPartition: RawPartData): Task[ReadablePartition]
}

/**
 * Statistics for a ChunkSource.  Some of this is used by unit tests.
 */
class ChunkSourceStats {
  private val readPartitionsCtr  = Kamon.counter("read-partitions").withoutTags
  private val readChunksetsCtr   = Kamon.counter("read-chunksets").withoutTags
  private val chunkNoInfoCtr     = Kamon.counter("read-chunks-with-no-info").withoutTags
  var readChunkSets: Int = 0
  var readPartitions: Int = 0

  def incrReadPartitions(numPartitions: Int): Unit = {
    readPartitionsCtr.increment(numPartitions)
    readPartitions += numPartitions
  }

  def incrReadChunksets(): Unit = {
    readChunksetsCtr.increment()
    readChunkSets += 1
  }

  def incrChunkWithNoInfo(): Unit = { chunkNoInfoCtr.increment() }
}

final case class SingleChunkInfo(id: Types.ChunkID, colNo: Int, bytes: ByteBuffer)

package filodb.cassandra.columnstore

import com.datastax.driver.core.TokenRange
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store._
import filodb.core.metadata.{Column, Projection, RichProjection}

/**
 * Implementation of a column store using Apache Cassandra tables.
 * This class must be thread-safe as it is intended to be used concurrently.
 *
 * Both the instances of the Segment* table classes above as well as ChunkRowMap entries
 * are cached for faster I/O.
 *
 * ==Configuration==
 * {{{
 *   cassandra {
 *     hosts = ["1.2.3.4", "1.2.3.5"]
 *     port = 9042
 *     keyspace = "my_cass_keyspace"
 *     username = ""
 *     password = ""
 *     read-timeout = 12 s    # default read timeout of 12 seconds
 *     connect-timeout = 5 s
 *   }
 *   columnstore {
 *     tablecache-size = 50    # Number of cache entries for C* for ChunkTable etc.
 *     segment-cache-size = 1000    # Number of segments to cache
 *     chunk-batch-size = 16
 *   }
 * }}}
 *
 * ==Constructor Args==
 * @param config see the Configuration section above for the needed config
 * @param readEc An ExecutionContext for reads.  This must be separate from writes to prevent deadlocks.
 * @param ec An ExecutionContext for futures for writes.  See this for a way to do backpressure with futures:
 *        http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
 */
class CassandraColumnStore(val config: Config, val readEc: ExecutionContext)
                          (implicit val ec: ExecutionContext)
extends CachedMergingColumnStore with CassandraColumnStoreScanner with StrictLogging {
  import filodb.core.store._
  import Types._
  import collection.JavaConverters._

  val segmentCacheSize = config.getInt("columnstore.segment-cache-size")
  val chunkBatchSize = config.getInt("columnstore.chunk-batch-size")
  logger.info(s"Starting CassandraColumnStore with config $cassandraConfig")

  val segmentCache = LruCache[Segment](segmentCacheSize)

  val mergingStrategy = new AppendingChunkMergingStrategy(this)

  /**
   * Initializes the column store for a given dataset projection.  Must be called once before appending
   * segments to that projection.
   */
  def initializeProjection(projection: Projection): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(projection.dataset)
    val rowMapTable = getOrCreateRowMapTable(projection.dataset)
    for { ctResp                    <- chunkTable.initialize()
          rmtResp                   <- rowMapTable.initialize() } yield { rmtResp }
  }

  /**
   * Clears all data from the column store for that given projection.
   */
  def clearProjectionDataInner(projection: Projection): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(projection.dataset)
    val rowMapTable = getOrCreateRowMapTable(projection.dataset)
    for { ctResp                    <- chunkTable.clearAll()
          rmtResp                   <- rowMapTable.clearAll() } yield { rmtResp }
  }

  /**
   * Implementations of low-level storage primitives
   */
  def writeChunks(dataset: TableName,
                  partition: BinaryPartition,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    chunkTable.writeChunks(partition, version, segmentId, chunks)
  }

  def writeChunkRowMap(dataset: TableName,
                       partition: BinaryPartition,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response] = {
    val (chunkIds, rowNums) = chunkRowMap.serialize()
    val rowMapTable = getOrCreateRowMapTable(dataset)
    rowMapTable.writeChunkMap(partition, version, segmentId,
                              chunkIds, rowNums, chunkRowMap.nextChunkId)
  }

  def shutdown(): Unit = {
    clusterConnector.shutdown()
  }

  /**
   * Splits scans of a dataset across multiple token ranges.
   * @param splitsPerNode  - how much parallelism or ways to divide a token range on each node
   * @return each split will have token_start, token_end, replicas filled in
   */
  def getScanSplits(dataset: TableName, splitsPerNode: Int = 1): Seq[ScanSplit] = {
    val metadata = clusterConnector.session.getCluster.getMetadata
    require(splitsPerNode >= 1, s"Must specify at least 1 splits_per_node, got $splitsPerNode")
    val tokensByReplica = metadata.getTokenRanges.asScala.toSeq.groupBy { tokenRange =>
      metadata.getReplicas(clusterConnector.keySpace.name, tokenRange)
    }
    val tokenRanges = for { key <- tokensByReplica.keys } yield {
      if (tokensByReplica(key).size > 1) {
        // NOTE: If vnodes are enabled, like in DSE, the token ranges are not contiguous and cannot
        // therefore be merged. In that case just give up.  Lesson: don't use a lot of vnodes.
        try {
          tokensByReplica(key).sorted.reduceLeft(_.mergeWith(_)).splitEvenly(splitsPerNode).asScala
        } catch {
          case e: IllegalArgumentException =>
            logger.warn(s"Non-contiguous token ranges, cannot merge.  This is probably because of vnodes.")
            logger.warn("Reduce the number of vnodes for better performance.")
            tokensByReplica(key)
        }
      }
      else {
        tokensByReplica(key).flatMap { range => range.splitEvenly(splitsPerNode).asScala }
      }
    }
    val tokensComplete = tokenRanges.flatMap { token => token } .toSeq
    tokensComplete.map { tokenRange =>
      val replicas = metadata.getReplicas(clusterConnector.keySpace.name, tokenRange).asScala
      CassandraTokenRangeSplit(tokenRange.getStart.toString,
                               tokenRange.getEnd.toString,
                               replicas.map(_.getSocketAddress).toSet)
    }
  }
}

case class CassandraTokenRangeSplit(startToken: String, endToken: String,
                                    replicas: Set[InetSocketAddress]) extends ScanSplit {
  def hostnames: Set[String] = replicas.map(_.getHostName)
}

trait CassandraColumnStoreScanner extends ColumnStoreScanner with StrictLogging {
  import filodb.core.store._
  import Types._
  import collection.JavaConverters._

  def config: Config

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = new ConcurrentLinkedHashMap.Builder[TableName, ChunkTable].
                          maximumWeightedCapacity(tableCacheSize).build
  val rowMapTableCache = new ConcurrentLinkedHashMap.Builder[TableName, ChunkRowMapTable].
                          maximumWeightedCapacity(tableCacheSize).build

  protected val clusterConnector = new FiloCassandraConnector {
    def config: Config = cassandraConfig
  }

  def readChunks(dataset: TableName,
                 columns: Set[ColumnId],
                 keyRange: BinaryKeyRange,
                 version: Int)(implicit ec: ExecutionContext): Future[Seq[ChunkedData]] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    Future.sequence(columns.toSeq.map(
                    chunkTable.readChunks(keyRange.partition, version, _,
                                          keyRange.start, keyRange.end,
                                          keyRange.endExclusive)))
  }

  def scanChunkRowMaps(projection: RichProjection,
                       version: Int,
                       method: ScanMethod)
                      (implicit ec: ExecutionContext):
    Future[Iterator[SegmentIndex[projection.PK, projection.SK]]] = {
    val rowMapTable = getOrCreateRowMapTable(projection.datasetName)
    val futCrmRecords = method match {
      case SinglePartitionScan(partition) =>
        val binPart = projection.partitionType.toBytes(partition.asInstanceOf[projection.PK])
        rowMapTable.getChunkMaps(binPart, version)

      case SinglePartitionRangeScan(k) =>
        rowMapTable.getChunkMaps(projection.toBinaryKeyRange(k), version)

      case FilteredPartitionScan(CassandraTokenRangeSplit(startToken, endToken, _), filterFunc) =>
        rowMapTable.scanChunkMaps(version, startToken, endToken)
          .map(_.filter { crm => filterFunc(projection.partitionType.fromBytes(crm.binPartition)) })

      case other: ScanMethod => ???
    }
    futCrmRecords.map { crmIt =>
      crmIt.map { case ChunkRowMapRecord(binPart, segmentId, chunkIds, rowNums, nextChunkId) =>
        toSegIndex(projection, (binPart, segmentId, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId)))
      }
    }
  }

  import java.util.function.{Function => JFunction}
  import scala.language.implicitConversions

  implicit def scalaFuncToJavaFunc[T, R](func1: Function[T, R]): JFunction[T, R] =
    new JFunction[T, R] {
      override def apply(t: T): R = func1.apply(t)
    }

  def getOrCreateChunkTable(dataset: TableName): ChunkTable = {
    chunkTableCache.computeIfAbsent(dataset,
                                    { (dataset: TableName) =>
                                      logger.debug(s"Creating a new ChunkTable for dataset $dataset")
                                      new ChunkTable(dataset, clusterConnector) })
  }

  def getOrCreateRowMapTable(dataset: TableName): ChunkRowMapTable = {
    rowMapTableCache.computeIfAbsent(dataset,
                                     { (dataset: TableName) =>
                                       logger.debug(s"Creating a new ChunkRowMapTable for dataset $dataset")
                                       new ChunkRowMapTable(dataset, clusterConnector) })
  }
}

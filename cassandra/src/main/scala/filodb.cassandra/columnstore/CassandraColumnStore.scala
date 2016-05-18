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
  logger.info(s"Starting CassandraColumnStore with config ${cassandraConfig.withoutPath("password")}")

  val segmentCache = LruCache[Segment](segmentCacheSize)

  val mergingStrategy = new AppendingChunkMergingStrategy(this)

  /**
   * Initializes the column store for a given dataset projection.  Must be called once before appending
   * segments to that projection.
   */
  def initializeProjection(projection: Projection): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(projection.dataset)
    clusterConnector.createKeyspace(chunkTable.keyspace)
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

  def dropDataset(dataset: DatasetRef): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    val rowMapTable = getOrCreateRowMapTable(dataset)
    for { ctResp                    <- chunkTable.drop() if ctResp == Success
          rmtResp                   <- rowMapTable.drop() if rmtResp == Success }
    yield {
      chunkTableCache.remove(dataset)
      rowMapTableCache.remove(dataset)
      rmtResp
    }
  }

  /**
   * Implementations of low-level storage primitives
   */
  def writeChunks(dataset: DatasetRef,
                  partition: BinaryPartition,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    chunkTable.writeChunks(partition, version, segmentId, chunks)
  }

  def writeChunkRowMap(dataset: DatasetRef,
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
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] = {
    val metadata = clusterConnector.session.getCluster.getMetadata
    val keyspace = clusterConnector.keySpaceName(dataset)
    require(splitsPerNode >= 1, s"Must specify at least 1 splits_per_node, got $splitsPerNode")

    val tokensByReplica = metadata.getTokenRanges.asScala.toSeq.groupBy { tokenRange =>
      metadata.getReplicas(keyspace, tokenRange)
    }

    val tokenRangeGroups: Seq[Seq[TokenRange]] = {
      tokensByReplica.flatMap { case (replicaKey, rangesPerReplica) =>
        // First, sort tokens in each replica group so that adjacent tokens are next to each other
        val sortedRanges = rangesPerReplica.sorted

        // If token ranges can be merged (adjacent), merge them and divide evenly into splitsPerNode
        try {
          // There is no "empty" or "zero" TokenRange, so we have to treat single range separately.
          val singleRange =
            if (sortedRanges.length > 1) { sortedRanges.reduceLeft(_.mergeWith(_)) }
            else                         { sortedRanges.head }
          // We end up with splitsPerNode sets of single token ranges
          singleRange.splitEvenly(splitsPerNode).asScala.map(Seq(_))

        // If they cannot be merged (DSE / vnodes), then try to group ranges into splitsPerNode groups
        // This is less efficient but less partitions is still much much better.  Having a huge
        // number of partitions is very slow for Spark, and we want to honor splitsPerNode.
        } catch {
          case e: IllegalArgumentException =>
            // First range goes to split 0, second goes to split 1, etc, capped by splits
            sortedRanges.zipWithIndex.groupBy(_._2 % splitsPerNode).values.map(_.map(_._1)).toSeq
        }
      }.toSeq
    }

    tokenRangeGroups.map { tokenRanges =>
      val replicas = metadata.getReplicas(keyspace, tokenRanges.head).asScala
      CassandraTokenRangeSplit(tokenRanges.map { range => (range.getStart.toString, range.getEnd.toString) },
                               replicas.map(_.getSocketAddress).toSet)
    }
  }
}

case class CassandraTokenRangeSplit(tokens: Seq[(String, String)],
                                    replicas: Set[InetSocketAddress]) extends ScanSplit {
  // NOTE: You need both the host string and the IP address for Spark's locality to work
  def hostnames: Set[String] = replicas.flatMap(r => Set(r.getHostString, r.getAddress.getHostAddress))
}

trait CassandraColumnStoreScanner extends ColumnStoreScanner with StrictLogging {
  import filodb.core.store._
  import Types._
  import collection.JavaConverters._

  def config: Config

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = new ConcurrentLinkedHashMap.Builder[DatasetRef, ChunkTable].
                          maximumWeightedCapacity(tableCacheSize).build
  val rowMapTableCache = new ConcurrentLinkedHashMap.Builder[DatasetRef, ChunkRowMapTable].
                          maximumWeightedCapacity(tableCacheSize).build

  protected val clusterConnector = new FiloCassandraConnector {
    def config: Config = cassandraConfig
    def ec: ExecutionContext = readEc
  }

  def readChunks(dataset: DatasetRef,
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
    val rowMapTable = getOrCreateRowMapTable(projection.datasetRef)
    val futCrmRecords = method match {
      case SinglePartitionScan(partition) =>
        val binPart = projection.partitionType.toBytes(partition.asInstanceOf[projection.PK])
        rowMapTable.getChunkMaps(binPart, version)

      case SinglePartitionRangeScan(k) =>
        rowMapTable.getChunkMaps(projection.toBinaryKeyRange(k), version)

      case FilteredPartitionScan(CassandraTokenRangeSplit(tokens, _), filterFunc) =>
        rowMapTable.scanChunkMaps(version, tokens)
          .map(_.filter { crm => filterFunc(projection.partitionType.fromBytes(crm.binPartition)) })

      case FilteredPartitionRangeScan(CassandraTokenRangeSplit(tokens, _),
                                      segRange, filterFunc) =>
        val binRange = projection.toBinarySegRange(segRange)
        rowMapTable.scanChunkMapsRange(version, tokens, binRange.start, binRange.end)
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

  def getOrCreateChunkTable(dataset: DatasetRef): ChunkTable = {
    chunkTableCache.computeIfAbsent(dataset,
                                    { (dataset: DatasetRef) =>
                                      logger.debug(s"Creating a new ChunkTable for dataset $dataset")
                                      new ChunkTable(dataset, clusterConnector)(readEc) })
  }

  def getOrCreateRowMapTable(dataset: DatasetRef): ChunkRowMapTable = {
    rowMapTableCache.computeIfAbsent(dataset,
                                     { (dataset: DatasetRef) =>
                                       logger.debug(s"Creating a new ChunkRowMapTable for dataset $dataset")
                                       new ChunkRowMapTable(dataset, clusterConnector)(readEc) })
  }
}

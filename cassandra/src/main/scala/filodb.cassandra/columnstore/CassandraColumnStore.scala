package filodb.cassandra.columnstore

import com.datastax.driver.core.TokenRange
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.HashSet
import kamon.trace.{TraceContext, Tracer}

import scala.concurrent.{ExecutionContext, Future}
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
extends ColumnStore with CassandraColumnStoreScanner with StrictLogging {
  import filodb.core.store._
  import Types._
  import collection.JavaConverters._
  import Perftools._

  logger.info(s"Starting CassandraColumnStore with config ${cassandraConfig.withoutPath("password")}")

  /**
   * Initializes the column store for a given dataset projection.  Must be called once before appending
 * segments to that projection.
   */
  def initializeProjection(projection: Projection): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(projection.dataset)
    clusterConnector.createKeyspace(chunkTable.keyspace)
    val indexTable = getOrCreateIndexTable(projection.dataset)
    for { ctResp                    <- chunkTable.initialize()
          rmtResp                   <- indexTable.initialize() } yield { rmtResp }
  }

  /**
   * Clears all data from the column store for that given projection.
   */
  def clearProjectionData(projection: Projection): Future[Response] = {
    logger.info(s"Clearing all columnar projection data for dataset ${projection.dataset}")
    val chunkTable = getOrCreateChunkTable(projection.dataset)
    val indexTable = getOrCreateIndexTable(projection.dataset)
    for { ctResp                    <- chunkTable.clearAll()
          rmtResp                   <- indexTable.clearAll() } yield { rmtResp }
  }

  def dropDataset(dataset: DatasetRef): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    val indexTable = getOrCreateIndexTable(dataset)
    for { ctResp                    <- chunkTable.drop() if ctResp == Success
          rmtResp                   <- indexTable.drop() if rmtResp == Success }
    yield {
      chunkTableCache.remove(dataset)
      indexTableCache.remove(dataset)
      rmtResp
    }
  }

  def appendSegment(projection: RichProjection,
                    segment: ChunkSetSegment,
                    version: Int): Future[Response] = Tracer.withNewContext("append-segment") {
    val ctx = Tracer.currentContext
    stats.segmentAppend()
    if (segment.chunkSets.isEmpty) {
      stats.segmentEmpty()
      return(Future.successful(NotApplied))
    }
    for { writeChunksResp <- writeChunks(projection.datasetRef, version, segment, ctx)
          writeIndexResp  <- writeIndices(projection.datasetRef, version, segment, ctx)
                               if writeChunksResp == Success
    } yield {
      ctx.finish()
      writeIndexResp
    }
  }

  private def writeChunks(dataset: DatasetRef,
                          version: Int,
                          segment: ChunkSetSegment,
                          ctx: TraceContext): Future[Response] = {
    asyncSubtrace(ctx, "write-chunks", "ingestion") {
      val binPartition = segment.binaryPartition
      val segmentId = segment.segmentId
      val chunkTable = getOrCreateChunkTable(dataset)
      Future.traverse(segment.chunkSets) { chunkSet =>
        chunkTable.writeChunks(binPartition, version, segmentId, chunkSet.info.id, chunkSet.chunks, stats)
      }.map { responses => responses.head }
    }
  }

  private def writeIndices(dataset: DatasetRef,
                           version: Int,
                           segment: ChunkSetSegment,
                           ctx: TraceContext): Future[Response] = {
    asyncSubtrace(ctx, "write-index", "ingestion") {
      val indexTable = getOrCreateIndexTable(dataset)
      val indices = segment.chunkSets.map { case ChunkSet(info, skips, _) =>
        (info.id, ChunkSetInfo.toBytes(info, skips))
      }
      indexTable.writeIndices(segment.binaryPartition, version, segment.segmentId, indices, stats)
    }
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

    val tokenRanges = unwrapTokenRanges(metadata.getTokenRanges.asScala.toSeq)
    logger.debug(s"unwrapTokenRanges: ${tokenRanges.toString()}")
    val tokensByReplica = tokenRanges.groupBy { tokenRange =>
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

  def unwrapTokenRanges(wrappedRanges : Seq[TokenRange]): Seq[TokenRange] =
    wrappedRanges.flatMap(_.unwrap().asScala.toSeq)
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
  import Iterators._

  def config: Config

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = concurrentCache[DatasetRef, ChunkTable](tableCacheSize)
  val indexTableCache = concurrentCache[DatasetRef, IndexTable](tableCacheSize)

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

  def multiPartRangeScan(projection: RichProjection,
                         keyRanges: Seq[KeyRange[_, _]],
                         indexTable: IndexTable,
                         version: Int)
                        (implicit ec: ExecutionContext): Future[Iterator[IndexRecord]] = {
    val futureRows = keyRanges.map { range => {
      indexTable.getIndices(projection.toBinaryKeyRange(range), version)
    }}
    Future.sequence(futureRows).map { rows => rows.flatten.toIterator }
  }

  def multiPartScan(projection: RichProjection,
                    partitions: Seq[Any],
                    indexTable: IndexTable,
                    version: Int)
                   (implicit ec: ExecutionContext): Future[Iterator[IndexRecord]] = {
    val futureRows = partitions.map { partition => {
      val binPart = projection.partitionType.toBytes(partition.asInstanceOf[projection.PK])
      indexTable.getIndices(binPart, version)
    }}
    Future.sequence(futureRows).map { rows => rows.flatten.toIterator }
  }

  def scanIndices(projection: RichProjection,
                  version: Int,
                  method: ScanMethod)
                 (implicit ec: ExecutionContext):
    Future[Iterator[SegmentIndex[projection.PK, projection.SK]]] = {
    val indexTable = getOrCreateIndexTable(projection.datasetRef)
    val filterFunc = method match {
      case FilteredPartitionScan(_, filterFunc) => filterFunc
      case FilteredPartitionRangeScan(_, segRange, filterFunc) => filterFunc
      case other: ScanMethod =>  (x: Any) => true
    }
    val futIndexRecords = method match {
      case SinglePartitionScan(partition) =>
        val binPart = projection.partitionType.toBytes(partition.asInstanceOf[projection.PK])
        indexTable.getIndices(binPart, version)

      case SinglePartitionRangeScan(k) =>
        indexTable.getIndices(projection.toBinaryKeyRange(k), version)

      case MultiPartitionScan(partitions) =>
        multiPartScan(projection, partitions, indexTable, version)

      case MultiPartitionRangeScan(keyRanges) =>
        multiPartRangeScan(projection, keyRanges, indexTable, version)

      case FilteredPartitionScan(CassandraTokenRangeSplit(tokens, _), _) =>
        indexTable.scanIndices(version, tokens)

      case FilteredPartitionRangeScan(CassandraTokenRangeSplit(tokens, _), segRange, _) =>
        val binRange = projection.toBinarySegRange(segRange)
        indexTable.scanIndicesRange(version, tokens, binRange.start, binRange.end)

      case other: ScanMethod => ???
    }
    futIndexRecords.map { indexIt =>
      indexIt.sortedGroupBy(index => (index.binPartition, index.segmentId))
             .filter { case ((binPart, _), _) => filterFunc(projection.partitionType.fromBytes(binPart)) }
             .map { case ((binPart, binSeg), records) =>
               val skips = records.map { r => ChunkSetInfo.fromBytes(r.data.array) }.toSeq
               toSegIndex(projection, binPart, binSeg, skips)
             }
    }
  }

  def getOrCreateChunkTable(dataset: DatasetRef): ChunkTable = {
    chunkTableCache.getOrElseUpdate(dataset,
                                    { (dataset: DatasetRef) =>
                                      new ChunkTable(dataset, clusterConnector)(readEc) })
  }

  def getOrCreateIndexTable(dataset: DatasetRef): IndexTable = {
    indexTableCache.getOrElseUpdate(dataset,
                                    { (dataset: DatasetRef) =>
                                      new IndexTable(dataset, clusterConnector)(readEc) })
  }

  def reset(): Unit = {}
}

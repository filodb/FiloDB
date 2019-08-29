package filodb.cassandra.columnstore

import java.net.InetSocketAddress

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Metadata, TokenRange}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.{DefaultFiloSessionProvider, FiloCassandraConnector, FiloSessionProvider, Util}
import filodb.cassandra.Util
import filodb.core._
import filodb.core.store._
import filodb.memory.BinaryRegionLarge

/**
 * Implementation of a column store using Apache Cassandra tables.
 * This class must be thread-safe as it is intended to be used concurrently.
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
 * @param readEc A Scheduler for reads.  This must be separate from writes to prevent deadlocks.
 * @param filoSessionProvider if provided, a session provider provides a session for the configuration
 * @param sched A Scheduler for writes
 */
class CassandraColumnStore(val config: Config, val readEc: Scheduler,
                           val filoSessionProvider: Option[FiloSessionProvider] = None)
                          (implicit val sched: Scheduler)
extends ColumnStore with CassandraChunkSource with StrictLogging {
  import collection.JavaConverters._

  import filodb.core.store._
  import Perftools._

  logger.info(s"Starting CassandraColumnStore with config ${cassandraConfig.withoutPath("password")}")

  private val writeParallelism = cassandraConfig.getInt("write-parallelism")

  val sinkStats = new ChunkSinkStats

  def initialize(dataset: DatasetRef): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    val partIndexTable = getOrCreatePartitionIndexTable(dataset)
    clusterConnector.createKeyspace(chunkTable.keyspace)
    val indexTable = getOrCreateIngestionTimeIndexTable(dataset)
    // Important: make sure nodes are in agreement before any schema changes
    clusterMeta.checkSchemaAgreement()
    for { ctResp    <- chunkTable.initialize()
          ixResp    <- indexTable.initialize()
          pitResp   <- partIndexTable.initialize() } yield pitResp
  }

  def truncate(dataset: DatasetRef): Future[Response] = {
    logger.info(s"Clearing all data for dataset ${dataset}")
    val chunkTable = getOrCreateChunkTable(dataset)
    val indexTable = getOrCreateIngestionTimeIndexTable(dataset)
    val partIndexTable = getOrCreatePartitionIndexTable(dataset)
    clusterMeta.checkSchemaAgreement()
    for { ctResp    <- chunkTable.clearAll()
          ixResp    <- indexTable.clearAll()
          pitResp   <- partIndexTable.clearAll() } yield pitResp
  }

  def dropDataset(dataset: DatasetRef): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    val indexTable = getOrCreateIngestionTimeIndexTable(dataset)
    val partIndexTable = getOrCreatePartitionIndexTable(dataset)
    clusterMeta.checkSchemaAgreement()
    for { ctResp    <- chunkTable.drop() if ctResp == Success
          ixResp    <- indexTable.drop() if ixResp == Success
          pitResp   <- partIndexTable.drop() if pitResp == Success }
    yield {
      chunkTableCache.remove(dataset)
      indexTableCache.remove(dataset)
      partitionIndexTableCache.remove(dataset)
      pitResp
    }
  }

  // Initial implementation: write each ChunkSet as its own transaction.  Will result in lots of writes.
  // Future optimization: group by token range and batch?
  def write(ref: DatasetRef,
            chunksets: Observable[ChunkSet],
            diskTimeToLive: Int = 259200): Future[Response] = {
    chunksets.mapAsync(writeParallelism) { chunkset =>
               val span = Kamon.buildSpan("write-chunkset").start()
               val partBytes = BinaryRegionLarge.asNewByteArray(chunkset.partition)
               val future =
                 for { writeChunksResp   <- writeChunks(ref, partBytes, chunkset, diskTimeToLive)
                       writeIndicesResp  <- writeIndices(ref, partBytes, chunkset, diskTimeToLive)
                                            if writeChunksResp == Success
                 } yield {
                   span.finish()
                   sinkStats.chunksetWrite()
                   writeIndicesResp
                 }
               Task.fromFuture(future)
             }.takeWhile(_ == Success)
             .countL.runAsync
             .map { chunksWritten =>
               if (chunksWritten > 0) Success else NotApplied
             }
  }

  private def writeChunks(ref: DatasetRef,
                          partition: Array[Byte],
                          chunkset: ChunkSet,
                          diskTimeToLive: Int): Future[Response] = {
    asyncSubtrace("write-chunks", "ingestion") {
      val chunkTable = getOrCreateChunkTable(ref)
      chunkTable.writeChunks(partition, chunkset.info, chunkset.chunks, sinkStats, diskTimeToLive)
        .collect {
          case Success => chunkset.invokeFlushListener(); Success
        }
    }
  }

  private def writeIndices(ref: DatasetRef,
                           partition: Array[Byte],
                           chunkset: ChunkSet,
                           diskTimeToLive: Int): Future[Response] = {
    asyncSubtrace("write-index", "ingestion") {
      val indexTable = getOrCreateIngestionTimeIndexTable(ref)
      val info = chunkset.info
      val infos = Seq((info.ingestionTime, info.startTime, ChunkSetInfo.toBytes(info)))
      indexTable.writeIndices(partition, infos, sinkStats, diskTimeToLive)
    }
  }

  def shutdown(): Unit = {
    clusterConnector.shutdown()
  }

  private def clusterMeta: Metadata = clusterConnector.session.getCluster.getMetadata

  /**
   * Splits scans of a dataset across multiple token ranges.
   * @param splitsPerNode  - how much parallelism or ways to divide a token range on each node
   * @return each split will have token_start, token_end, replicas filled in
   */
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = 1): Seq[ScanSplit] = {
    val keyspace = clusterConnector.keySpaceName(dataset)
    require(splitsPerNode >= 1, s"Must specify at least 1 splits_per_node, got $splitsPerNode")

    val tokenRanges = unwrapTokenRanges(clusterMeta.getTokenRanges.asScala.toSeq)
    logger.debug(s"unwrapTokenRanges: ${tokenRanges.toString()}")
    val tokensByReplica = tokenRanges.groupBy { tokenRange =>
      clusterMeta.getReplicas(keyspace, tokenRange)
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
      val replicas = clusterMeta.getReplicas(keyspace, tokenRanges.head).asScala
      CassandraTokenRangeSplit(tokenRanges.map { range => (range.getStart.toString, range.getEnd.toString) },
                               replicas.map(_.getSocketAddress).toSet)
    }
  }

  def unwrapTokenRanges(wrappedRanges : Seq[TokenRange]): Seq[TokenRange] =
    wrappedRanges.flatMap(_.unwrap().asScala.toSeq)

  def getPartKeyTimeBucket(ref: DatasetRef, shardNum: Int, timeBucket: Int): Observable[PartKeyTimeBucketSegment] = {
    getOrCreatePartitionIndexTable(ref).getPartKeySegments(shardNum, timeBucket)
  }

  def writePartKeyTimeBucket(ref: DatasetRef,
                             shardNum: Int,
                             timeBucket: Int,
                             partitionIndex: Seq[Array[Byte]],
                             diskTimeToLive: Int): Future[Response] = {

    val table = getOrCreatePartitionIndexTable(ref)
    val write = table.writePartKeySegments(shardNum, timeBucket, partitionIndex.map(Util.toBuffer(_)), diskTimeToLive)
    write.map { response =>
      sinkStats.indexTimeBucketWritten(partitionIndex.map(_.length).sum)
      response
    }
  }
}

case class CassandraTokenRangeSplit(tokens: Seq[(String, String)],
                                    replicas: Set[InetSocketAddress]) extends ScanSplit {
  // NOTE: You need both the host string and the IP address for Spark's locality to work
  def hostnames: Set[String] = replicas.flatMap(r => Set(r.getHostString, r.getAddress.getHostAddress))
}

trait CassandraChunkSource extends RawChunkSource with StrictLogging {
  import Iterators._

  def config: Config
  def filoSessionProvider: Option[FiloSessionProvider]
  def readEc: Scheduler

  implicit val readSched = readEc

  val stats = new ChunkSourceStats

  val cassandraConfig = config.getConfig("cassandra")
  val ingestionConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("ingestion-consistency-level"))
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = concurrentCache[DatasetRef, TimeSeriesChunksTable](tableCacheSize)
  val indexTableCache = concurrentCache[DatasetRef, IngestionTimeIndexTable](tableCacheSize)
  val partitionIndexTableCache = concurrentCache[DatasetRef, PartitionIndexTable](tableCacheSize)

  protected val clusterConnector = new FiloCassandraConnector {
    def config: Config = cassandraConfig
    def ec: ExecutionContext = readEc
    val sessionProvider = filoSessionProvider.getOrElse(new DefaultFiloSessionProvider(cassandraConfig))
  }

  val partParallelism = 4

  def readRawPartitions(ref: DatasetRef,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = {
    val indexTable = getOrCreateIngestionTimeIndexTable(ref)
    logger.debug(s"Scanning partitions for $ref with method $partMethod...")
    val (filters, infoRecords) = partMethod match {
      case SinglePartitionScan(partition, _) =>
        (Nil, indexTable.getInfos(partition))

      case MultiPartitionScan(partitions, _) =>
        (Nil, indexTable.getMultiInfos(partitions))

      case FilteredPartitionScan(CassandraTokenRangeSplit(tokens, _), filters) =>
        (filters, indexTable.scanInfos(tokens))

      case other: PartitionScanMethod =>  ???
    }
    // For now, partition filter func is disabled.  In the near future, this logic to scan through partitions
    // and filter won't be necessary as we will have Lucene indexes to pull up specific partitions.
    // val filterFunc = KeyFilter.makePartitionFilterFunc(dataset, filters)

    partMethod match {
      // Compute minimum start time from the infos, then do efficient range query MULTIGET
      case MultiPartitionScan(partitions, _) =>
        val chunkTable = getOrCreateChunkTable(ref)
        Observable.fromTask(startTimeFromInfos(infoRecords, chunkMethod.startTime, chunkMethod.endTime))
          .flatMap { case startTime =>
            chunkTable.readRawPartitionRange(partitions, startTime, chunkMethod.endTime)
          }
      case other: PartitionScanMethod =>
        // NOTE: we use hashCode as an approx means to identify ByteBuffers/partition keys which are identical
        infoRecords.sortedGroupBy(_.binPartition.hashCode)
                   .mapAsync(partParallelism) { case (_, binIndices) =>
                     val infoBytes = binIndices.map(_.data.array)
                     assembleRawPartData(ref, binIndices.head.binPartition.array, infoBytes, chunkMethod)
                   }
    }
  }

  // Returns the minimum start time from all the ChunkInfos that are within [startTime, endTime) query range
  // Needed so we can do range queries efficiently by chunkID
  private def startTimeFromInfos(infoRecords: Observable[InfoRecord],
                                 startTime: Long,
                                 endTime: Long): Task[Long] = {
    infoRecords.foldLeftL(Long.MaxValue) { case (curStart, InfoRecord(_, data)) =>
      val infoStartTime = ChunkSetInfo.getStartTime(data.array)
      val infoEndTime = ChunkSetInfo.getEndTime(data.array)
      if (infoStartTime <= endTime && infoEndTime >= startTime) {
        Math.min(curStart, infoStartTime)
      } else {
        curStart
      }
    }
  }

  def getOrCreateChunkTable(dataset: DatasetRef): TimeSeriesChunksTable = {
    chunkTableCache.getOrElseUpdate(dataset, { (dataset: DatasetRef) =>
      new TimeSeriesChunksTable(dataset, clusterConnector, ingestionConsistencyLevel)(readEc) })
  }

  def getOrCreateIngestionTimeIndexTable(dataset: DatasetRef): IngestionTimeIndexTable = {
    indexTableCache.getOrElseUpdate(dataset,
                                    { (dataset: DatasetRef) =>
                                      new IngestionTimeIndexTable(dataset, clusterConnector)(readEc) })
  }

  def getOrCreatePartitionIndexTable(dataset: DatasetRef): PartitionIndexTable = {
    partitionIndexTableCache.getOrElseUpdate(dataset, { dataset: DatasetRef =>
      new PartitionIndexTable(dataset, clusterConnector, ingestionConsistencyLevel)(readEc)
    })
  }

  def reset(): Unit = {}

  private def assembleRawPartData(ref: DatasetRef,
                                  partKeyBytes: Array[Byte],
                                  chunkInfos: Seq[Array[Byte]],
                                  chunkMethod: ChunkScanMethod): Task[RawPartData] = {
    val chunkTable = getOrCreateChunkTable(ref)

    val filteredInfos = chunkInfos.filter { infoBytes =>
      ChunkSetInfo.getStartTime(infoBytes) <= chunkMethod.endTime &&
      ChunkSetInfo.getEndTime(infoBytes) >= chunkMethod.startTime
    }

    chunkTable.readRawPartitionData(partKeyBytes, filteredInfos)
  }
}

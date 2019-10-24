package filodb.cassandra.columnstore

import java.net.InetSocketAddress

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.datastax.driver.core.{ConsistencyLevel, Metadata, TokenRange}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.{DefaultFiloSessionProvider, FiloCassandraConnector, FiloSessionProvider, Util}
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
                       if writeChunksResp == Success
                       writeIndicesResp  <- writeIndices(ref, partBytes, chunkset, diskTimeToLive)
                       if writeIndicesResp == Success
                 } yield {
                   span.finish()
                   sinkStats.chunksetWrite()
                   writeIndicesResp
                 }
               Task.fromFuture(future)
             }
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

  /**
    * Reads chunks by querying partitions by ingestion time range and subsequently filtering by user time range.
    * ** User/Ingestion End times are exclusive **
    */
  def getChunksByIngestionTimeRange(datasetRef: DatasetRef,
                                    splits: Iterator[ScanSplit],
                                    ingestionTimeStart: Long,
                                    ingestionTimeEnd: Long,
                                    userTimeStart: Long,
                                    userTimeEnd: Long,
                                    batchSize: Int,
                                    batchTime: FiniteDuration): Observable[Seq[RawPartData]] = {
    val partKeys = Observable.fromIterator(splits).flatMap {
      case split: CassandraTokenRangeSplit =>
        val indexTable = getOrCreateIngestionTimeIndexTable(datasetRef)
        logger.debug(s"Querying cassandra for partKeys for split=$split ingestionTimeStart=$ingestionTimeStart " +
          s"ingestionTimeEnd=$ingestionTimeEnd")
        indexTable.scanPartKeysByIngestionTime(split.tokens, ingestionTimeStart, ingestionTimeEnd)
      case split => throw new UnsupportedOperationException(s"Unknown split type $split seen")
    }

    import filodb.core.Iterators._

    val chunksTable = getOrCreateChunkTable(datasetRef)
    partKeys.bufferTimedAndCounted(batchTime, batchSize).map { parts =>
      logger.debug(s"Querying cassandra for chunks from ${parts.size} partitions userTimeStart=$userTimeStart " +
        s"userTimeEnd=$userTimeEnd")
      // TODO evaluate if we can increase parallelism here. This needs to be tuneable
      // based on how much faster downsampling should run, and how much additional read load cassandra can take.
      chunksTable.readRawPartitionRangeBB(parts, userTimeStart, userTimeEnd).toIterator().toSeq
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
                        maxChunkTime: Long,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = {
    val partitions = partMethod match {
      case MultiPartitionScan(p, _) => p
      case SinglePartitionScan(p, _) => Seq(p)
      case p => throw new UnsupportedOperationException(s"PartitionScan $p to be implemented later")
    }
    val chunkTable = getOrCreateChunkTable(ref)
    val startTime = if (chunkMethod == AllChunkScan) Long.MinValue else chunkMethod.startTime - maxChunkTime
    chunkTable.readRawPartitionRange(partitions, startTime, chunkMethod.endTime)
  }

  def getOrCreateChunkTable(dataset: DatasetRef): TimeSeriesChunksTable = {
    chunkTableCache.getOrElseUpdate(dataset, { dataset: DatasetRef =>
      new TimeSeriesChunksTable(dataset, clusterConnector, ingestionConsistencyLevel)(readEc) })
  }

  def getOrCreateIngestionTimeIndexTable(dataset: DatasetRef): IngestionTimeIndexTable = {
    indexTableCache.getOrElseUpdate(dataset,
                                    { dataset: DatasetRef =>
                                      new IngestionTimeIndexTable(dataset, clusterConnector)(readEc) })
  }

  def getOrCreatePartitionIndexTable(dataset: DatasetRef): PartitionIndexTable = {
    partitionIndexTableCache.getOrElseUpdate(dataset, { dataset: DatasetRef =>
      new PartitionIndexTable(dataset, clusterConnector, ingestionConsistencyLevel)(readEc)
    })
  }

  def reset(): Unit = {}

}

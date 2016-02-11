package filodb.cassandra.columnstore

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.store.{CachedMergingColumnStore, ColumnStoreScanner}
import filodb.core.metadata.{Column, Projection}

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
 *     keyspace = "my_cass_keyspace"
 *   }
 *   columnstore {
 *     tablecache-size = 50    # Number of cache entries for C* for ChunkTable etc.
 *     segment-cache-size = 1000    # Number of segments to cache
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
  logger.info(s"Starting CassandraColumnStore with config $cassandraConfig")

  val segmentCache = LruCache[Segment](segmentCacheSize)

  val mergingStrategy = new AppendingChunkMergingStrategy(this)

  /**
   * Initializes the column store for a given dataset projection.  Must be called once before appending
   * segments to that projection.
   */
  def initializeProjection(projection: Projection): Future[Response] =
    for { (chunkTable, rowMapTable) <- getSegmentTables(projection.dataset)
          ctResp                    <- chunkTable.initialize()
          rmtResp                   <- rowMapTable.initialize() } yield { rmtResp }

  /**
   * Clears all data from the column store for that given projection.
   */
  def clearProjectionDataInner(projection: Projection): Future[Response] =
    for { (chunkTable, rowMapTable) <- getSegmentTables(projection.dataset)
          ctResp                    <- chunkTable.clearAll()
          rmtResp                   <- rowMapTable.clearAll() } yield { rmtResp }

  /**
   * Implementations of low-level storage primitives
   */
  def writeChunks(dataset: TableName,
                  partition: BinaryPartition,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          resp <- chunkTable.writeChunks(partition, version, segmentId, chunks) }
    yield { resp }
  }

  def writeChunkRowMap(dataset: TableName,
                       partition: BinaryPartition,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response] = {
    val (chunkIds, rowNums) = chunkRowMap.serialize()
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          resp <- rowMapTable.writeChunkMap(partition, version, segmentId,
                                            chunkIds, rowNums, chunkRowMap.nextChunkId) }
    yield { resp }
  }

  def shutdown(): Unit = {
    clusterConnector.shutdown()
  }

  /**
   * Splits scans of a dataset across multiple token ranges.
   * params:
   *   splits_per_node - how much parallelism or ways to divide a token range on each node
   *
   * @return each split will have token_start, token_end, replicas filled in
   */
  def getScanSplits(dataset: TableName,
                    params: Map[String, String] = Map.empty): Seq[Map[String, String]] = {
    val metadata = clusterConnector.session.getCluster.getMetadata
    val splitsPerNode = params.getOrElse("splits_per_node", "1").toInt
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
      Map("token_start" -> tokenRange.getStart.toString,
          "token_end"   -> tokenRange.getEnd.toString,
          "replicas"    -> replicas.map(_.toString).mkString(","))
    }
  }
}

trait CassandraColumnStoreScanner extends ColumnStoreScanner with StrictLogging {
  import filodb.core.store._
  import Types._
  import collection.JavaConverters._

  def config: Config

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = LruCache[ChunkTable](tableCacheSize)
  val rowMapTableCache = LruCache[ChunkRowMapTable](tableCacheSize)

  protected val clusterConnector = new FiloCassandraConnector {
    def config: Config = cassandraConfig
  }

  def readChunks(dataset: TableName,
                 columns: Set[ColumnId],
                 keyRange: BinaryKeyRange,
                 version: Int)(implicit ec: ExecutionContext): Future[Seq[ChunkedData]] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          data <- Future.sequence(columns.toSeq.map(
                    chunkTable.readChunks(keyRange.partition, version, _,
                                          keyRange.start, keyRange.end,
                                          keyRange.endExclusive))) }
    yield { data }
  }

  def readChunkRowMaps(dataset: TableName,
                       keyRange: BinaryKeyRange,
                       version: Int)
                      (implicit ec: ExecutionContext): Future[Iterator[ChunkMapInfo]] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          cassRowMaps <- rowMapTable.getChunkMaps(keyRange, version) }
    yield {
      cassRowMaps.toIterator.map {
        case ChunkRowMapRecord(segmentId, chunkIds, rowNums, nextChunkId) =>
          val rowMap = new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId)
          (keyRange.partition, segmentId, rowMap)
      }
    }
  }

  /**
   * required params:
   *   token_start - string representation of start of token range
   *   token_end   - string representation of end of token range
   */
  def scanChunkRowMaps(dataset: TableName,
                       version: Int,
                       params: Map[String, String])
                      (implicit ec: ExecutionContext): Future[Iterator[ChunkMapInfo]] = {
    val tokenStart = params("token_start")
    val tokenEnd   = params("token_end")
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          rowMaps <- rowMapTable.scanChunkMaps(version, tokenStart, tokenEnd) }
    yield {
      rowMaps.map { case (part, ChunkRowMapRecord(segmentId, chunkIds, rowNums, nextChunkId)) =>
        (part, segmentId, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId))
      }
    }
  }

  // Retrieve handles to the tables for a particular dataset from the cache, creating the instances
  // if necessary
  def getSegmentTables(dataset: TableName)
                      (implicit ec: ExecutionContext): Future[(ChunkTable, ChunkRowMapTable)] = {
    val chunkTableFuture = chunkTableCache(dataset) {
      logger.debug(s"Creating a new ChunkTable for dataset $dataset")
      new ChunkTable(dataset, clusterConnector)
    }
    val chunkRowMapTableFuture = rowMapTableCache(dataset) {
      logger.debug(s"Creating a new ChunkRowMapTable for dataset $dataset")
      new ChunkRowMapTable(dataset, clusterConnector)
    }
    for { chunkTable <- chunkTableFuture
          rowMapTable <- chunkRowMapTableFuture }
    yield { (chunkTable, rowMapTable) }
  }
}

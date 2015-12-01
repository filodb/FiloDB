package filodb.cassandra.columnstore

import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import spray.caching._

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.columnstore.CachedMergingColumnStore
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
 * @param ec An ExecutionContext for futures.  See this for a way to do backpressure with futures:
 *        http://quantifind.com/blog/2015/06/throttling-instantiations-of-scala-futures-1/
 */
class CassandraColumnStore(config: Config)
                          (implicit val ec: ExecutionContext)
extends CachedMergingColumnStore with StrictLogging {
  import filodb.core.columnstore._
  import Types._
  import collection.JavaConverters._

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")
  val segmentCacheSize = config.getInt("columnstore.segment-cache-size")

  val chunkTableCache = LruCache[ChunkTable](tableCacheSize)
  val rowMapTableCache = LruCache[ChunkRowMapTable](tableCacheSize)
  val segmentCache = LruCache[Segment[_]](segmentCacheSize)

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
                  partition: PartitionKey,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          resp <- chunkTable.writeChunks(partition, version, segmentId, chunks) }
    yield { resp }
  }

  def writeChunkRowMap(dataset: TableName,
                       partition: PartitionKey,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response] = {
    val (chunkIds, rowNums) = chunkRowMap.serialize()
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          resp <- rowMapTable.writeChunkMap(partition, version, segmentId,
                                            chunkIds, rowNums, chunkRowMap.nextChunkId) }
    yield { resp }
  }

  def readChunks[K](columns: Set[ColumnId],
                    keyRange: KeyRange[K],
                    version: Int): Future[Seq[ChunkedData]] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(keyRange.dataset)
          data <- Future.sequence(columns.toSeq.map(
                    chunkTable.readChunks(keyRange.partition, version, _,
                                          keyRange.binaryStart, keyRange.binaryEnd,
                                          keyRange.endExclusive))) }
    yield { data }
  }

  def readChunkRowMaps[K](keyRange: KeyRange[K],
                          version: Int): Future[Seq[(SegmentId, BinaryChunkRowMap)]] = {
    for { (chunkTable, rowMapTable) <- getSegmentTables(keyRange.dataset)
          cassRowMaps <- rowMapTable.getChunkMaps(keyRange.partition, version,
                                                  keyRange.binaryStart, keyRange.binaryEnd) }
    yield {
      cassRowMaps.map {
        case ChunkRowMapRecord(segmentId, chunkIds, rowNums, nextChunkId) =>
          val rowMap = new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId)
          (segmentId, rowMap)
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
                       partitionFilter: (PartitionKey => Boolean),
                       params: Map[String, String]): Future[Iterator[ChunkMapInfo]] = {
    val tokenStart = params("token_start")
    val tokenEnd   = params("token_end")
    for { (chunkTable, rowMapTable) <- getSegmentTables(dataset)
          rowMaps <- rowMapTable.scanChunkMaps(version, tokenStart, tokenEnd) }
    yield {
      rowMaps.filter { case (part, _) => partitionFilter(part) }
             .map { case (part, ChunkRowMapRecord(segmentId, chunkIds, rowNums, nextChunkId)) =>
        (part, segmentId, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId))
      }
    }
  }

  private val clusterConnector = new FiloCassandraConnector {
    def config: Config = cassandraConfig
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
        tokensByReplica(key).reduceLeft(_.mergeWith(_)).splitEvenly(splitsPerNode).asScala
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

  // Retrieve handles to the tables for a particular dataset from the cache, creating the instances
  // if necessary
  def getSegmentTables(dataset: TableName): Future[(ChunkTable, ChunkRowMapTable)] = {
    val chunkTableFuture = chunkTableCache(dataset) {
      logger.debug(s"Creating a new ChunkTable for dataset $dataset with config $cassandraConfig")
      new ChunkTable(dataset, cassandraConfig)
    }
    val chunkRowMapTableFuture = rowMapTableCache(dataset) {
      logger.debug(s"Creating a new ChunkRowMapTable for dataset $dataset with config $cassandraConfig")
      new ChunkRowMapTable(dataset, cassandraConfig)
    }
    for { chunkTable <- chunkTableFuture
          rowMapTable <- chunkRowMapTableFuture }
    yield { (chunkTable, rowMapTable) }
  }
}

package filodb.cassandra.columnstore

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Metadata, TokenRange}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong

import filodb.cassandra.{DefaultFiloSessionProvider, FiloCassandraConnector, FiloSessionProvider}
import filodb.cassandra.Util
import filodb.core._
import filodb.core.metadata.Dataset
import filodb.core.query._
import filodb.core.store._
import filodb.memory.format.FiloVector

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
  private val partitionListNumStripesPerShard = cassandraConfig.getInt("partition-list-num-stripes-per-shard")

  val sinkStats = new ChunkSinkStats

  def initialize(dataset: DatasetRef): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    clusterConnector.createKeyspace(chunkTable.keyspace)
    val indexTable = getOrCreateIndexTable(dataset)
    // Important: make sure nodes are in agreement before any schema changes
    clusterMeta.checkSchemaAgreement()
    for { ctResp    <- chunkTable.initialize()
          rmtResp   <- indexTable.initialize() } yield rmtResp
  }

  def truncate(dataset: DatasetRef): Future[Response] = {
    logger.info(s"Clearing all data for dataset ${dataset}")
    val chunkTable = getOrCreateChunkTable(dataset)
    val indexTable = getOrCreateIndexTable(dataset)
    clusterMeta.checkSchemaAgreement()
    for { ctResp    <- chunkTable.clearAll()
          rmtResp   <- indexTable.clearAll() } yield rmtResp
  }

  def dropDataset(dataset: DatasetRef): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    val indexTable = getOrCreateIndexTable(dataset)
    clusterMeta.checkSchemaAgreement()
    for { ctResp    <- chunkTable.drop() if ctResp == Success
          rmtResp   <- indexTable.drop() if rmtResp == Success }
    yield {
      chunkTableCache.remove(dataset)
      indexTableCache.remove(dataset)
      rmtResp
    }
  }

  // Initial implementation: write each ChunkSet as its own transaction.  Will result in lots of writes.
  // Future optimization: group by token range and batch?
  def write(dataset: Dataset,
            chunksets: Observable[ChunkSet],
            diskTimeToLive: Int = 259200): Future[Response] = {
    chunksets.mapAsync(writeParallelism) { chunkset =>
               val span = Kamon.buildSpan("write-chunkset").start()
               val partBytes = dataset.partKeySchema.asByteArray(chunkset.partition)
               val future =
                 for { writeChunksResp  <- writeChunks(dataset.ref, partBytes, chunkset, diskTimeToLive)
                       writeIndexResp   <- writeIndices(dataset, partBytes, chunkset, diskTimeToLive)
                                           if writeChunksResp == Success
                 } yield {
                   span.finish()
                   sinkStats.chunksetWrite()
                   writeIndexResp
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

  private def writeIndices(dataset: Dataset,
                           partition: Array[Byte],
                           chunkset: ChunkSet,
                           diskTimeToLive: Int): Future[Response] = {
    asyncSubtrace("write-index", "ingestion") {
      val indexTable = getOrCreateIndexTable(dataset.ref)
      val indices = Seq((chunkset.info.id, ChunkSetInfo.toBytes(chunkset.info)))
      indexTable.writeIndices(partition, indices, sinkStats, diskTimeToLive)
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

  def getPartitionIndex(dataset: Dataset, shardNum: Int, timeBucket: Int): Observable[PartitionIndexRecord] = {
    getOrCreatePartitionIndexTable(dataset.ref).getPartitions(shardNum, timeBucket)
  }

  def writePartitionIndex(dataset: Dataset,
                          shardNum: Int,
                          timeBucket: Int,
                    partitionIndex: Seq[Array[Byte]],
                          diskTimeToLive: Int): Future[Response] = {

    val table = getOrCreatePartitionIndexTable(dataset.ref)
    val writes = partitionIndex.zipWithIndex.map { case (byteArray, segmentId) =>
      table.writePartitions(shardNum, timeBucket, segmentId, Util.toBuffer(byteArray), diskTimeToLive)
    }

    Future.sequence(writes).map { responses =>
      responses.find(_ != Success).getOrElse(Success)
    }
  }

}

case class CassandraTokenRangeSplit(tokens: Seq[(String, String)],
                                    replicas: Set[InetSocketAddress]) extends ScanSplit {
  // NOTE: You need both the host string and the IP address for Spark's locality to work
  def hostnames: Set[String] = replicas.flatMap(r => Set(r.getHostString, r.getAddress.getHostAddress))
}

trait CassandraChunkSource extends ChunkSource with StrictLogging {
  import Iterators._

  def config: Config
  def filoSessionProvider: Option[FiloSessionProvider]
  def readEc: Scheduler

  val stats = new ChunkSourceStats

  val cassandraConfig = config.getConfig("cassandra")
  val ingestionConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("ingestion-consistency-level"))
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = concurrentCache[DatasetRef, ChunkTable](tableCacheSize)
  val indexTableCache = concurrentCache[DatasetRef, IndexTable](tableCacheSize)
  val partitionIndexTableCache = concurrentCache[DatasetRef, PartitionIndexTable](tableCacheSize)

  protected val clusterConnector = new FiloCassandraConnector {
    def config: Config = cassandraConfig
    def ec: ExecutionContext = readEc
    val sessionProvider = filoSessionProvider.getOrElse(new DefaultFiloSessionProvider(cassandraConfig))
  }

  // Produce an empty stream of chunks so that results can still be returned correctly
  def emptyChunkStream(infosSkips: ChunkSetInfo.ChunkInfosAndSkips, colNo: Int):
    Observable[SingleChunkInfo] =
    Observable.fromIterator(infosSkips.toIterator.map { case (info, skips) =>
      // scalastyle:off
      SingleChunkInfo(info.id, colNo, null.asInstanceOf[ByteBuffer])
      // scalastyle:on
    })

  def multiPartScan(dataset: Dataset,
                    partitions: Seq[Array[Byte]],
                    indexTable: IndexTable): Observable[IndexRecord] = {
    // Get each partition index observable concurrently.  As observables they are lazy
    val its = partitions.map { partition =>
      indexTable.getIndices(partition)
    }
    Observable.concat(its: _*)
  }

  def scanPartitions(dataset: Dataset,
                     partMethod: PartitionScanMethod): Observable[FiloPartition] = {
    val indexTable = getOrCreateIndexTable(dataset.ref)
    logger.debug(s"Scanning partitions for ${dataset.ref} with method $partMethod...")
    val (filters, indexRecords) = partMethod match {
      case SinglePartitionScan(partition, _) =>
        (Nil, indexTable.getIndices(partition))

      case MultiPartitionScan(partitions, _) =>
        (Nil, multiPartScan(dataset, partitions, indexTable))

      case FilteredPartitionScan(CassandraTokenRangeSplit(tokens, _), filters, scanMethod) =>
        // Ignore scanMethod we always serve for AllChunkScan since
        // we don't have a searchable row-key index in cassandra.
        // Typically, on-demand-paging will not issue FilteredPartitionScan
        (filters, indexTable.scanIndices(tokens))

      case other: PartitionScanMethod =>  ???
    }
    // For now, partition filter func is disabled.  In the near future, this logic to scan through partitions
    // and filter won't be necessary as we will have Lucene indexes to pull up specific partitions.
    // val filterFunc = KeyFilter.makePartitionFilterFunc(dataset, filters)

    // NOTE: we use hashCode as an approx means to identify ByteBuffers/partition keys which are identical
    indexRecords.sortedGroupBy(_.binPartition.hashCode)
                .collect { case (_, binIndices) =>
                  val (base, offset, _) = binIndices.head.partBaseOffset
                  val newIndex = new ChunkIDPartitionChunkIndex(base, offset, dataset)
                  binIndices.foreach { binIndex =>
                    val info = ChunkSetInfo.fromBytes(binIndex.data.array)
                    newIndex.add(info, Nil)
                  }
                  new CassandraPartition(newIndex, dataset, this)
                }
  }

  def getOrCreateChunkTable(dataset: DatasetRef): ChunkTable = {
    chunkTableCache.getOrElseUpdate(dataset,
                                    { (dataset: DatasetRef) =>
                                      new ChunkTable(dataset, clusterConnector, ingestionConsistencyLevel)(readEc) })
  }

  def getOrCreateIndexTable(dataset: DatasetRef): IndexTable = {
    indexTableCache.getOrElseUpdate(dataset,
                                    { (dataset: DatasetRef) =>
                                      new IndexTable(dataset, clusterConnector)(readEc) })
  }

  def getOrCreatePartitionIndexTable(dataset: DatasetRef): PartitionIndexTable = {
    partitionIndexTableCache.getOrElseUpdate(dataset, { dataset: DatasetRef =>
      new PartitionIndexTable(dataset, clusterConnector, ingestionConsistencyLevel)(readEc)
    })
  }

  def reset(): Unit = {}

}

/**
 * Represents one partition in the Cassandra ChunkSource.  Reads chunks lazily when streamReaders is called.
 * The index state can be cached for lower read latency.
 */
class CassandraPartition(index: ChunkIDPartitionChunkIndex,
                         val dataset: Dataset,
                         scanner: CassandraChunkSource) extends FiloPartition with StrictLogging {
  import Iterators._

  val partKeyBase = index.partKeyBase.asInstanceOf[Array[Byte]]
  val partKeyOffset = index.partKeyOffset

  def numChunks: Int = index.numChunks

  def appendingChunkLen: Int = index.latestN(1).toSeq.headOption.map(_._1.numRows).getOrElse(0)

  // For now just report a dummy shard.  In the future figure this out.
  val shard = 0

  private val chunkTable = scanner.getOrCreateChunkTable(dataset.ref)
  private val readers = new NonBlockingHashMapLong[ChunkSetReader](32, false)

  private def mergeChunks(chunkStreams: Seq[Observable[SingleChunkInfo]],
                          columnIds: Array[Int]): Observable[ChunkSetReader] = {
    Observable.merge(chunkStreams: _*)
              .map { case SingleChunkInfo(id, pos, buf) =>
                readers.get(id) match {
                  // scalastyle:off
                  case null =>
                  // scalastyle:on
                    scanner.stats.incrChunkWithNoInfo()
                    None
                  //scalastyle:off
                  case reader: MutableChunkSetReader if buf != null =>
                  //scalastyle:on
                    reader.addChunk(pos, dataset.vectorMakers(columnIds(pos))(buf, reader.length))
                    if (reader.isFull) {
                      readers.remove(id)
                      scanner.stats.incrReadChunksets()
                      Some(reader)
                    } else { None }
                  case reader: MutableChunkSetReader =>
                    logger.debug(s"Skipping chunk $id due to empty buffer / no data from Cassandra")
                    None
                }
              }.collect { case Some(reader) => reader }
  }

  // Asynchronously reads different columns from Cassandra, merging the reads into ChunkSetReaders
  override def streamReaders(method: ChunkScanMethod, columnIds: Array[Int]): Observable[ChunkSetReader] = {
    // parse ChunkScanMethod into infosSkips....
    val (rangeQuery, infosSkips) = method match {
      case AllChunkScan             => (true, index.allChunks.toSeq)
      case RowKeyChunkScan(k1, k2)  => (false, index.rowKeyRange(k1.binRec, k2.binRec).toSeq)
      case LastSampleChunkScan      => (false, index.latestN(1).toSeq)
    }
    logger.debug(s"Reading chunks from columns ${columnIds.toList}, $stringPartition, method $method")

    // from infoSkips, create the MutableChunkSetReader's
    infosSkips.foreach { case (info, skips) =>
      readers.putIfAbsent(info.id, new MutableChunkSetReader(info, skips, columnIds.size))
    }

    // Read chunks in, populate MutableChunkSetReader's, and emit readers when they are full
    val ids = infosSkips.map(_._1.id).toBuffer
    val chunkStreams = (0 until columnIds.size).map { pos =>
      if (Dataset.isPartitionID(columnIds(pos))) {
        val constVect = constPartitionVector(columnIds(pos))
        // directly populate the ChunkReaders with constant vectors -- no I/O!!
        ids.foreach { id => readers.get(id).asInstanceOf[MutableChunkSetReader].addChunk(pos, constVect) }
        Observable.empty[SingleChunkInfo]
      } else {
        chunkTable.readChunks(partKeyBytes, columnIds(pos), pos, ids, false)
                  .switchIfEmpty(scanner.emptyChunkStream(infosSkips, pos))
      }
    }
    mergeChunks(chunkStreams, columnIds)
  }

  def readers(method: ChunkScanMethod, columnIds: Array[Int]): Iterator[ChunkSetReader] =
    streamReaders(method, columnIds).toIterator()

  def lastVectors: Array[FiloVector[_]] = ???
}

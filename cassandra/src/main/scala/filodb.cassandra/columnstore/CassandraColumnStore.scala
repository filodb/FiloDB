package filodb.cassandra.columnstore

import com.datastax.driver.core.TokenRange
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import kamon.trace.{TraceContext, Tracer}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable
import org.jctools.maps.NonBlockingHashMapLong
import org.velvia.filo.FiloVector
import scala.concurrent.{ExecutionContext, Future}

import filodb.cassandra.{DefaultFiloSessionProvider, FiloCassandraConnector, FiloSessionProvider}
import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
import filodb.core.store._
import filodb.core.metadata.{Column, Projection, RichProjection}
import filodb.core.query._

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
  import filodb.core.store._
  import Types._
  import collection.JavaConverters._
  import Perftools._

  logger.info(s"Starting CassandraColumnStore with config ${cassandraConfig.withoutPath("password")}")

  private val writeParallelism = cassandraConfig.getInt("write-parallelism")

  val sinkStats = new ChunkSinkStats

  /**
   * Initializes the column store for a given dataset projection.  Must be called once before appending
 * segments to that projection.
   */
  def initializeProjection(projection: Projection): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(projection.dataset)
    clusterConnector.createKeyspace(chunkTable.keyspace)
    val indexTable = getOrCreateIndexTable(projection.dataset)
    for { ctResp    <- chunkTable.initialize()
          rmtResp   <- indexTable.initialize() } yield rmtResp
  }

  /**
   * Clears all data from the column store for that given projection.
   */
  def clearProjectionData(projection: Projection): Future[Response] = {
    logger.info(s"Clearing all columnar projection data for dataset ${projection.dataset}")
    val chunkTable = getOrCreateChunkTable(projection.dataset)
    val indexTable = getOrCreateIndexTable(projection.dataset)
    for { ctResp    <- chunkTable.clearAll()
          rmtResp   <- indexTable.clearAll() } yield rmtResp
  }

  def dropDataset(dataset: DatasetRef): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    val indexTable = getOrCreateIndexTable(dataset)
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
  def write(projection: RichProjection,
            version: Int,
            chunksets: Observable[ChunkSet]): Future[Response] = {
    chunksets.mapAsync(writeParallelism) { chunkset =>
               Tracer.withNewContext("write-chunkset") {
                 val ctx = Tracer.currentContext
                 val future =
                   for { writeChunksResp  <- writeChunks(projection.datasetRef, version, chunkset, ctx)
                         writeIndexResp   <- writeIndices(projection, version, chunkset, ctx)
                                             if writeChunksResp == Success
                   } yield {
                     ctx.finish()
                     sinkStats.chunksetWrite()
                     writeIndexResp
                   }
                 Task.fromFuture(future)
               }
             }.takeWhile(_ == Success)
             .countL.runAsync
             .map { chunksWritten =>
               if (chunksWritten > 0) Success else NotApplied
             }
  }

  private def writeChunks(dataset: DatasetRef,
                          version: Int,
                          chunkset: ChunkSet,
                          ctx: TraceContext): Future[Response] = {
    asyncSubtrace("write-chunks", "ingestion", Some(ctx)) {
      val chunkTable = getOrCreateChunkTable(dataset)
      chunkTable.writeChunks(chunkset.partition, version, chunkset.info.id, chunkset.chunks, sinkStats)
    }
  }

  private def writeIndices(projection: RichProjection,
                           version: Int,
                           chunkset: ChunkSet,
                           ctx: TraceContext): Future[Response] = {
    asyncSubtrace("write-index", "ingestion", Some(ctx)) {
      val indexTable = getOrCreateIndexTable(projection.datasetRef)
      val indices = Seq((chunkset.info.id, ChunkSetInfo.toBytes(projection, chunkset.info, chunkset.skips)))
      indexTable.writeIndices(chunkset.partition, version, indices, sinkStats)
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

trait CassandraChunkSource extends ChunkSource with StrictLogging {
  import ChunkSetReader._
  import Types._
  import collection.JavaConverters._
  import Iterators._

  def config: Config
  def filoSessionProvider: Option[FiloSessionProvider]
  def readEc: Scheduler

  val stats = new ChunkSourceStats

  val cassandraConfig = config.getConfig("cassandra")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = concurrentCache[DatasetRef, ChunkTable](tableCacheSize)
  val indexTableCache = concurrentCache[DatasetRef, IndexTable](tableCacheSize)

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

  def multiPartScan(projection: RichProjection,
                    partitions: Seq[Types.PartitionKey],
                    indexTable: IndexTable,
                    version: Int): Observable[IndexRecord] = {
    // Get each partition index observable concurrently.  As observables they are lazy
    val its = partitions.map { partition =>
      indexTable.getIndices(partition, version)
    }
    Observable.concat(its: _*)
  }

  def scanPartitions(projection: RichProjection,
                     version: Int,
                     partMethod: PartitionScanMethod,
                     colToMaker: ColumnToMaker = defaultColumnToMaker): Observable[FiloPartition] = {
    val indexTable = getOrCreateIndexTable(projection.datasetRef)
    logger.debug(s"Scanning partitions for ${projection.datasetRef} with method $partMethod...")
    val (filters, indexRecords) = partMethod match {
      case SinglePartitionScan(partition, _) =>
        (Nil, indexTable.getIndices(partition, version))

      case MultiPartitionScan(partitions, _) =>
        (Nil, multiPartScan(projection, partitions, indexTable, version))

      case FilteredPartitionScan(CassandraTokenRangeSplit(tokens, _), filters) =>
        (filters, indexTable.scanIndices(version, tokens))

      case other: PartitionScanMethod =>  ???
    }
    val filterFunc = KeyFilter.makePartitionFilterFunc(projection, filters)
    indexRecords.sortedGroupBy(_.partition(projection))
                .collect { case (binPart, binIndices) if filterFunc(binPart) =>
                  val newIndex = new ChunkIDPartitionChunkIndex(binPart, projection)
                  binIndices.foreach { binIndex =>
                    val (info, skips) = ChunkSetInfo.fromBytes(projection, binIndex.data.array)
                    newIndex.add(info, skips)
                  }
                  new CassandraPartition(newIndex, projection, this, colToMaker)
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

/**
 * Represents one partition in the Cassandra ChunkSource.  Reads chunks lazily when streamReaders is called.
 * The index state can be cached for lower read latency.
 */
class CassandraPartition(index: ChunkIDPartitionChunkIndex,
                         projection: RichProjection,
                         scanner: CassandraChunkSource,
                         colToMaker: ChunkSetReader.ColumnToMaker) extends FiloPartition with StrictLogging {
  import ChunkSetInfo._
  import Iterators._

  val binPartition = index.binPartition

  def numChunks: Int = index.numChunks

  def latestChunkLen: Int = index.latestN(1).toSeq.headOption.map(_._1.numRows).getOrElse(0)

  // For now just report a dummy shard.  In the future figure this out.
  val shard = 0

  private val chunkTable = scanner.getOrCreateChunkTable(projection.datasetRef)
  private val readers = new NonBlockingHashMapLong[ChunkSetReader](32, false)

  // NOTE: positions are index of partitionColumns/nonPartitionColumns within projection
  override def streamReaders(method: ChunkScanMethod, positions: Array[Int]): Observable[ChunkSetReader] = {
    val columns = positions.map { p =>
      if (p < 0) projection.partitionColumns(-p - 1) else projection.nonPartitionColumns(p)
    }
    val columnNames = columns.map(_.name).toArray
    val makers = columns.map(colToMaker).toArray

    // parse ChunkScanMethod into infosSkips....
    val (rangeQuery, infosSkips) = method match {
      case AllChunkScan             => (true, index.allChunks.toSeq)
      case RowKeyChunkScan(k1, k2)  => (false, index.rowKeyRange(k1.binRec, k2.binRec).toSeq)
      case SingleChunkScan(key, id) => (false, index.singleChunk(key.binRec, id).toSeq)
      case LastSampleChunkScan      => (false, index.latestN(1).toSeq)
    }
    logger.debug(s"Reading chunks from columns ${columnNames.toList}, ${index.binPartition}, method $method")

    // from infoSkips, create the MutableChunkSetReader's
    infosSkips.foreach { case (info, skips) =>
      readers.put(info.id, new MutableChunkSetReader(info, index.binPartition, skips, makers))
    }

    // Read chunks in, populate MutableChunkSetReader's, and emit readers when they are full
    val ids = infosSkips.map(_._1.id).toBuffer
    val chunkStreams = (0 until positions.size).map { pos =>
      chunkTable.readChunks(index.binPartition, 0, columnNames(pos), pos, ids, false)
                .switchIfEmpty(scanner.emptyChunkStream(infosSkips, pos))
    }
    Observable.merge(chunkStreams: _*)
              .map { case SingleChunkInfo(id, colNo, buf) =>
                readers.get(id) match {
                  // scalastyle:off
                  case null =>
                  // scalastyle:on
                    scanner.stats.incrChunkWithNoInfo()
                    None
                  case reader: MutableChunkSetReader =>
                    reader.addChunk(colNo, buf)
                    if (reader.isFull) {
                      readers.remove(id)
                      scanner.stats.incrReadChunksets()
                      Some(reader)
                    } else { None }
                }
              }.collect { case Some(reader) => reader }
  }

  def readers(method: ChunkScanMethod, positions: Array[Int]): Iterator[ChunkSetReader] =
    streamReaders(method, positions).toIterator()

  def lastVectors: Array[FiloVector[_]] = ???
}

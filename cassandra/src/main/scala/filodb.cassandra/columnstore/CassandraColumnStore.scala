package filodb.cassandra.columnstore

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import com.datastax.driver.core.{ConsistencyLevel, Metadata, Session, TokenRange}
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.cassandra.FiloCassandraConnector
import filodb.core._
import filodb.core.metadata.Schemas
import filodb.core.store._
import filodb.memory.BinaryRegionLarge

/**
 * Implementation of a column store using Apache Cassandra tables.
 * This class must be thread-safe as it is intended to be used concurrently.
 *
 * ==Configuration==
 * {{{
 *   cassandra {
 *     session-provider-fqcn = filodb.cassandra.DefaultFiloSessionProvider
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
 * @param sched A Scheduler for writes
 */
class CassandraColumnStore(val config: Config, val readEc: Scheduler,
                           val session: Session,
                           val downsampledData: Boolean = false)
                          (implicit val sched: Scheduler)
extends ColumnStore with CassandraChunkSource with StrictLogging {
  import collection.JavaConverters._

  import filodb.core.store._

  logger.info(s"Starting CassandraColumnStore with config ${cassandraConfig.withoutPath("password")}")

  val schemas = Schemas.fromConfig(config).get

  private val writeParallelism = cassandraConfig.getInt("write-parallelism")
  private val indexScanParallelismPerShard =
    Math.min(cassandraConfig.getInt("index-scan-parallelism-per-shard"), Runtime.getRuntime.availableProcessors())
  private val pkByUTNumSplits = cassandraConfig.getInt("pk-by-updated-time-table-num-splits")
  private val pkv2NumBuckets = cassandraConfig.getInt("pk-v2-table-num-buckets")
  private val writeTimeIndexTtlSeconds = cassandraConfig.getDuration("write-time-index-ttl", TimeUnit.SECONDS).toInt
  // NOTE: the following TTL is used in the `writePartKeyUpdates` call.
  private val pkUpdatesTTLSeconds = cassandraConfig.getDuration("pk-published-updates-ttl", TimeUnit.SECONDS).toInt
  private val createTablesEnabled = cassandraConfig.getBoolean("create-tables-enabled")
  private val numTokenRangeSplitsForScans = cassandraConfig.getInt("num-token-range-splits-for-scans")

  val sinkStats = new ChunkSinkStats

  val writeChunksetLatency = Kamon.histogram("cass-write-chunkset-latency", MeasurementUnit.time.milliseconds)
                                              .withoutTags()
  val writePksLatency = Kamon.histogram("cass-write-part-keys-latency", MeasurementUnit.time.milliseconds)
                                              .withoutTags()
  val readChunksBatchLatency = Kamon.histogram("cassandra-per-batch-chunk-read-latency",
                            MeasurementUnit.time.milliseconds).withoutTags()

  def initialize(dataset: DatasetRef, numShards: Int, resources: Config): Future[Response] = {
    // Initialize clusterConnector with dataset specific keyspaces if provided
    initClusterConnector(dataset, resources)
    // Note the next two lines of code do not create the tables, just trigger the creation of proxy class
    val chunkTable = getOrCreateChunkTable(dataset)
    val partitionKeysByUpdateTimeTable = getOrCreatePartitionKeysByUpdateTimeTable(dataset)
    val pkPublishedUpdatesTable = getOrCreatePartKeyPublishedUpdatesTableCache(dataset)
    if (createTablesEnabled) {
      createKeyspace(dataset, chunkTable.keyspace)
      val indexTable = getOrCreateIngestionTimeIndexTable(dataset)
      // Important: make sure nodes are in agreement before any schema changes
      clusterMeta.checkSchemaAgreement()

      def partitionKeysV2TableInit(): Future[Response] = {
        if (partKeysV2TableEnabled) {
          val partitionKeysV2Table = getOrCreatePartitionKeysV2Table(dataset)
          partitionKeysV2Table.initialize()
        } else Future.successful(Success)
      }

      def partitionKeysV1TableInit(): Future[Response] = {
        if (!partKeysV2TableEnabled) {
          Observable.fromIterable(0.until(numShards)).map { s =>
            getOrCreatePartitionKeysTable(dataset, s)
          }.mapEval(t => Task.fromFuture(t.initialize())).toListL.runToFuture
            .map(_.find(_ != Success).getOrElse(Success))
        } else Future.successful(Success)
      }

      for {ctResp <- chunkTable.initialize() if ctResp == Success
           pkv2Resp <- partitionKeysV2TableInit() if pkv2Resp == Success
           ixResp <- indexTable.initialize() if ixResp == Success
           pkutResp <- partitionKeysByUpdateTimeTable.initialize() if pkutResp == Success
           partKeyTablesResp <- partitionKeysV1TableInit() if partKeyTablesResp == Success
           partKeyUpdatesTableResp <- pkPublishedUpdatesTable.initialize() if partKeyUpdatesTableResp == Success
      } yield Success
    } else {
      // ensure the table handles are eagerly created
      // Note tables are not being created here, just the handle(proxy class)
      if (partKeysV2TableEnabled) {
        getOrCreatePartitionKeysV2Table(dataset)
      } else {
        0.until(numShards).foreach(getOrCreatePartitionKeysTable(dataset, _))
      }
      Future.successful(Success)
    }
  }

  def truncate(dataset: DatasetRef, numShards: Int): Future[Response] = {
    logger.info(s"Clearing all data for dataset ${dataset}")
    val chunkTable = getOrCreateChunkTable(dataset)
    val partitionKeysByUpdateTimeTable = getOrCreatePartitionKeysByUpdateTimeTable(dataset)
    val pkPublishedUpdatesTable = getOrCreatePartKeyPublishedUpdatesTableCache(dataset)
    val indexTable = getOrCreateIngestionTimeIndexTable(dataset)
    clusterMeta.checkSchemaAgreement()

    def partitionKeysV2TableTruncate(): Future[Response] = {
      if (partKeysV2TableEnabled) {
        val partitionKeysV2Table = getOrCreatePartitionKeysV2Table(dataset)
        partitionKeysV2Table.clearAll()
      } else Future.successful(Success)
    }

    def partitionKeysV1TableTruncate(): Future[Response] = {
      if (!partKeysV2TableEnabled) {
        Observable.fromIterable(0.until(numShards)).map { s =>
          getOrCreatePartitionKeysTable(dataset, s)
        }.mapEval(t => Task.fromFuture(t.clearAll())).toListL.runToFuture
        .map(_.find(_ != Success).getOrElse(Success))
      } else Future.successful(Success)
    }

    for { ctResp    <- chunkTable.clearAll() if ctResp == Success
          pkv2Resp  <- partitionKeysV2TableTruncate() if pkv2Resp == Success
          ixResp    <- indexTable.clearAll() if ixResp == Success
          pkutResp  <- partitionKeysByUpdateTimeTable.clearAll() if pkutResp == Success
          partKeyTablesResp <- partitionKeysV1TableTruncate() if partKeyTablesResp == Success
          partKeyUpdatesTableResp <- pkPublishedUpdatesTable.clearAll() if partKeyUpdatesTableResp == Success
    } yield Success
  }

  def dropDataset(dataset: DatasetRef, numShards: Int): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(dataset)
    val partitionKeysByUpdateTimeTable = getOrCreatePartitionKeysByUpdateTimeTable(dataset)
    val pkPublishedUpdatesTable = getOrCreatePartKeyPublishedUpdatesTableCache(dataset)
    val indexTable = getOrCreateIngestionTimeIndexTable(dataset)
    clusterMeta.checkSchemaAgreement()

    def partitionKeysV2TableDrop(): Future[Response] = {
      if (partKeysV2TableEnabled) {
        val partitionKeysV2Table = getOrCreatePartitionKeysV2Table(dataset)
        partitionKeysV2Table.drop()
      } else Future.successful(Success)
    }

    def partitionKeysV1TableDrop(): Future[Response] = {
      if (!partKeysV2TableEnabled) {
        Observable.fromIterable(0.until(numShards)).map { s =>
          getOrCreatePartitionKeysTable(dataset, s)
        }.mapEval(t => Task.fromFuture(t.drop())).toListL.runToFuture
          .map(_.find(_ != Success).getOrElse(Success))
      } else Future.successful(Success)
    }

    for {ctResp <- chunkTable.drop() if ctResp == Success
         ixResp <- indexTable.drop() if ixResp == Success
         pkv2Resp  <- partitionKeysV2TableDrop() if pkv2Resp == Success
         pkutResp  <- partitionKeysByUpdateTimeTable.drop() if pkutResp == Success
         partKeyTablesResp <- partitionKeysV1TableDrop() if partKeyTablesResp == Success
         partKeyUpdatesTableResp <- pkPublishedUpdatesTable.drop() if partKeyUpdatesTableResp == Success
    } yield {
      chunkTableCache.remove(dataset)
      indexTableCache.remove(dataset)
      partKeysV2TableCache.remove(dataset)
      partitionKeysTableCache.remove(dataset)
      Success
    }
  }

  // Initial implementation: write each ChunkSet as its own transaction.  Will result in lots of writes.
  // Future optimization: group by token range and batch?
  def write(ref: DatasetRef,
            chunksets: Observable[ChunkSet],
            diskTimeToLiveSeconds: Long = 259200): Future[Response] = {
    chunksets.mapParallelUnordered(writeParallelism) { chunkset =>
      val start = System.currentTimeMillis()
      val partBytes = BinaryRegionLarge.asNewByteArray(chunkset.partition)
           val future =
             for { writeChunksResp   <- writeChunks(ref, partBytes, chunkset, diskTimeToLiveSeconds)
                   if writeChunksResp == Success
                   writeIndicesResp  <- writeIndices(ref, partBytes, chunkset, writeTimeIndexTtlSeconds)
                   if writeIndicesResp == Success
             } yield {
               writeChunksetLatency.record(System.currentTimeMillis() - start)
               sinkStats.chunksetWrite()
               writeIndicesResp
             }
           Task.fromFuture(future)
         }
         .countL.runToFuture
         .map { chunksWritten =>
           if (chunksWritten > 0) Success else NotApplied
         }
  }

  private def writeChunks(ref: DatasetRef,
                          partition: Array[Byte],
                          chunkset: ChunkSet,
                          diskTimeToLiveSeconds: Long): Future[Response] = {
    val chunkTable = getOrCreateChunkTable(ref)
    chunkTable.writeChunks(partition, chunkset.info, chunkset.chunks, sinkStats, diskTimeToLiveSeconds)
      .collect {
        case Success => chunkset.invokeFlushListener(); Success
      }
  }

  private def writeIndices(ref: DatasetRef,
                           partition: Array[Byte],
                           chunkset: ChunkSet,
                           diskTimeToLiveSeconds: Int): Future[Response] = {
    val indexTable = getOrCreateIngestionTimeIndexTable(ref)
    val info = chunkset.info
    val infos = Seq((info.ingestionTime, info.startTime, ChunkSetInfo.toBytes(info)))
    indexTable.writeIndices(partition, infos, sinkStats, diskTimeToLiveSeconds)
  }

  /**
    * Reads chunks by querying partitions by ingestion time range and subsequently filtering by user time range.
    *
    * Important Details:
    * 1. User End time is exclusive. Important since we should not downsample one sample in two job runs
    * 2. Since we do a query based on maxChunkTime which is usually configured to be slightly greater than
    *    flush interval, results can include chunks that are before the requested range. Callers need to
    *    handle this case.
    */
  // scalastyle:off parameter.number
  def getChunksByIngestionTimeRangeNoAsync(datasetRef: DatasetRef,
                                           splits: Iterator[ScanSplit],
                                           ingestionTimeStart: Long,
                                           ingestionTimeEnd: Long,
                                           userTimeStart: Long,
                                           endTimeExclusive: Long,
                                           maxChunkTime: Long,
                                           batchSize: Int,
                                           cassFetchSize: Int): Iterator[Seq[RawPartData]] = {
    val partKeys = splits.flatMap {
      case split: CassandraTokenRangeSplit =>
        val indexTable = getOrCreateIngestionTimeIndexTable(datasetRef)
        logger.debug(s"Querying cassandra for partKeys for split=$split ingestionTimeStart=$ingestionTimeStart " +
          s"ingestionTimeEnd=$ingestionTimeEnd")
        indexTable.scanPartKeysByIngestionTimeNoAsync(split.tokens, ingestionTimeStart, ingestionTimeEnd, cassFetchSize)
      case split => throw new UnsupportedOperationException(s"Unknown split type $split seen")
    }

    val chunksTable = getOrCreateChunkTable(datasetRef)
    partKeys.sliding(batchSize, batchSize).map { parts =>
      logger.debug(s"Querying cassandra for chunks from ${parts.size} partitions userTimeStart=$userTimeStart " +
        s"endTimeExclusive=$endTimeExclusive maxChunkTime=$maxChunkTime")
      // This could be more parallel, but decision was made to control parallelism at one place: In spark (via its
      // parallelism configuration. Revisit if needed later.
      val start = System.currentTimeMillis()
      try {
        chunksTable.readRawPartitionRangeBBNoAsync(parts, userTimeStart - maxChunkTime, endTimeExclusive)
      } finally {
        readChunksBatchLatency.record(System.currentTimeMillis() - start)
      }
    }
  }

  /**
   * Copy a range of partitionKey records to a target ColumnStore, for performing disaster recovery or
   * backfills.
   *
   * @param diskTimeToLiveSeconds ttl
   */
  // scalastyle:off method.length
  def copyPartitionKeysByTimeRange(datasetRef: DatasetRef,
                                   numOfShards: Int,
                                   splits: Iterator[ScanSplit],
                                   repairStartTime: Long,
                                   repairEndTime: Long,
                                   target: CassandraColumnStore,
                                   diskTimeToLiveSeconds: Int): Unit = {

    def copyRows(targetPartitionKeysTable: PartitionKeysTable, records: Set[PartKeyRecord],
                 shard: Int) = {
      val partKeys = records.map(partKeyRecord =>
        targetPartitionKeysTable.readPartKey(partKeyRecord.partKey) match {
          case Some(targetPkr) => compareAndGet(partKeyRecord, targetPkr)
          case None => partKeyRecord
        }
      )
      val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
      Await.result(
        target.writePartKeys(datasetRef,
          shard, Observable.fromIterable(partKeys), diskTimeToLiveSeconds, updateHour, !downsampledData),
        5.minutes
      )
    }

    def copyRowsV2(targetPartitionKeysTable: PartitionKeysV2Table, records: Set[PartKeyRecord]) = {
      val partKeys = records.map(pkr =>
        targetPartitionKeysTable.readPartKey(pkr.shard,
                                             PartKeyRecord.getBucket(pkr.partKey, schemas, pkv2NumBuckets),
                                             pkr.partKey) match {
          case Some(targetPkr) => compareAndGet(pkr, targetPkr)
          case None => pkr
        }
      )
      val updateHour = System.currentTimeMillis() / 1000 / 60 / 60
      Await.result(
        target.writePartKeys(datasetRef, shard = 0 /* not used */,
          Observable.fromIterable(partKeys), diskTimeToLiveSeconds, updateHour, !downsampledData),
        5.minutes // TODO leaving per old code. Need to configure this.
      )
    }

    if (partKeysV2TableEnabled) {
      // for every split, scan PartitionKeysV2Table
      for (split <- splits) {
        val tokens = split.asInstanceOf[CassandraTokenRangeSplit].tokens
        val srcPartKeysTable = getOrCreatePartitionKeysV2Table(datasetRef)
        val targetPartKeysTable = target.getOrCreatePartitionKeysV2Table(datasetRef)
        // CQL does not support OR operator. So we need to query separately to get the timeSeries partitionKeys
        // which were born or died during the data loss period (aka repair window).
        val rowsByStartTime = srcPartKeysTable.scanRowsByStartTimeRangeNoAsync(tokens, repairStartTime, repairEndTime)
        val rowsByEndTime = srcPartKeysTable.scanRowsByEndTimeRangeNoAsync(tokens, repairStartTime, repairEndTime)
        // add to a Set to eliminate duplicate entries.
        val records = rowsByStartTime.++(rowsByEndTime).map(PartitionKeysV2Table.rowToPartKeyRecord)
        if (records.nonEmpty)
          copyRowsV2(targetPartKeysTable, records)
      }
    } else {
      // for every split, scan PartitionKeysTable for all the shards.
      for (split <- splits; shard <- 0 until numOfShards) {
        val tokens = split.asInstanceOf[CassandraTokenRangeSplit].tokens
        val srcPartKeysTable = getOrCreatePartitionKeysTable(datasetRef, shard)
        val targetPartKeysTable = target.getOrCreatePartitionKeysTable(datasetRef, shard)
        // CQL does not support OR operator. So we need to query separately to get the timeSeries partitionKeys
        // which were born or died during the data loss period (aka repair window).
        val rowsByStartTime = srcPartKeysTable.scanRowsByStartTimeRangeNoAsync(tokens, repairStartTime, repairEndTime)
        val rowsByEndTime = srcPartKeysTable.scanRowsByEndTimeRangeNoAsync(tokens, repairStartTime, repairEndTime)
        // add to a Set to eliminate duplicate entries.
        val records = rowsByStartTime.++(rowsByEndTime).map(r => PartitionKeysTable.rowToPartKeyRecord(r, shard))
        if (records.nonEmpty)
          copyRows(targetPartKeysTable, records, shard)
      }
    }
  }
  // scalastyle:on method.length

  private def compareAndGet(sourceRec: PartKeyRecord, targetRec: PartKeyRecord): PartKeyRecord = {

    val startTime = // compare and get the oldest start time
      if (sourceRec.startTime < targetRec.startTime) sourceRec.startTime else targetRec.startTime
    val endTime = // compare and get the latest end time
      if (sourceRec.endTime > targetRec.endTime) sourceRec.endTime else targetRec.endTime
    PartKeyRecord(sourceRec.partKey, startTime, endTime, sourceRec.shard)
  }

  /**
    * Copy a range of chunks to a target ColumnStore, for performing disaster recovery or
    * backfills. This method can also be used to delete chunks, by specifying a ttl of zero.
    * If the target is the same as the source, then this effectively deletes from the source.
    *
    * @param diskTimeToLiveSeconds pass zero to delete chunks
    */
  // scalastyle:off null method.length
  def copyOrDeleteChunksByIngestionTimeRange(datasetRef: DatasetRef,
                                             splits: Iterator[ScanSplit],
                                             ingestionTimeStart: Long,
                                             ingestionTimeEnd: Long,
                                             batchSize: Int,
                                             target: CassandraColumnStore,
                                             diskTimeToLiveSeconds: Int): Unit =
  {
    val sourceIndexTable = getOrCreateIngestionTimeIndexTable(datasetRef)
    val sourceChunksTable = getOrCreateChunkTable(datasetRef)

    val targetIndexTable = target.getOrCreateIngestionTimeIndexTable(datasetRef)
    val targetChunksTable = target.getOrCreateChunkTable(datasetRef)

    val chunkInfos = new ArrayBuffer[ByteBuffer]()
    val futures = new ArrayBuffer[Future[Response]]()

    def finishBatch(partition: ByteBuffer): Unit = {
      if (diskTimeToLiveSeconds == 0) {
        futures += targetChunksTable.deleteChunks(partition, chunkInfos)
      } else {
        for (row <- sourceChunksTable.readChunksNoAsync(partition, chunkInfos).iterator.asScala) {
          futures += targetChunksTable.writeChunks(partition, row, sinkStats, diskTimeToLiveSeconds)
        }
      }

      chunkInfos.clear()

      for (f <- futures) {
        try {
          Await.result(f, Duration(10, SECONDS))
        } catch {
          case e: Exception => {
            logger.error(s"Async cassandra chunk copy failed", e)
          }
        }
      }

      futures.clear()
    }

    var lastPartition: ByteBuffer = null

    for (split <- splits) {
      val tokens = split.asInstanceOf[CassandraTokenRangeSplit].tokens
      val rows = sourceIndexTable.scanRowsByIngestionTimeNoAsync(tokens, ingestionTimeStart, ingestionTimeEnd)
      for (row <- rows) {
        val partition = row.getBytes(0) // partition

        if (!partition.equals(lastPartition)) {
          if (lastPartition != null) {
            finishBatch(lastPartition);
          }
          lastPartition = partition;
        }

        chunkInfos += row.getBytes(3) // info

        if (diskTimeToLiveSeconds == 0) {
          futures += targetIndexTable.deleteIndex(row);
        } else {
          futures += targetIndexTable.writeIndex(row, sinkStats, diskTimeToLiveSeconds);
        }

        if (chunkInfos.size >= batchSize) {
          finishBatch(partition)
        }
      }
    }

    if (lastPartition != null) {
      finishBatch(lastPartition);
    }
  }
  // scalastyle:on

  def shutdown(): Unit = shutdownConnector

  private def clusterMeta: Metadata = session.getCluster.getMetadata

  /**
   * Splits scans of a dataset across multiple token ranges.
   * @param splitsPerNode  - how much parallelism or ways to divide a token range on each node
   * @return each split will have token_start, token_end, replicas filled in
   */
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int = numTokenRangeSplitsForScans): Seq[ScanSplit] = {
    val keyspace = getKeyspace(dataset)
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

  def scanPartKeys(ref: DatasetRef, shard: Int): Observable[PartKeyRecord] = {

    if (partKeysV2TableEnabled) {
      val table = getOrCreatePartitionKeysV2Table(ref)
      table.scanPartKeys(shard, indexScanParallelismPerShard, pkv2NumBuckets)
    } else {
      val table = getOrCreatePartitionKeysTable(ref, shard)
      Observable.fromIterable(getScanSplits(ref)).flatMap { tokenRange =>
        table.scanPartKeys(tokenRange.asInstanceOf[CassandraTokenRangeSplit].tokens, indexScanParallelismPerShard)
      }
    }
  }

  // returns the persisted partKey record, or default partKey record (argument) if there is no persisted value.
  def getPartKeyRecordOrDefault(ref: DatasetRef,
                                shard: Int,
                                pkr: PartKeyRecord): PartKeyRecord = {

    val opt = if (partKeysV2TableEnabled) {
      val bucket = PartKeyRecord.getBucket(pkr.partKey, schemas, pkv2NumBuckets)
      getOrCreatePartitionKeysV2Table(ref).readPartKey(shard, bucket, pkr.partKey)
    } else {
      getOrCreatePartitionKeysTable(ref, shard).readPartKey(pkr.partKey)
    }
    opt.getOrElse(pkr)
  }

  def writePartKeyUpdates(ref: DatasetRef,
                          epoch5mBucket: Long,
                          updatedTimeMs: Long,
                          offset: Long,
                          tagSet: TagSet,
                          partKeys: Observable[PartKeyRecord]): Future[Response] = {
    val start = System.currentTimeMillis()
    val updatesTable = getOrCreatePartKeyPublishedUpdatesTableCache(ref)
    val ret = partKeys.mapParallelUnordered(writeParallelism) { pk =>
      val split = PartKeyRecord.getBucket(pk.partKey, schemas, pkByUTNumSplits)
      val updateFut = updatesTable.writePartKey(split, pkUpdatesTTLSeconds, epoch5mBucket, updatedTimeMs, offset, pk)
      Task.fromFuture(updateFut).map { resp =>
        // Track metrics for observability
        resp match {
          case Success => sinkStats.partKeyUpdatesSuccess(1, tagSet)
          // Failed for any other case
          case _ => sinkStats.partKeyUpdatesFailed(1, tagSet)
        }
        resp
      }
    }.findL(_.isInstanceOf[ErrorResponse]).map(_.getOrElse(Success)).runToFuture
    ret.onComplete { _ =>
      sinkStats.partKeyUpdatesLatency(System.currentTimeMillis() - start, tagSet)
    }
    ret
  }

  def getUpdatedPartKeysByTimeBucket(ref: DatasetRef,
                                     shard: Int,
                                     timeBucket: Long): Observable[PartKeyRecord] = {
    val updatesTable = getOrCreatePartKeyPublishedUpdatesTableCache(ref)
    Observable.fromIterable(0 until pkByUTNumSplits)
      .flatMap { split => updatesTable.scanPartKeys(shard, timeBucket, split) }
  }

  def writePartKeys(ref: DatasetRef,
                    shard: Int, // TODO not used if v2. Remove after migration to v2 tables
                    partKeys: Observable[PartKeyRecord],
                    diskTTLSeconds: Long, updateHour: Long,
                    writeToPkUTTable: Boolean = true): Future[Response] = {
    val pkByUTTable = getOrCreatePartitionKeysByUpdateTimeTable(ref)
    val start = System.currentTimeMillis()
    val ret = partKeys.mapParallelUnordered(writeParallelism) { pk =>
      val ttl = if (pk.endTime == Long.MaxValue) -1 else diskTTLSeconds
      // caller needs to supply hash for partKey - cannot be None
      // Logical & MaxValue needed to make split positive by zeroing sign bit
      val split = PartKeyRecord.getBucket(pk.partKey, schemas, pkByUTNumSplits)
      def writePk() = if (partKeysV2TableEnabled) {
        val bucket = PartKeyRecord.getBucket(pk.partKey, schemas, pkv2NumBuckets)
        val pkv2Table = getOrCreatePartitionKeysV2Table(ref)
        pkv2Table.writePartKey(bucket, pk, ttl)
      } else {
        val pkTable = getOrCreatePartitionKeysTable(ref, shard)
        pkTable.writePartKey(pk, ttl)
      }
      val writePkFut = writePk().flatMap {
        case resp if resp == Success && writeToPkUTTable =>
          pkByUTTable.writePartKey(pk.shard, updateHour, split, pk, writeTimeIndexTtlSeconds)
        case resp =>
          Future.successful(resp)
      }
      Task.fromFuture(writePkFut).map { resp =>
        sinkStats.partKeysWrite(1)
        resp
      }
    }.findL(_.isInstanceOf[ErrorResponse]).map(_.getOrElse(Success)).runToFuture
    ret.onComplete { _ =>
      writePksLatency.record(System.currentTimeMillis() - start)
    }
    ret
  }

  def scanPartKeysByStartEndTimeRangeNoAsync(ref: DatasetRef,
                                             shard: Int, // TODO not used for v2; Remove after migration to v2 tables
                                             split: (String, String),
                                             startTimeGTE: Long,
                                             startTimeLTE: Long,
                                             endTimeGTE: Long,
                                             endTimeLTE: Long): Iterator[PartKeyRecord] = {
    if (partKeysV2TableEnabled) {
      val pkTable = getOrCreatePartitionKeysV2Table(ref)
      pkTable.scanPksByStartEndTimeRangeNoAsync(split, startTimeGTE, startTimeLTE, endTimeGTE, endTimeLTE)
    } else {
      val pkTable = getOrCreatePartitionKeysTable(ref, shard)
      pkTable.scanPksByStartEndTimeRangeNoAsync(split, startTimeGTE, startTimeLTE, endTimeGTE, endTimeLTE)
    }
  }

  def deletePartKeyNoAsync(ref: DatasetRef,
                           shard: Int,
                           pk: Array[Byte]): Response = {
    if (partKeysV2TableEnabled) {
      val pkTable = getOrCreatePartitionKeysV2Table(ref)
      val bucket = PartKeyRecord.getBucket(pk, schemas, pkv2NumBuckets)
      pkTable.deletePartKeyNoAsync(shard, bucket, pk)
    } else {
      val pkTable = getOrCreatePartitionKeysTable(ref, shard)
      pkTable.deletePartKeyNoAsync(pk)
    }
  }

  def getPartKeysByUpdateHour(ref: DatasetRef,
                              shard: Int,
                              updateHour: Long): Observable[PartKeyRecord] = {
    val pkByUTTable = getOrCreatePartitionKeysByUpdateTimeTable(ref)
    Observable.fromIterable(0 until pkByUTNumSplits)
              .flatMap { split => pkByUTTable.scanPartKeys(shard, updateHour, split) }
  }
}

/**
  * FIXME this works only for Murmur3Partitioner because it generates
  * Long based tokens. If other partitioners are used, this can potentially break.
  * Correct way is to pass Token objects so CQL stmts can bind tokens with stmt.bind().setPartitionKeyToken(token)
  */
case class CassandraTokenRangeSplit(tokens: Seq[(String, String)],
                                    replicas: Set[InetSocketAddress]) extends ScanSplit {
  // NOTE: You need both the host string and the IP address for Spark's locality to work
  def hostnames: Set[String] = replicas.flatMap(r => Set(r.getHostString, r.getAddress.getHostAddress))
}

trait CassandraChunkSource extends RawChunkSource with StrictLogging {

  def config: Config
  def session: Session
  def readEc: Scheduler

  implicit val readSched = readEc

  val stats = new ChunkSourceStats

  def downsampledData: Boolean

  val cassandraConfig = config.getConfig("cassandra")
  val ingestionConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("ingestion-consistency-level"))
  val readConsistencyLevel = ConsistencyLevel.valueOf(cassandraConfig.getString("default-read-consistency-level"))
  val partKeysV2TableEnabled = cassandraConfig.getBoolean("part-keys-v2-table-enabled")
  val tableCacheSize = config.getInt("columnstore.tablecache-size")

  val chunkTableCache = concurrentCache[DatasetRef, TimeSeriesChunksTable](tableCacheSize)
  val partKeysV2TableCache = concurrentCache[DatasetRef, PartitionKeysV2Table](tableCacheSize)
  val indexTableCache = concurrentCache[DatasetRef, IngestionTimeIndexTable](tableCacheSize)
  val partKeysByUTTableCache = concurrentCache[DatasetRef, PartitionKeysByUpdateTimeTable](tableCacheSize)
  val partKeysPublishedUpdatesTableCache = concurrentCache[DatasetRef, PartKeyPublishedUpdatesTable](tableCacheSize)
  val partitionKeysTableCache = concurrentCache[DatasetRef,
                                  ConcurrentLinkedHashMap[Int, PartitionKeysTable]](tableCacheSize)

  private val clusterConnectors: mutable.Map[String, FiloCassandraConnector] = mutable.Map.empty

  private def getClusterConnector(dataset: DatasetRef): FiloCassandraConnector =
    clusterConnectors.getOrElseUpdate(dataset.dataset, newClusterConnector(ConfigFactory.empty))

  protected def initClusterConnector(dataset: DatasetRef, resources: Config): Unit = {
    if (!clusterConnectors.contains(dataset.dataset))
      clusterConnectors(dataset.dataset) = newClusterConnector(resources)
  }

  protected def shutdownConnector = clusterConnectors.headOption.map(_._2.shutdown())

  private def newClusterConnector(resources: Config) = new FiloCassandraConnector {
    def config: Config = if (resources.hasPath("cassandra"))
                            resources.getConfig("cassandra").withFallback(cassandraConfig)
                         else
                            cassandraConfig
    def session: Session = CassandraChunkSource.this.session
    def ec: ExecutionContext = readEc

    val keyspace: String = if (!downsampledData) config.getString("keyspace")
                           else config.getString("downsample-keyspace")
  }

  /**
    * Read chunks from persistent store. Note the following constraints under which query is optimized:
    *
    * 1. Within a cassandra partition, chunks are ordered by chunkId. ChunkIds have this property:
    * `chunkID(t1) > chunkId(t2) if and only if t1 > t2`.
    *
    * 2. All chunks have samples with a range of userTime. During ingestion, we restrict the maximum
    * range for the userTime. This restriction makes it possible to issue single CQL query to fetch
    * all relevant chunks from cassandra. We do this by searching for searching in cassandra for chunkIds
    * between `chunkID(queryStartTime - maxChunkTime)` and `chunkID(queryEndTime)`. The reason we need to
    * subtract maxChunkTime from queryStartTime is for the range to include the first chunk which may have
    * relevant data but may have a startTime outside the query range.
    *
    * @param ref dataset ref
    * @param maxChunkTime maximum userTime (in millis) allowed in a single chunk. This restriction makes it
    *                     possible to issue single CQL query to fetch all relevant chunks from cassandra
    * @param partMethod selector for partitions
    * @param chunkMethod selector for chunks
    * @return Stored chunks and infos for each matching partition
    */
  def readRawPartitions(ref: DatasetRef,
                        maxChunkTime: Long,
                        partMethod: PartitionScanMethod,
                        chunkMethod: ChunkScanMethod = AllChunkScan): Observable[RawPartData] = {
    val chunkTable = getOrCreateChunkTable(ref)
    partMethod match {
      case FilteredPartitionScan(CassandraTokenRangeSplit(tokens, _), Nil)  =>
        chunkTable.scanPartitionsBySplit(tokens)
      case _ =>
        val partitions = partMethod match {
          case MultiPartitionScan(p, _) => p
          case SinglePartitionScan(p, _) => Seq(p)
          case p => throw new UnsupportedOperationException(s"PartitionScan $p to be implemented later")
        }
        val (start, end) = if (chunkMethod == AllChunkScan) (minChunkUserTime, maxChunkUserTime)
                           else (chunkMethod.startTime - maxChunkTime, chunkMethod.endTime)
        chunkTable.readRawPartitionRange(partitions, start, end)
    }
  }

  def getOrCreateChunkTable(dataset: DatasetRef): TimeSeriesChunksTable = {

    chunkTableCache.getOrElseUpdate(dataset, { dataset: DatasetRef =>
      new TimeSeriesChunksTable(dataset, getClusterConnector(dataset),
        ingestionConsistencyLevel, readConsistencyLevel)(readEc) })
  }

  def getOrCreateIngestionTimeIndexTable(dataset: DatasetRef): IngestionTimeIndexTable = {
    indexTableCache.getOrElseUpdate(dataset,
                        { dataset: DatasetRef =>
                          new IngestionTimeIndexTable(dataset,
                            getClusterConnector(dataset), ingestionConsistencyLevel,
                            readConsistencyLevel)(readEc) })
}

  def getOrCreatePartitionKeysV2Table(dataset: DatasetRef): PartitionKeysV2Table = {
    require(partKeysV2TableEnabled) // to make sure we don't trigger table creation unintentionally
    partKeysV2TableCache.getOrElseUpdate(dataset, { dataset: DatasetRef =>
      new PartitionKeysV2Table(dataset, getClusterConnector(dataset), ingestionConsistencyLevel,
        readConsistencyLevel)(readEc)
    })
  }

  def getOrCreatePartitionKeysByUpdateTimeTable(dataset: DatasetRef): PartitionKeysByUpdateTimeTable = {
    partKeysByUTTableCache.getOrElseUpdate(dataset,
      { dataset: DatasetRef =>
        new PartitionKeysByUpdateTimeTable(dataset, getClusterConnector(dataset), ingestionConsistencyLevel,
          readConsistencyLevel)(readEc) })
  }

  def getOrCreatePartKeyPublishedUpdatesTableCache(dataset: DatasetRef): PartKeyPublishedUpdatesTable = {
    partKeysPublishedUpdatesTableCache.getOrElseUpdate(
      dataset,
      { dataset: DatasetRef =>
        new PartKeyPublishedUpdatesTable(dataset, getClusterConnector(dataset),
          ingestionConsistencyLevel, readConsistencyLevel)(readEc)
      }
    )
  }

  def getOrCreatePartitionKeysTable(dataset: DatasetRef, shard: Int): PartitionKeysTable = {
    require(!partKeysV2TableEnabled) // to make sure we don't trigger table creation unintentionally
    val map = partitionKeysTableCache.getOrElseUpdate(dataset, { _ =>
      concurrentCache[Int, PartitionKeysTable](tableCacheSize)
    })
    map.getOrElseUpdate(shard, { shard: Int =>
      new PartitionKeysTable(dataset, shard, getClusterConnector(dataset),
        ingestionConsistencyLevel, readConsistencyLevel)(readEc)
    })
  }

  def getKeyspace(dataset: DatasetRef): String = getClusterConnector(dataset).keyspace

  def createKeyspace(dataset: DatasetRef, keyspace: String): Unit =
    getClusterConnector(dataset).createKeyspace(keyspace)

  def reset(): Unit = {}

}

package filodb.core.memstore

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import kamon.metric.MeasurementUnit
import kamon.tag.TagSet
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.binaryrecord2.RecordSchema.schemaID
import filodb.core.downsample.{DownsampleConfig, DownsampledTimeSeriesShardStats}
import filodb.core.memstore.DownsampleIndexBootstrapper.{currentThreadScheduler, ShapeStats}
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.metadata.Column.ColumnType.HistogramColumn
import filodb.core.store.{AllChunkScan, ColumnStore, PartKeyRecord, SinglePartitionScan, TimeRangeChunkScan}
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.vectors.HistogramVector

class RawIndexBootstrapper(colStore: ColumnStore) {

  /**
   * Bootstrap the lucene index for the shard
   * using PartKeyRecord objects read from some persistent source.
   *
   * The partId used in the lucene index is generated by invoking
   * the function provided on the threadpool requested.
   *
   * @param index the lucene index to populate
   * @param shardNum shard number
   * @param ref dataset ref
   * @param assignPartId the function to invoke to get the partitionId to be used to populate the index record
   * @return number of updated records
   */
  def bootstrapIndexRaw(index: PartKeyIndexRaw,
                        shardNum: Int,
                        ref: DatasetRef)
                       (assignPartId: PartKeyRecord => Int): Task[Long] = {

    val recoverIndexLatency = Kamon.gauge("shard-recover-index-latency", MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis()
    colStore.scanPartKeys(ref, shardNum)
      .map { pk =>
        val partId = assignPartId(pk)
        // -1 is returned if we skiped the part key for any reason, such as
        // unknown schema or memory issues
        //
        // assignPartId will log these cases, so no extra logging is needed
        // here
        if (partId != -1) {
          index.addPartKey(pk.partKey, partId, pk.startTime, pk.endTime)()
        }
      }
      .countL
      .map { count =>
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - start)
        count
      }
  }
}

object DownsampleIndexBootstrapper {
  /**
   * Describes the shape-stats to be published against a specific key (i.e. sequence of label-values).
   */
  case class ShapeStats(key: Seq[String],
                        labelCount: Int,
                        keyLengths: Seq[Int],
                        valueLengths: Seq[Int],
                        metricLength: Int,
                        totalLength: Int,
                        bucketCount: Option[Int])

  val currentThreadScheduler = {
    Scheduler.apply(ExecutionContext.fromExecutor(cmd => cmd.run()))
  }
}



class DownsampleIndexBootstrapper(colStore: ColumnStore,
                                  schemas: Schemas,
                                  stats: DownsampledTimeSeriesShardStats,
                                  datasetRef: DatasetRef,
                                  downsampleConfig: DownsampleConfig) extends StrictLogging {
  val schemaHashToHistCol = schemas.schemas.values
    // Also include DS schemas
    .flatMap(schema => schema.downsample.map(Seq(_)).getOrElse(Nil) ++ Seq(schema))
    .map{ schema =>
      val histIndex = schema.data.columns.indexWhere(col => col.columnType == HistogramColumn)
      (schema.schemaHash, histIndex)
    }
    .filter{ case (_, histIndex) => histIndex > -1 }
    .toMap

  /**
   * Build/return a ShapeStats object that contains all data to be published as shape-stats metrics.
   * A ShapeStats is returned iff the stats should be published according to the config.
   */
  private def getShapeStats(pk: PartKeyRecord, shardNum: Int, schema: Schema): Option[ShapeStats] = {
    val statsKey = new mutable.ArraySeq[String](downsampleConfig.dataShapeKeyIndex.size)
    val labelValuePairs = schema.partKeySchema.toStringPairs(pk.partKey, UnsafeUtils.arayOffset)
    val keyLengths = new mutable.ArrayBuffer[Int](labelValuePairs.size)
    val valueLengths = new mutable.ArrayBuffer[Int](labelValuePairs.size)
    var metricLength = 0
    var totalLength = 0
    var bucketCount: Option[Int] = None
    labelValuePairs.foreach{ case (key, value) =>
      // Build the statsKey in the pre-defined order (we will use this to determine
      //   whether-or-not these stats should be published)
      val keyIndex = downsampleConfig.dataShapeKeyIndex.getOrElse(key, -1)
      if (keyIndex > -1) {
        statsKey(keyIndex) = value
      }
      totalLength += key.length + value.length
      keyLengths.append(key.length)
      valueLengths.append(value.length)
      if (key == schema.options.metricColumn) {
        metricLength = value.length
      }
    }
    if (!shouldPublishShapeStats(statsKey)) {
      return None
    }
    if (downsampleConfig.enableDataShapeBucketCount && schemaHashToHistCol.contains(schema.schemaHash)) {
      bucketCount = Some(getHistBucketCount(pk, shardNum, schema))
    }
    Some(ShapeStats(
      statsKey, labelValuePairs.size, keyLengths,
      valueLengths, metricLength, totalLength, bucketCount))
  }

  /**
   * Returns true iff the key is "covered" by the trie.
   * The key is "covered" iff each of:
   *   (a) there exists a path through the trie that sequentially
   *       steps through a prefix of the strings in `key`.
   *   (b) the path is ended by an empty Map.
   *
   * For example, each of the following tries cover the key:
   *   key: [foo, bar]
   *   trie1: Map(foo -> Map(bar -> Map()))
   *   trie2: Map(foo -> Map())
   *   trie3: Map()  // The prefix can be empty!
   *
   * @param trie a Map[String ,Map[String, Map[...]]]
   */
  private def keyIsCovered(key: Seq[String], trie: Map[String, Any]): Boolean = {
    var i = 0
    var ptr = trie
    while (ptr.nonEmpty) {
      val value = key(i)
      if (!ptr.contains(value)) {
        return false
      }
      ptr = ptr(value).asInstanceOf[Map[String, Any]]
      i += 1
    }
    true
  }

  /**
   * Returns true iff shape-stats should be published for the key.
   */
  private def shouldPublishShapeStats(key: Seq[String]): Boolean = {
    val allowTrie: Map[String, Any] = downsampleConfig.dataShapeAllowTrie
    val blockTrie: Map[String, Any] = downsampleConfig.dataShapeBlockTrie
    if (!keyIsCovered(key, allowTrie)) {
      return false
    }
    if (blockTrie.isEmpty) {
      return true
    }
    !keyIsCovered(key, blockTrie)
  }

  private def updateStatsWithTags(shapeStats: ShapeStats): Unit = {
    var builder = TagSet.builder()
    for ((key, value) <- downsampleConfig.dataShapeKeyPublishLabels.zip(shapeStats.key)) {
      builder = builder.add(key, value)
    }
    val tags = builder.build()
    stats.dataShapeLabelCount.withTags(tags).record(shapeStats.labelCount)
    for (keyLength <- shapeStats.keyLengths) {
      stats.dataShapeKeyLength.withTags(tags).record(keyLength)
    }
    for (valueLength <- shapeStats.valueLengths) {
      stats.dataShapeValueLength.withTags(tags).record(valueLength)
    }
    stats.dataShapeMetricLength.withTags(tags).record(shapeStats.metricLength)
    stats.dataShapeTotalLength.withTags(tags).record(shapeStats.totalLength)
    if (shapeStats.bucketCount.isDefined) {
      stats.dataShapeBucketCount.withTags(tags).record(shapeStats.bucketCount.get)
    }
  }

  private def updateStatsWithoutTags(shapeStats: ShapeStats): Unit = {
    stats.dataShapeLabelCount.record(shapeStats.labelCount)
    for (keyLength <- shapeStats.keyLengths) {
      stats.dataShapeKeyLength.record(keyLength)
    }
    for (valueLength <- shapeStats.valueLengths) {
      stats.dataShapeValueLength.record(valueLength)
    }
    stats.dataShapeMetricLength.record(shapeStats.metricLength)
    stats.dataShapeTotalLength.record(shapeStats.totalLength)
    if (shapeStats.bucketCount.isDefined) {
      stats.dataShapeBucketCount.record(shapeStats.bucketCount.get)
    }
  }

  /**
   * Publish metrics about the series' "shape", e.g. label/value lengths/counts, bucket counts, etc.
   */
  private def updateDataShapeStats(pk: PartKeyRecord, shardNum: Int, schema: Schema): Unit = {
    val shapeStats = getShapeStats(pk, shardNum, schema)
    if (!shapeStats.isDefined) {
      return
    }
    if (downsampleConfig.dataShapeKeyPublishLabels.nonEmpty) {
      updateStatsWithTags(shapeStats.get)
    } else {
      updateStatsWithoutTags(shapeStats.get)
    }
  }

  /**
   * Given a PartKeyRecord for a histogram, returns the count of buckets in its most-recent chunk.
   */
  private def getHistBucketCount(pk: PartKeyRecord, shardNum: Int, schema: Schema): Int = {
    val rawPartFut = colStore.readRawPartitions(
        datasetRef,
        pk.endTime,
        SinglePartitionScan(pk.partKey, shardNum),
        TimeRangeChunkScan(pk.endTime, pk.endTime))  // we only want the most-recent chunk
      .headL
      .runToFuture(currentThreadScheduler)
    val readablePart = {
      val rawPart = Await.result(rawPartFut, Duration(5, TimeUnit.SECONDS))
      new PagedReadablePartition(
        schema, shard = 0, partID = 0, partData = rawPart, minResolutionMs = 1)
    }
    val histReader = {
      val info = readablePart.infos(AllChunkScan).nextInfoReader
      val histCol = schemaHashToHistCol(schema.schemaHash)
      val ptr = info.vectorAddress(colId = histCol)
      val acc = info.vectorAccessor(colId = histCol)
      HistogramVector(acc, ptr)
    }
    histReader.buckets.numBuckets
  }


  /**
   * If configured, update data-shape stats.
   */
  def updateDataShapeStatsIfEnabled(pk: PartKeyRecord, shardNum: Int): Unit = {
    if (downsampleConfig.dataShapeKey.nonEmpty) {
      try {
        val schema = schemas(schemaID(pk.partKey, UnsafeUtils.arayOffset))
        updateDataShapeStats(pk, shardNum, schema)
      } catch {
        case t: Throwable =>
          logger.error("swallowing exception during data-shape stats update", t)
      }
    }
  }

  /**
   * Same as bootstrapIndexRaw, except that we parallelize lucene update for
   * faster bootstrap of large number of index entries in downsample cluster.
   * Not doing this in raw cluster since parallel TimeSeriesPartition
   * creation requires more careful contention analysis. Bootstrap index operation
   * builds entire index from scratch
   */
  def bootstrapIndexDownsample(index: PartKeyIndexDownsampled,
                               shardNum: Int,
                               ref: DatasetRef,
                               ttlMs: Long,
                               parallelism: Int = Runtime.getRuntime.availableProcessors()): Task[Long] = {
    val startCheckpoint = System.currentTimeMillis()
    val recoverIndexLatency = Kamon.gauge("shard-recover-index-latency", MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis() - ttlMs
    colStore.scanPartKeys(ref, shardNum)
      .filter(_.endTime > start)
      .mapParallelUnordered(parallelism) { pk =>
        Task.evalAsync {
          updateDataShapeStatsIfEnabled(pk, shardNum)
          index.addPartKey(pk.partKey, partId = -1, pk.startTime, pk.endTime)(
            pk.partKey.length,
            PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(pk.partKey, 0, pk.partKey.length)
          )
        }
      }
      .countL
      .map { count =>
        // Ensures index is made durable to secondary store
        index.commit()
        // Note that we do not set an end time for the Synced here, instead
        // we will do it from DownsampleTimeSeriesShard
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - startCheckpoint)
        count
      }
  }

  // scalastyle:off method.length
  /**
   * Refresh index with real-time data rom colStore's raw dataset
   * @param fromHour fromHour inclusive
   * @param toHour toHour inclusive
   * @param parallelism number of threads to use to concurrently load the index
   * @param lookUpOrAssignPartId function to invoke to assign (or lookup) partId to the partKey
   *
   * @return number of records refreshed
   */
  def refreshWithDownsamplePartKeys(
                                     index: PartKeyIndexDownsampled,
                                     shardNum: Int,
                                     ref: DatasetRef,
                                     fromHour: Long,
                                     toHour: Long,
                                     schemas: Schemas,
                                     parallelism: Int = Runtime.getRuntime.availableProcessors()): Task[Long] = {

    // This method needs to be invoked for updating a range of time in an existing index. This assumes the
    // Index is already present and we need to update some partKeys in it. The lookUpOrAssignPartId is expensive
    // The part keys byte array unlike in bootstrapIndexDownsample is not opaque. The part key is broken down into
    // key value pairs, looked up in index to find the already assigned partId if any. If no partId is found the next
    // available value from the counter in DownsampleTimeSeriesShard is allocated. However, since partId is an integer
    // the max value it can reach is 2^32. This is a lot of timeseries in one shard, however, with time, especially in
    // case of durable index, and in environments with high churn, partIds evicted are not reclaimed and we may
    // potentially exceed the limit requiring us to preiodically reclaim partIds, eliminate the notion of partIds or
    // comeup with alternate solutions to come up a partId which can either be a long value or some string
    // representation
    val recoverIndexLatency = Kamon.gauge("downsample-store-refresh-index-latency",
      MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis()
    Observable.fromIterable(fromHour to toHour).flatMap { hour =>
      colStore.getPartKeysByUpdateHour(ref, shardNum, hour)
    }.mapParallelUnordered(parallelism) { pk =>
      // Same PK can be updated multiple times, but they wont be close for order to matter.
      // Hence using mapParallelUnordered
      Task.evalAsync {
        updateDataShapeStatsIfEnabled(pk, shardNum)
        val downsamplePartKey = RecordBuilder.buildDownsamplePartKey(pk.partKey, schemas)
        downsamplePartKey.foreach { dpk =>
          index.upsertPartKey(dpk, partId = -1, pk.startTime, pk.endTime)(
            dpk.length, PartKeyLuceneIndex.partKeyByteRefToSHA256Digest(dpk, 0, dpk.length)
          )
        }
      }
    }
      .countL
      .map { count =>
        // Forces sync with underlying filesystem, on problem is for initial index sync
        // its all or nothing as we do not mark partial progress, but given the index
        // update is parallel it makes sense to wait for all to be added to index
        index.commit()
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - start)
        count
      }
  }
  // scalastyle:on method.length
}

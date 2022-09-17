package filodb.downsampler.chunk.windowprocessors

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.downsample._
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.store.{ChunkSet, RawPartData}
import filodb.core.{DatasetRef, ErrorResponse, Instance}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.{BatchedWindowProcessor, DownsamplerSettings, SingleWindowProcessor}
import filodb.memory.format.UnsafeUtils
import filodb.memory.{BinaryRegionLarge, MemFactory}
import filodb.query.exec.UnknownSchemaQueryErr
import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.reactive.Observable
import spire.syntax.cfor._


import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class DownsampleWindowProcessor(settings: DownsamplerSettings)
  extends SingleWindowProcessor with Instance with Serializable {
  @transient lazy val numPartitionsEncountered = Kamon.counter("num-partitions-encountered").withoutTags()
  @transient lazy val numPartitionsBlocked = Kamon.counter("num-partitions-blocked").withoutTags()
  @transient lazy val numPartitionsCompleted = Kamon.counter("num-partitions-completed").withoutTags()
  @transient lazy val numPartitionsNoDownsampleSchema = Kamon.counter("num-partitions-no-downsample-schema")
    .withoutTags()
  @transient lazy val numPartitionsFailed = Kamon.counter("num-partitions-failed").withoutTags()
  @transient lazy val numPartitionsSkipped = Kamon.counter("num-partitions-skipped").withoutTags()
  @transient lazy val numRawChunksDownsampled = Kamon.counter("num-raw-chunks-downsampled").withoutTags()
  @transient lazy val numDownsampledChunksWritten = Kamon.counter("num-downsampled-chunks-written").withoutTags()

  @transient lazy val downsampleBatchLatency = Kamon.histogram("downsample-batch-latency",
    MeasurementUnit.time.milliseconds).withoutTags()
  @transient lazy val downsampleSinglePartLatency = Kamon.histogram("downsample-single-partition-latency",
    MeasurementUnit.time.milliseconds).withoutTags()
  @transient lazy val downsampleBatchPersistLatency = Kamon.histogram("cassandra-downsample-batch-persist-latency",
    MeasurementUnit.time.milliseconds).withoutTags()

  @transient lazy private val session = DownsamplerContext.getOrCreateCassandraSession(settings.cassandraConfig)

  @transient lazy private[downsampler] val downsampleCassandraColStore =
    new CassandraColumnStore(settings.filodbConfig, DownsamplerContext.readSched, session,
      true)(DownsamplerContext.writeSched)

  @transient lazy private[downsampler] val rawCassandraColStore =
    new CassandraColumnStore(settings.filodbConfig, DownsamplerContext.readSched, session,
      false)(DownsamplerContext.writeSched)

  @transient lazy private val kamonTags = Map("rawDataset" -> settings.rawDatasetName,
    "owner" -> "BatchDownsampler")

  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(settings.filodbConfig).get

  @transient lazy private val rawSchemas = settings.rawSchemaNames.map { s => schemas.schemas(s) }

  /**
   * Downsample Schemas
   */
  @transient lazy private val dsSchemas = settings.rawSchemaNames.flatMap { s => schemas.schemas(s).downsample }

  /**
   * Chunk Downsamplers by Raw Schema Id
   */
  @transient lazy private val chunkDownsamplersByRawSchemaId = {
    val map = debox.Map.empty[Int, scala.Seq[ChunkDownsampler]]
    rawSchemas.foreach { s => map += s.schemaHash -> s.data.downsamplers }
    map
  }

  @transient lazy private val downsamplePeriodMarkersByRawSchemaId = {
    val map = debox.Map.empty[Int, DownsamplePeriodMarker]
    rawSchemas.foreach { s => map += s.schemaHash -> s.data.downsamplePeriodMarker }
    map
  }

  /**
   * Raw dataset from which we downsample data
   */
  @transient lazy private[downsampler] val rawDatasetRef = DatasetRef(settings.rawDatasetName)

  // FIXME * 4 exists to workaround an issue where we see under-allocation for metaspan due to
  // possible mis-calculation of max block meta size.
  @transient lazy private val maxMetaSize = dsSchemas.map(_.data.blockMetaSize).max * 4

  /**
   * Datasets to which we write downsampled data. Keyed by Downsample resolution.
   */
  @transient lazy private[downsampler] val downsampleRefsByRes = settings.downsampleResolutions
    .zip(settings.downsampledDatasetRefs).toMap

  @transient lazy private[downsampler] val shardStats = new TimeSeriesShardStats(rawDatasetRef, -1) // TODO fix

  /**
   * Downsample batch of raw partitions, and store downsampled chunks to cassandra
   */
  // scalastyle:off method.length
  def downsampleBatchWrapper(rawPartData: RawPartData,
                             userEndTimeExclusive: Long,
                             necessaryPageThing: BatchedWindowProcessor#SharedWindowData): Unit = {
    val downsampledChunksToPersist = MMap[FiniteDuration, Iterator[ChunkSet]]()
    settings.downsampleResolutions.foreach { res =>
      downsampledChunksToPersist(res) = Iterator.empty
    }
    val downsampledPartsToFree = ArrayBuffer[TimeSeriesPartition]()
    val offHeapMem = new OffHeapMemory(rawSchemas.flatMap(_.downsample),
      kamonTags, maxMetaSize, settings.downsampleStoreConfig)
    var numDsChunks = 0
    val dsRecordBuilder = new RecordBuilder(MemFactory.onHeapFactory)
    try {
      numPartitionsEncountered.increment(1)
      val rawSchemaId = RecordSchema.schemaID(rawPartData.partitionKey, UnsafeUtils.arayOffset)
      val schema = schemas(rawSchemaId)
      if (schema != Schemas.UnknownSchema) {
        val pkPairs = schema.partKeySchema.toStringPairs(rawPartData.partitionKey, UnsafeUtils.arayOffset)
        if (settings.isEligibleForDownsample(pkPairs)) {
          try {
            val shouldTrace = settings.shouldTrace(pkPairs)
            downsamplePart(offHeapMem, rawPartData, necessaryPageThing, downsampledPartsToFree,
              downsampledChunksToPersist, userEndTimeExclusive, dsRecordBuilder, shouldTrace)
            numPartitionsCompleted.increment()
          } catch {
            case e: Exception =>
              DownsamplerContext.dsLogger.error(s"Error occurred when downsampling partition $pkPairs", e)
              numPartitionsFailed.increment()
          }
        } else {
          DownsamplerContext.dsLogger.debug(s"Skipping blocked partition $pkPairs")
          numPartitionsBlocked.increment()
        }
      } else {
        numPartitionsSkipped.increment()
        DownsamplerContext.dsLogger.warn(s"Skipping series with unknown schema ID $rawSchemaId")
      }
      numDsChunks = persistDownsampledChunks(downsampledChunksToPersist)
    } finally {
      offHeapMem.free() // free offheap mem
      downsampledPartsToFree.clear()
    }
  }

  /**
   * Creates new downsample partitions per per the resolutions
   * * specified by `bufferPools`.
   * Downsamples all chunks in `partToDownsample` per the resolutions and stores
   * downsampled data into the newly created partition.
   *
   * NOTE THAT THE OFF HEAP NEED TO BE FREED/SHUT DOWN BY THE CALLER ONCE CHUNKS ARE PERSISTED
   *
   * @param pagedPartsToFree           raw partitions that need to be freed are added to this mutable list
   * @param downsampledPartsToFree     downsample partitions to be freed are added to this mutable list
   * @param downsampledChunksToPersist downsample chunks to persist are added to this mutable map
   */
  // scalastyle:off parameter.number
  private def downsamplePart(offHeapMem: OffHeapMemory,
                             rawPart: RawPartData,
                             sharedWindowData: BatchedWindowProcessor#SharedWindowData,
                             downsampledPartsToFree: ArrayBuffer[TimeSeriesPartition],
                             downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]],
                             userTimeEndExclusive: Long,
                             dsRecordBuilder: RecordBuilder,
                             shouldTrace: Boolean) = {

    val rawSchemaId = RecordSchema.schemaID(rawPart.partitionKey, UnsafeUtils.arayOffset)
    val rawPartSchema = schemas(rawSchemaId)
    if (rawPartSchema == Schemas.UnknownSchema) throw UnknownSchemaQueryErr(rawSchemaId)
    rawPartSchema.downsample match {
      case Some(downsampleSchema) =>
        val rawReadablePart = sharedWindowData.getReadablePartition()
        DownsamplerContext.dsLogger.debug(s"Downsampling partition ${rawReadablePart.stringPartition}")
        val downsamplers = chunkDownsamplersByRawSchemaId(rawSchemaId)
        val periodMarker = downsamplePeriodMarkersByRawSchemaId(rawSchemaId)
        val shardInfo = TimeSeriesShardInfo(-1, shardStats, offHeapMem.bufferPools, offHeapMem.nativeMemoryManager)

        val (_, partKeyPtr, _) = BinaryRegionLarge.allocateAndCopy(rawReadablePart.partKeyBase,
          rawReadablePart.partKeyOffset,
          offHeapMem.nativeMemoryManager)

        // update schema of the partition key to downsample schema
        RecordBuilder.updateSchema(UnsafeUtils.ZeroPointer, partKeyPtr, downsampleSchema)

        val downsampledParts = settings.downsampleResolutions.zip(settings.downsampledDatasetRefs).map {
          case (res, ref) =>
            val part = if (shouldTrace)
              new TracingTimeSeriesPartition(0, ref, downsampleSchema, partKeyPtr, shardInfo, 1)
            else
              new TimeSeriesPartition(0, downsampleSchema, partKeyPtr, shardInfo, 1)
            res -> part
        }.toMap

        val downsamplePartStart = System.currentTimeMillis()
        downsampleChunks(offHeapMem, sharedWindowData, downsamplers, periodMarker,
          downsampledParts, userTimeEndExclusive, dsRecordBuilder, shouldTrace)

        downsampledPartsToFree ++= downsampledParts.values

        downsampledParts.foreach { case (res, dsPartition) =>
          dsPartition.switchBuffers(offHeapMem.blockMemFactory, true)
          val newIt = downsampledChunksToPersist(res) ++ dsPartition.makeFlushChunks(offHeapMem.blockMemFactory)
          downsampledChunksToPersist(res) = newIt
        }
        downsampleSinglePartLatency.record(System.currentTimeMillis() - downsamplePartStart)
      case None =>
        numPartitionsNoDownsampleSchema.increment()
        DownsamplerContext.dsLogger.debug(s"Skipping downsampling of partition " +
          s"${rawPartSchema.partKeySchema.stringify(rawPart.partitionKey)} which does not have a downsample schema")
    }
  }

  /**
   * Downsample chunks in a partition, ingest the downsampled data into downsampled partitions
   *
   * @param rawPartToDownsample raw partition to downsample
   * @param downsamplers        chunk downsamplers to use to downsample
   * @param downsampleResToPart the downsample parts in which to ingest downsampled data
   */
  private def downsampleChunks(offHeapMem: OffHeapMemory,
                               sharedWindowData: BatchedWindowProcessor#SharedWindowData,
                               downsamplers: Seq[ChunkDownsampler],
                               periodMarker: DownsamplePeriodMarker,
                               downsampleResToPart: Map[FiniteDuration, TimeSeriesPartition],
                               userTimeEndExclusive: Long,
                               dsRecordBuilder: RecordBuilder,
                               shouldTrace: Boolean) = {

    require(downsamplers.size > 1,
      s"Number of downsamplers for ${sharedWindowData.getReadablePartition().stringPartition} should be > 1")

    // for each downsample resolution
    sharedWindowData.getChunkRanges().foreach { cr =>
      val chunkSet = cr.chunkSetInfoReader
      val startRow = cr.istartRow
      val endRow = cr.iendRow
      val rawPartToDownsample = sharedWindowData.getReadablePartition()

      downsampleResToPart.foreach { case (resolution, part) =>
      val resMillis = resolution.toMillis

        if (shouldTrace) {
          downsamplers.zipWithIndex.foreach { case (d, i) =>
            val ptr = chunkSet.vectorAddress(i)
            val acc = chunkSet.vectorAccessor(i)
            val reader = rawPartToDownsample.chunkReader(i, acc, ptr)
            DownsamplerContext.dsLogger.info(s"Hex Vectors: Col $i for ${rawPartToDownsample.stringPartition} uses " +
              s"downsampler ${d.encoded} vector=${reader.toHexString(acc, ptr)}RemoveEOL")
          }
        }

        val downsamplePeriods =
          periodMarker.periods(rawPartToDownsample, chunkSet, resMillis, startRow, endRow).toArray()
        java.util.Arrays.sort(downsamplePeriods)

        if (shouldTrace)
          DownsamplerContext.dsLogger.info(s"Downsample Periods for ${part.stringPartition} " +
            s"${chunkSet.debugString} resolution=$resolution " +
            s"downsamplePeriods=${downsamplePeriods.mkString(",")}")

        try {
          // for each downsample period
          var first = startRow
          cforRange {
            0 until downsamplePeriods.length
          } { i =>
            val last = downsamplePeriods(i)

            dsRecordBuilder.startNewRecord(part.schema)
            // for each column, add downsample column value
            cforRange {
              0 until downsamplers.length
            } { col =>
              val downsampler = downsamplers(col)
              downsampler match {
                case t: TimeChunkDownsampler =>
                  dsRecordBuilder.addLong(t.downsampleChunk(rawPartToDownsample, chunkSet, first, last))
                case d: DoubleChunkDownsampler =>
                  dsRecordBuilder.addDouble(d.downsampleChunk(rawPartToDownsample, chunkSet, first, last))
                case h: HistChunkDownsampler =>
                  dsRecordBuilder.addBlob(h.downsampleChunk(rawPartToDownsample, chunkSet, first, last).serialize())
              }
            }
            dsRecordBuilder.endRecord(false)
            first = last + 1 // first row for next downsample period is last + 1
          }

          for {c <- dsRecordBuilder.allContainers
               row <- c.iterate(part.schema.ingestionSchema)
          } {
            part.ingest(userTimeEndExclusive, row, offHeapMem.blockMemFactory, createChunkAtFlushBoundary = false,
              flushIntervalMillis = Option.empty, acceptDuplicateSamples = false)
          }
        } catch {
          case e: Exception =>
            DownsamplerContext.dsLogger.error(s"Error downsampling partition " +
              s"hexPartKey=${rawPartToDownsample.hexPartKey} " +
              s"schema=${rawPartToDownsample.schema.name} " +
              s"resolution=$resolution " +
              s"startRow=$startRow " +
              s"endRow=$endRow " +
              s"downsamplePeriods=${downsamplePeriods.mkString("/")} " +
              s"chunkset: ${chunkSet.debugString}", e)
            // log debugging information and re-throw
            throw e
        }
        dsRecordBuilder.removeAndFreeContainers(dsRecordBuilder.allContainers.size)
      }
      numRawChunksDownsampled.increment()
    }
  }

  /**
   * Persist chunks in `downsampledChunksToPersist` to Cassandra.
   */
  private def persistDownsampledChunks(downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]]): Int = {
    val start = System.currentTimeMillis()
    @volatile var numChunks = 0
    // write all chunks to cassandra
    val writeFut = downsampledChunksToPersist.map { case (res, chunks) =>
      // FIXME if listener in chunkset below is not copied + overridden to no-op, we get a SEGV because
      // of a bug in either monix's mapAsync or cassandra driver where the future is completed prematurely.
      // This causes a race condition between free memory and chunkInfo.id access in updateFlushedId.
      val chunksToPersist = chunks.map { c =>
        numChunks += 1
        c.copy(listener = _ => {})
      }
      downsampleCassandraColStore.write(downsampleRefsByRes(res),
        Observable.fromIteratorUnsafe(chunksToPersist), settings.ttlByResolution(res))
    }

    writeFut.foreach { fut =>
      val response = Await.result(fut, settings.cassWriteTimeout)
      DownsamplerContext.dsLogger.debug(s"Got message $response for cassandra write call")
      if (response.isInstanceOf[ErrorResponse])
        DownsamplerContext.dsLogger.error(s"Got response $response when writing to Cassandra")
    }
    numDownsampledChunksWritten.increment(numChunks)
    downsampleBatchPersistLatency.record(System.currentTimeMillis() - start)
    numChunks
  }

  override def process(rawPartData: RawPartData,
                       userEndTime: Long,
                       sharedWindowData: BatchedWindowProcessor#SharedWindowData): Unit = {
    downsampleBatchWrapper(rawPartData, userEndTime, sharedWindowData)
  }
}

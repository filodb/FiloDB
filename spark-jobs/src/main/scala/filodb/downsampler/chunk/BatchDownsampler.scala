package filodb.downsampler.chunk

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.reactive.Observable
import spire.syntax.cfor._

import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, ErrorResponse, Instance}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.downsample._
import filodb.core.memstore._
import filodb.core.metadata.Schemas
import filodb.core.store.{ChunkSet, ReadablePartition}
import filodb.downsampler.{DownsamplerContext, Utils}
import filodb.memory.{BinaryRegionLarge, MemFactory}
import filodb.memory.format.UnsafeUtils
import filodb.query.exec.UnknownSchemaQueryErr

/**
  * This object maintains state during the processing of a batch of TSPartitions to downsample. Namely
  * a. The memory manager used for the paged partitions
  * b. The buffer pool used to ingest and chunk the downsampled data
  * c. Block store for overflow chunks that go beyond write buffers
  * d. Statistics
  * e. The Cassandra Store API from which to read raw data as well as write downsampled data
  *
  * It performs the operation of downsampling all partitions in the batch and writes downsampled data
  * into cassandra.
  *
  * All of the necessary params for the behavior are loaded from DownsampleSettings.
  */
class BatchDownsampler(settings: DownsamplerSettings) extends Instance with Serializable {

  @transient lazy val numBatchesStarted = Kamon.counter("num-batches-started").withoutTags()
  @transient lazy val numBatchesCompleted = Kamon.counter("num-batches-completed").withoutTags()
  @transient lazy val numBatchesFailed = Kamon.counter("num-batches-failed").withoutTags()
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

  @transient lazy private val kamonTags = Map( "rawDataset" -> settings.rawDatasetName,
                               "owner" -> "BatchDownsampler")

  @transient lazy private[downsampler] val schemas = Schemas.fromConfig(settings.filodbConfig).get

  @transient lazy private val rawSchemas = settings.rawSchemaNames.map { s => schemas.schemas(s)}

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
  def downsampleBatch(readablePartsBatch: Seq[ReadablePartition],
                      userTimeStart: Long,
                      userTimeEndExclusive: Long): Unit = {
    DownsamplerContext.dsLogger.info(s"Starting to downsample batchSize=${readablePartsBatch.size} partitions " +
      s"rawDataset=${settings.rawDatasetName} for " +
      s"userTimeStart=${java.time.Instant.ofEpochMilli(userTimeStart)} " +
      s"userTimeEndExclusive=${java.time.Instant.ofEpochMilli(userTimeEndExclusive)}")
    numBatchesStarted.increment()
    val startedAt = System.currentTimeMillis()
    val downsampledChunksToPersist = MMap[FiniteDuration, Iterator[ChunkSet]]()
    settings.downsampleResolutions.foreach { res =>
      downsampledChunksToPersist(res) = Iterator.empty
    }
    val pagedPartsToFree = ArrayBuffer[PagedReadablePartition]()
    val downsampledPartsToFree = ArrayBuffer[TimeSeriesPartition]()
    val offHeapMem = new OffHeapMemory(rawSchemas.flatMap(_.downsample),
      kamonTags, maxMetaSize, settings.downsampleStoreConfig)
    var numDsChunks = 0
    val dsRecordBuilder = new RecordBuilder(MemFactory.onHeapFactory)
    try {
      numPartitionsEncountered.increment(readablePartsBatch.length)
      readablePartsBatch.foreach { part =>
        val rawSchemaId = RecordSchema.schemaID(part.partKeyBytes, UnsafeUtils.arayOffset)
        val schema = schemas(rawSchemaId)
        if (schema != Schemas.UnknownSchema) {
          val pkPairs = schema.partKeySchema.toStringPairs(part.partKeyBytes, UnsafeUtils.arayOffset)
          if (settings.isEligibleForDownsample(pkPairs)) {
            try {
              val shouldTrace = settings.shouldTrace(pkPairs)
              downsamplePart(offHeapMem, part, pagedPartsToFree, downsampledPartsToFree,
                downsampledChunksToPersist, userTimeStart, userTimeEndExclusive, dsRecordBuilder, shouldTrace)
              numPartitionsCompleted.increment()
            } catch { case e: Exception =>
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
      }
      numDsChunks = persistDownsampledChunks(downsampledChunksToPersist)
    } catch { case e: Exception =>
      numBatchesFailed.increment()
      throw e // will be logged by spark
    } finally {
      downsampleBatchLatency.record(System.currentTimeMillis() - startedAt)
      offHeapMem.free()   // free offheap mem
      downsampledPartsToFree.clear()
    }
    numBatchesCompleted.increment()
    val endedAt = System.currentTimeMillis()
    DownsamplerContext.dsLogger.info(
      s"Finished iterating through and downsampling batchSize=${readablePartsBatch.size} " +
      s"partitions in current executor timeTakenMs=${endedAt-startedAt} numDsChunks=$numDsChunks")
  }

  /**
    * Creates new downsample partitions per per the resolutions
    * * specified by `bufferPools`.
    * Downsamples all chunks in `partToDownsample` per the resolutions and stores
    * downsampled data into the newly created partition.
    *
    * NOTE THAT THE OFF HEAP NEED TO BE FREED/SHUT DOWN BY THE CALLER ONCE CHUNKS ARE PERSISTED
    *
    * @param pagedPartsToFree raw partitions that need to be freed are added to this mutable list
    * @param downsampledPartsToFree downsample partitions to be freed are added to this mutable list
    * @param downsampledChunksToPersist downsample chunks to persist are added to this mutable map
    */
  // scalastyle:off parameter.number
  private def downsamplePart(offHeapMem: OffHeapMemory,
                             readablePart: ReadablePartition,
                             pagedPartsToFree: ArrayBuffer[PagedReadablePartition],
                             downsampledPartsToFree: ArrayBuffer[TimeSeriesPartition],
                             downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]],
                             userTimeStart: Long,
                             userTimeEndExclusive: Long,
                             dsRecordBuilder: RecordBuilder,
                             shouldTrace: Boolean) = {

    val rawSchemaId = RecordSchema.schemaID(readablePart.partKeyBytes, UnsafeUtils.arayOffset)
    val rawPartSchema = schemas(rawSchemaId)
    if (rawPartSchema == Schemas.UnknownSchema) throw UnknownSchemaQueryErr(rawSchemaId)
    rawPartSchema.downsample match {
      case Some(downsampleSchema) =>
        DownsamplerContext.dsLogger.debug(s"Downsampling partition ${readablePart.stringPartition}")
        val downsamplers = chunkDownsamplersByRawSchemaId(rawSchemaId)
        val periodMarker = downsamplePeriodMarkersByRawSchemaId(rawSchemaId)
        val shardInfo = TimeSeriesShardInfo(-1, shardStats, offHeapMem.bufferPools, offHeapMem.nativeMemoryManager)

        val (_, partKeyPtr, _) = BinaryRegionLarge.allocateAndCopy(readablePart.partKeyBase,
                                                   readablePart.partKeyOffset,
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
        downsampleChunks(offHeapMem, readablePart, downsamplers, periodMarker, downsampledParts,
                         userTimeStart, userTimeEndExclusive, dsRecordBuilder, shouldTrace)

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
          s"${rawPartSchema.partKeySchema.stringify(readablePart.partKeyBytes)} +" +
          s"which does not have a downsample schema")
    }
  }

  /**
    * Downsample chunks in a partition, ingest the downsampled data into downsampled partitions
    *
    * @param rawPartToDownsample raw partition to downsample
    * @param downsamplers chunk downsamplers to use to downsample
    * @param downsampleResToPart the downsample parts in which to ingest downsampled data
    */
  private def downsampleChunks(offHeapMem: OffHeapMemory,
                               rawPartToDownsample: ReadablePartition,
                               downsamplers: Seq[ChunkDownsampler],
                               periodMarker: DownsamplePeriodMarker,
                               downsampleResToPart: Map[FiniteDuration, TimeSeriesPartition],
                               userTimeStart: Long,
                               userTimeEndExclusive: Long,
                               dsRecordBuilder: RecordBuilder,
                               shouldTrace: Boolean) = {

    require(downsamplers.size > 1, s"Number of downsamplers for ${rawPartToDownsample.stringPartition} should be > 1")

    // for each chunk
    Utils.getChunkRangeIter(rawPartToDownsample, userTimeStart, userTimeEndExclusive).foreach{ chunkRange =>

      val chunkset = chunkRange.chunkSetInfoReader
      val startRow = chunkRange.istartRow
      val endRow = chunkRange.iendRow

      if (shouldTrace) {
        downsamplers.zipWithIndex.foreach { case (d, i) =>
          val ptr = chunkset.vectorAddress(i)
          val acc = chunkset.vectorAccessor(i)
          val reader = rawPartToDownsample.chunkReader(i, acc, ptr)
          DownsamplerContext.dsLogger.info(s"Hex Vectors: Col $i for ${rawPartToDownsample.stringPartition} uses " +
            s"downsampler ${d.encoded} vector=${reader.toHexString(acc, ptr)}RemoveEOL")
        }
      }

      // for each downsample resolution
      downsampleResToPart.foreach { case (resolution, part) =>
        val resMillis = resolution.toMillis

        val downsamplePeriods =
          periodMarker.periods(rawPartToDownsample, chunkset, resMillis, startRow, endRow).toArray()
        java.util.Arrays.sort(downsamplePeriods)

        if (shouldTrace)
          DownsamplerContext.dsLogger.info(s"Downsample Periods for ${part.stringPartition} " +
            s"${chunkset.debugString} resolution=$resolution " +
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
                  dsRecordBuilder.addLong(t.downsampleChunk(rawPartToDownsample, chunkset, first, last))
                case d: DoubleChunkDownsampler =>
                  dsRecordBuilder.addDouble(d.downsampleChunk(rawPartToDownsample, chunkset, first, last))
                case h: HistChunkDownsampler =>
                  dsRecordBuilder.addBlob(h.downsampleChunk(rawPartToDownsample, chunkset, first, last).serialize())
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
              s"chunkset: ${chunkset.debugString}", e)
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

}

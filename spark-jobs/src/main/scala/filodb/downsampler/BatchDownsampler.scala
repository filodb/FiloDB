package filodb.downsampler

import scala.collection.mutable.{ArrayBuffer, Map => MMap}
import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import java.util
import monix.execution.Scheduler
import monix.reactive.Observable
import scalaxy.loops._

import filodb.cassandra.FiloSessionProvider
import filodb.cassandra.columnstore.CassandraColumnStore
import filodb.core.{DatasetRef, ErrorResponse, Instance}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.downsample._
import filodb.core.memstore.{PagedReadablePartition, TimeSeriesPartition, TimeSeriesShardStats}
import filodb.core.metadata.Schemas
import filodb.core.store.{AllChunkScan, ChunkSet, RawPartData, ReadablePartition}
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
object BatchDownsampler extends StrictLogging with Instance {

  val settings = DownsamplerSettings

  private val readSched = Scheduler.io("cass-read-sched")
  private val writeSched = Scheduler.io("cass-write-sched")

  private val sessionProvider = settings.sessionProvider.map { p =>
                                              val clazz = createClass(p).get
                                              val args = Seq(classOf[Config] -> settings.cassandraConfig)
                                              createInstance[FiloSessionProvider](clazz, args).get
                                            }

  private[downsampler] val downsampleCassandraColStore =
    new CassandraColumnStore(settings.filodbConfig, readSched, sessionProvider, true)(writeSched)

  private[downsampler] val rawCassandraColStore =
    new CassandraColumnStore(settings.filodbConfig, readSched, sessionProvider, false)(writeSched)

  private val kamonTags = Map( "rawDataset" -> settings.rawDatasetName,
                               "owner" -> "BatchDownsampler")

  private[downsampler] val schemas = Schemas.fromConfig(settings.filodbConfig).get

  private val rawSchemas = settings.rawSchemaNames.map { s => schemas.schemas(s)}

  /**
    * Downsample Schemas
    */
  private val dsSchemas = settings.rawSchemaNames.map { s => schemas.schemas(s).downsample.get}

  /**
    * Chunk Downsamplers by Raw Schema Id
    */
  private val chunkDownsamplersByRawSchemaId = debox.Map.empty[Int, scala.Seq[ChunkDownsampler]]
  rawSchemas.foreach { s => chunkDownsamplersByRawSchemaId += s.schemaHash -> s.data.downsamplers }

  private val downsamplePeriodMarkersByRawSchemaId = debox.Map.empty[Int, DownsamplePeriodMarker]
  rawSchemas.foreach { s => downsamplePeriodMarkersByRawSchemaId += s.schemaHash -> s.data.downsamplePeriodMarker }

  /**
    * Raw dataset from which we downsample data
    */
  private[downsampler] val rawDatasetRef = DatasetRef(settings.rawDatasetName)

  // FIXME * 4 exists to workaround an issue where we see under-allocation for metaspan due to
  // possible mis-calculation of max block meta size.
  private val maxMetaSize = dsSchemas.map(_.data.blockMetaSize).max * 4

  /**
    * Datasets to which we write downsampled data. Keyed by Downsample resolution.
    */
  private[downsampler] val downsampleRefsByRes = settings.downsampleResolutions
                .zip(settings.downsampledDatasetRefs).toMap

  private[downsampler] val shardStats = new TimeSeriesShardStats(rawDatasetRef, -1) // TODO fix

  import java.time.Instant._

  /**
    * Downsample batch of raw partitions, and store downsampled chunks to cassandra
    */
  def downsampleBatch(rawPartsBatch: Seq[RawPartData],
                      userTimeStart: Long,
                      userTimeEndExclusive: Long): Unit = {
    logger.info(s"Starting to downsample batchSize=${rawPartsBatch.size} partitions " +
      s"rawDataset=${settings.rawDatasetName} for " +
      s"userTimeStart=${ofEpochMilli(userTimeStart)} userTimeEndExclusive=${ofEpochMilli(userTimeEndExclusive)}")

    val startedAt = System.currentTimeMillis()
    val downsampledChunksToPersist = MMap[FiniteDuration, Iterator[ChunkSet]]()
    settings.downsampleResolutions.foreach { res =>
      downsampledChunksToPersist(res) = Iterator.empty
    }
    val pagedPartsToFree = ArrayBuffer[PagedReadablePartition]()
    val downsampledPartsToFree = ArrayBuffer[TimeSeriesPartition]()
    val offHeapMem = new OffHeapMemory(rawSchemas.map(_.downsample.get),
      kamonTags, maxMetaSize, settings.downsampleStoreConfig)
    var numDsChunks = 0
    val dsRecordBuilder = new RecordBuilder(MemFactory.onHeapFactory)
    try {
      rawPartsBatch.foreach { rawPart =>
        val rawSchemaId = RecordSchema.schemaID(rawPart.partitionKey, UnsafeUtils.arayOffset)
        val schema = schemas(rawSchemaId)
        if (schema != Schemas.UnknownSchema) {
          val pkPairs = schema.partKeySchema.toStringPairs(rawPart.partitionKey, UnsafeUtils.arayOffset)
          if (isEligibleForDownsample(pkPairs)) {
            try {
              downsamplePart(offHeapMem, rawPart, pagedPartsToFree, downsampledPartsToFree,
                downsampledChunksToPersist, userTimeStart, userTimeEndExclusive, dsRecordBuilder)
            } catch { case e: Exception =>
                logger.error(s"Error occurred when downsampling partition $pkPairs", e)
                // TODO there certain exceptions caused by data that should not abort the job
                // Rerun of such batches will not help unless data is fixed.
                throw e
            }
          }
        } else logger.warn(s"Skipping series with unknown schema ID $rawSchemaId")
      }
      numDsChunks = persistDownsampledChunks(downsampledChunksToPersist)
    } finally {
      offHeapMem.free()   // free offheap mem
      pagedPartsToFree.clear()
      downsampledPartsToFree.clear()
    }

    val endedAt = System.currentTimeMillis()
    logger.info(s"Finished iterating through and downsampling batchSize=${rawPartsBatch.size} " +
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
  private def downsamplePart(offHeapMem: OffHeapMemory,
                             rawPart: RawPartData,
                             pagedPartsToFree: ArrayBuffer[PagedReadablePartition],
                             downsampledPartsToFree: ArrayBuffer[TimeSeriesPartition],
                             downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]],
                             userTimeStart: Long,
                             userTimeEndExclusive: Long,
                             dsRecordBuilder: RecordBuilder) = {
    val rawSchemaId = RecordSchema.schemaID(rawPart.partitionKey, UnsafeUtils.arayOffset)
    val rawPartSchema = schemas(rawSchemaId)
    if (rawPartSchema == Schemas.UnknownSchema) throw UnknownSchemaQueryErr(rawSchemaId)
    rawPartSchema.downsample match {
      case Some(downsampleSchema) =>
        val rawReadablePart = new PagedReadablePartition(rawPartSchema, 0, 0, rawPart)
        logger.debug(s"Downsampling partition ${rawReadablePart.stringPartition}")
        val bufferPool = offHeapMem.bufferPools(rawPartSchema.downsample.get.schemaHash)
        val downsamplers = chunkDownsamplersByRawSchemaId(rawSchemaId)
        val periodMarker = downsamplePeriodMarkersByRawSchemaId(rawSchemaId)
        val (_, partKeyPtr, _) = BinaryRegionLarge.allocateAndCopy(rawReadablePart.partKeyBase,
                                                   rawReadablePart.partKeyOffset,
                                                   offHeapMem.nativeMemoryManager)

        val downsampledParts = settings.downsampleResolutions.map { res =>
          val part = new TimeSeriesPartition(0, downsampleSchema, partKeyPtr,
                                            0, bufferPool, shardStats, offHeapMem.nativeMemoryManager, 1)
          res -> part
        }.toMap

        downsampleChunks(offHeapMem, rawReadablePart, downsamplers, periodMarker,
                         downsampledParts, userTimeStart, userTimeEndExclusive, dsRecordBuilder)

        pagedPartsToFree += rawReadablePart
        downsampledPartsToFree ++= downsampledParts.values

        downsampledParts.foreach { case (res, dsPartition) =>
          dsPartition.switchBuffers(offHeapMem.blockMemFactory, true)
          val newIt = downsampledChunksToPersist(res) ++ dsPartition.makeFlushChunks(offHeapMem.blockMemFactory)
          downsampledChunksToPersist(res) = newIt
        }
      case None =>
        logger.warn(s"Encountered partition ${rawPartSchema.partKeySchema.stringify(rawPart.partitionKey)}" +
          s" which does not have a downsample schema")
    }
  }

  /**
    * Downsample chunks in a partition, ingest the downsampled data into downsampled partitions
    *
    * @param rawPartToDownsample raw partition to downsample
    * @param downsamplers chunk downsamplers to use to downsample
    * @param downsampleResToPart the downsample parts in which to ingest downsampled data
    */
  // scalastyle:off method.length
  private def downsampleChunks(offHeapMem: OffHeapMemory,
                               rawPartToDownsample: ReadablePartition,
                               downsamplers: Seq[ChunkDownsampler],
                               periodMarker: DownsamplePeriodMarker,
                               downsampleResToPart: Map[FiniteDuration, TimeSeriesPartition],
                               userTimeStart: Long,
                               userTimeEndExclusive: Long,
                               dsRecordBuilder: RecordBuilder) = {
    val timestampCol = 0
    val rawChunksets = rawPartToDownsample.infos(AllChunkScan)

    // for each chunk
    while (rawChunksets.hasNext) {
      val chunkset = rawChunksets.nextInfoReader
      // Cassandra query to fetch eligible chunks is broader because of the increased maxChunkTime
      // Hence additional check is needed to ensure that chunk indeed overlaps with the downsample
      // user time range
      if (chunkset.startTime < userTimeEndExclusive && userTimeStart <= chunkset.endTime) {
        val tsPtr = chunkset.vectorAddress(timestampCol)
        val tsAcc = chunkset.vectorAccessor(timestampCol)
        val tsReader = rawPartToDownsample.chunkReader(timestampCol, tsAcc, tsPtr).asLongReader

        val startRow = tsReader.binarySearch(tsAcc, tsPtr, userTimeStart) & 0x7fffffff
        // userTimeEndExclusive-1 since ceilingIndex does an inclusive check
        val endRow = Math.min(tsReader.ceilingIndex(tsAcc, tsPtr, userTimeEndExclusive - 1), chunkset.numRows - 1)

        if (startRow <= endRow) {
          // for each downsample resolution
          downsampleResToPart.foreach { case (resolution, part) =>
            val resMillis = resolution.toMillis

            val downsamplePeriods =
              periodMarker.periods(rawPartToDownsample, chunkset, resMillis, startRow, endRow).toArray()
            util.Arrays.sort(downsamplePeriods)

            try {
              // for each downsample period
              var first = startRow
              for {i <- 0 until downsamplePeriods.length optimized} {
                val last = downsamplePeriods(i)

                dsRecordBuilder.startNewRecord(part.schema)
                // for each column, add downsample column value
                for {col <- 0 until downsamplers.length optimized} {
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
                part.ingest(userTimeEndExclusive, row, offHeapMem.blockMemFactory)
              }
            } catch {
              case e: Exception =>
                logger.error(s"Error downsampling partition ${rawPartToDownsample.stringPartition} " +
                  s"resolution=$resolution " +
                  s"startRow=$startRow " +
                  s"endRow=$endRow " +
                  s"downsamplePeriods=$downsamplePeriods " +
                  s"chunkset: ${chunkset.debugString(rawPartToDownsample.schema)}", e)
                // log debugging information and re-throw
                throw e
            }
            dsRecordBuilder.removeAndFreeContainers(dsRecordBuilder.allContainers.size)
          }
        } else {
          logger.warn(s"Skipping since startRow lessThan endRow when downsampling " +
            s"partition ${rawPartToDownsample.stringPartition} " +
            s"startRow=$startRow " +
            s"endRow=$endRow " +
            s"chunkset: ${chunkset.debugString(rawPartToDownsample.schema)}")
        }
      }
    }
  }

  /**
    * Persist chunks in `downsampledChunksToPersist` to Cassandra.
    */
  private def persistDownsampledChunks(
                                    downsampledChunksToPersist: MMap[FiniteDuration, Iterator[ChunkSet]]): Int = {
    @volatile
    var numChunks = 0
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
        Observable.fromIterator(chunksToPersist), settings.ttlByResolution(res))
    }

    writeFut.foreach { fut =>
      val response = Await.result(fut, settings.cassWriteTimeout)
      logger.debug(s"Got message $response for cassandra write call")
      if (response.isInstanceOf[ErrorResponse])
        logger.error(s"Got response $response when writing to Cassandra")
    }
    numChunks
  }

  /**
    * Two conditions should satisfy for eligibility:
    * (a) If whitelist is nonEmpty partKey should match a filter in the whitelist.
    * (b) It should not match any filter in blacklist
    */
  private def isEligibleForDownsample(pkPairs: Seq[(String, String)]): Boolean = {
    import DownsamplerSettings._
    if (whitelist.nonEmpty && !whitelist.exists(w => w.forall(pkPairs.contains))) {
      false
    } else {
      blacklist.forall(w => !w.forall(pkPairs.contains))
    }
  }

}

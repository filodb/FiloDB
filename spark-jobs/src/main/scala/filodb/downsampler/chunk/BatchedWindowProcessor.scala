package filodb.downsampler.chunk

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import kamon.Kamon

import filodb.core.Instance
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.PagedReadablePartition
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.store.{AllChunkScan, ChunkSetInfoReader, RawPartData, ReadablePartition}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.windowprocessors.{DownsampleWindowProcessor, ExportWindowProcessor}
import filodb.memory.format.UnsafeUtils

// TODO(a_theimer): would be nice if this could be an iterator
/**
 * Specifies a range of a chunk's rows.
 */
case class ChunkRange(chunkSetInfoReader: ChunkSetInfoReader,
                      istartRow: Int,
                      iendRow: Int)

/**
 * Lazily evaluated value; also requires that a lock is acquired before
 *   evaluation (to prevent multiple and/or concurrent evaluations).
 * @param producer produces the value to store.
 */
class LazySynchronized[T](producer: () => T) {
  var value: Option[T] = None
  def get(): T = {
    if (value.isEmpty) {
      value.synchronized{
        // second check in case we acquired the lock after another thread evaluated
        if (value.isEmpty) {
          value = Some(producer.apply())
        }
      }
    }
    value.get
  }
}

trait SingleWindowProcessor {
  /**
   * Process a window of data for a single partition.
   */
  def process(rawPartData: RawPartData,
              userEndTime: Long,
              sharedWindowData: BatchedWindowProcessor#SharedWindowData): Unit
}

class BatchedWindowProcessor(downsamplerSettings: DownsamplerSettings)
  extends Instance with Serializable {

  @transient lazy val numBatchesStarted = Kamon.counter("num-batches-started").withoutTags()
  @transient lazy val numBatchesCompleted = Kamon.counter("num-batches-completed").withoutTags()
  @transient lazy val numBatchesFailed = Kamon.counter("num-batches-failed").withoutTags()
  @transient lazy val numRawChunksSkipped = Kamon.counter("num-raw-chunks-skipped").withoutTags()

  @transient lazy val schemas = Schemas.fromConfig(downsamplerSettings.filodbConfig).get

  @transient lazy val singleWindowProcessors: Seq[SingleWindowProcessor] =
    Seq(new DownsampleWindowProcessor(downsamplerSettings),
            ExportWindowProcessor(schemas, downsamplerSettings))

  /**
   * This class enforces that expensive computations:
   *     (a) don't happen if they're never needed, or
   *     (b) happen exactly once, and their results are reused across SingleWindowProcessors.
   */
  class SharedWindowData(rawPartData: RawPartData,
                         userTimeStart: Long,
                         userTimeEndExclusive: Long,
                         rawPartSchema: Schema) {
    private val chunkRanges = new LazySynchronized[Seq[ChunkRange]](
        () => makeChunkRanges(pagedReadablePartition.get()))
    private val pagedReadablePartition = new LazySynchronized[PagedReadablePartition](
        () => new PagedReadablePartition(rawPartSchema, shard = 0, partID = 0, rawPartData, minResolutionMs = 1))

    /**
     * Page-in all chunks and determine, for each chunk, the range of rows with timestamps inside the time-range
     *   given by userTimeStart and userTimeEndExclusive.
     */
    private def makeChunkRanges(rawPart: ReadablePartition): Seq[ChunkRange] = {
      val timestampCol = 0  // FIXME: need a dynamic (but efficient) way to determine this
      val rawChunksets = rawPart.infos(AllChunkScan)

      // TODO(a_theimer): keeping this until confirmed we don't need to return an iterator
      val it = new Iterator[ChunkSetInfoReader]() {
        override def hasNext: Boolean = rawChunksets.hasNext
        override def next(): ChunkSetInfoReader = rawChunksets.nextInfoReader
      }

      it.filter(chunkset => chunkset.startTime < userTimeEndExclusive && userTimeStart <= chunkset.endTime)
        .map { chunkset =>
          val tsPtr = chunkset.vectorAddress(timestampCol)
          val tsAcc = chunkset.vectorAccessor(timestampCol)
          val tsReader = rawPart.chunkReader(timestampCol, tsAcc, tsPtr).asLongReader

          val startRow = tsReader.binarySearch(tsAcc, tsPtr, userTimeStart) & 0x7fffffff
          // userTimeEndExclusive-1 since ceilingIndex does an inclusive check
          val endRow = Math.min(tsReader.ceilingIndex(tsAcc, tsPtr, userTimeEndExclusive - 1), chunkset.numRows - 1)
          ChunkRange(chunkset, startRow, endRow)
        }
        .filter{ chunkRange =>
          val isValidChunk = chunkRange.istartRow <= chunkRange.iendRow
          if (!isValidChunk) {
            numRawChunksSkipped.increment()
            // TODO(a_theimer): use DsContext?
            DownsamplerContext.dsLogger.warn(s"Skipping chunk of partition since startRow lessThan endRow " +
              s"hexPartKey=${rawPart.hexPartKey} " +
              s"startRow=${chunkRange.istartRow} " +
              s"endRow=${chunkRange.iendRow} " +
              s"chunkset: ${chunkRange.chunkSetInfoReader.debugString}")
          }
          isValidChunk
        }
    }.toSeq

    def getReadablePartition(): ReadablePartition = {
      pagedReadablePartition.get
    }

    /**
     * For each chunk, determine the range of rows with timestamps inside the window.
     */
    def getChunkRanges(): Seq[ChunkRange] = {
      chunkRanges.get
    }
  }

  /**
   * Process a batch of partitions for a given time window.
   */
  def process(rawPartsBatch: Seq[RawPartData], userTimeStart: Long, userTimeEndExclusive: Long): Unit = {
    // TODO(a_theimer): use DownsamplerContext logger?
    DownsamplerContext.dsLogger.info(s"Starting to downsample " +
      s"batchSize=${rawPartsBatch.size} partitions " +
      s"rawDataset=${downsamplerSettings.rawDatasetName} for " +
      s"userTimeStart=${java.time.Instant.ofEpochMilli(userTimeStart)} " +
      s"userTimeEndExclusive=${java.time.Instant.ofEpochMilli(userTimeEndExclusive)}")
    numBatchesStarted.increment()
    val startedAt = System.currentTimeMillis()
    try {
      rawPartsBatch.foreach { rawPartData =>
        val rawSchemaId = RecordSchema.schemaID(rawPartData.partitionKey, UnsafeUtils.arayOffset)
        val schema = schemas(rawSchemaId)
        val sharedWindowData = new SharedWindowData(rawPartData, userTimeStart, userTimeEndExclusive, schema)
        // spawn threads for all processors except the first
        val futures = singleWindowProcessors.tail.map { processor =>
          Future[Unit] {
            processor.process(rawPartData, userTimeEndExclusive, sharedWindowData)
          }
        }
        // run the first processor synchronously here
        singleWindowProcessors.head.process(rawPartData, userTimeEndExclusive, sharedWindowData)
        // wait on the others to get done
        futures.foreach(Await.result(_, Duration.Inf))
      }
    } catch {
      case e: Exception =>
        numBatchesFailed.increment()
        throw e // will be logged by spark
    }
    numBatchesCompleted.increment()
    val endedAt = System.currentTimeMillis()
    DownsamplerContext.dsLogger.info(s"Finished iterating through and downsampling/exporting " +
      s"batchSize=${rawPartsBatch.size} partitions in current executor " +
      s"timeTakenMs=${endedAt-startedAt}")
  }
}

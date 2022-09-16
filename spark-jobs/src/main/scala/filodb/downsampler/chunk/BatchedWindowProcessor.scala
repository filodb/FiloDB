package filodb.downsampler.chunk

import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.PagedReadablePartition
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.store.{AllChunkScan, ChunkSetInfoReader, RawPartData, ReadablePartition}
import filodb.downsampler.DownsamplerContext
import filodb.downsampler.chunk.windowprocessors.{DownsampleWindowProcessor, ExportWindowProcessor}
import filodb.memory.format.UnsafeUtils
import kamon.Kamon

// TODO(a_theimer): docs / move
case class ChunkRows(chunkSetInfoReader: ChunkSetInfoReader, istartRow: Int, iendRow: Int)

// TODO(a_theimer): doc
trait SingleWindowProcessor {
  def process(rawPartData: RawPartData,
              userEndTime: Long,
              partitionAutoPager: PartitionAutoPager): Unit
}

// TODO(a_theimer): docs / move
class PartitionAutoPager(rawPartData: RawPartData,
                         userTimeStart: Long,
                         userTimeEndExclusive: Long,
                         rawPartSchema: Schema) {
  private lazy val pagedReadablePartition =
    new PagedReadablePartition(rawPartSchema, shard = 0, partID = 0, rawPartData, minResolutionMs = 1)
  private lazy val chunkRows = makeChunkRows(pagedReadablePartition)

  // TODO(a_theimer): needs larger scope
  @transient lazy val numRawChunksSkipped = Kamon.counter("num-raw-chunks-skipped").withoutTags()

  // TODO(a_theimer): docs
  private def makeChunkRows(rawPart: ReadablePartition): Seq[ChunkRows] = {
    val timestampCol = 0
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
        if (startRow > endRow) {
          numRawChunksSkipped.increment()
          DownsamplerContext.dsLogger.warn(s"Skipping chunk of partition since startRow lessThan endRow " +
            s"hexPartKey=${rawPart.hexPartKey} " +
            s"startRow=$startRow " +
            s"endRow=$endRow " +
            s"chunkset: ${chunkset.debugString}")
        }
        (chunkset, (startRow, endRow))
      }
      .filter{case (_, pair) => pair._1 <= pair._2}
      .map{case (chunk, pair) => ChunkRows(chunk, pair._1, pair._2)}
  }.toSeq

  def getReadablePartition(): ReadablePartition = {
    pagedReadablePartition
  }

  def getChunkRows(): Seq[ChunkRows] = {
    chunkRows
  }
}

// TODO(a_theimer): doc
case class BatchedWindowProcessor(schemas: Schemas, downsamplerSettings: DownsamplerSettings) {

  @transient lazy val numBatchesStarted = Kamon.counter("num-batches-started").withoutTags()
  @transient lazy val numBatchesCompleted = Kamon.counter("num-batches-completed").withoutTags()
  @transient lazy val numBatchesFailed = Kamon.counter("num-batches-failed").withoutTags()

  val singleWindowProcessors: Seq[SingleWindowProcessor] =
    Seq(new DownsampleWindowProcessor(downsamplerSettings),
            ExportWindowProcessor(schemas, downsamplerSettings))

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
        val partitionAutoPager = new PartitionAutoPager(rawPartData, userTimeStart, userTimeEndExclusive, schema)
        singleWindowProcessors.foreach { processor =>
          processor.process(rawPartData, userTimeEndExclusive, partitionAutoPager)
        }
      }
    } catch {
      case e: Exception =>
        numBatchesFailed.increment()
        throw e // will be logged by spark
    } finally {
      // TODO(a_theimer): release memory now? or after each completes?
    }
    // TODO(a_theimer): add "numDsChunks" replacement to below log
    numBatchesCompleted.increment()
    val endedAt = System.currentTimeMillis()
    DownsamplerContext.dsLogger.info(s"Finished iterating through and downsampling batchSize=${rawPartsBatch.size} " +
      s"partitions in current executor timeTakenMs=${endedAt-startedAt}")
  }
}

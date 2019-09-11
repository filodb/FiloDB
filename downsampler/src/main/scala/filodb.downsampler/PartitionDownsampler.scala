package filodb.downsampler

import scala.concurrent.duration.FiniteDuration

import com.typesafe.scalalogging.StrictLogging
import scalaxy.loops._

import filodb.core.downsample._
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesShardStats, WriteBufferPool}
import filodb.core.metadata.Schema
import filodb.core.store.{AllChunkScan, ReadablePartition}
import filodb.memory.{BlockMemFactory, MemFactory}
import filodb.memory.format.SeqRowReader

object PartitionDownsampler extends StrictLogging {

  /**
    * Creates new donwsample partitions per per the resolutions
    * * specified by `bufferPools`.
    * Downsamples all chunks in `partToDownsample` per the resolutions and stoes
    * downsampled data into the newly created partition.
    *
    * @param nativeMemManager downsampled chunks are stored into offheap memory
    *                         allocated by this memory manager
    * @param bufferPool buffers for the downsampled partitions are procured from this pool
    *
    * @return a TimeSeriesPartition for each resolution to be downsampled.
    *         NOTE THAT THE PARITIONS NEED TO BE FREED/SHUT DOWN ONCE CHUNKS ARE EXTRACTED FROM THEM
    */
  // scalastyle:off method.length parameter.number
  def downsample(shardStats: TimeSeriesShardStats,
                 resolutions: Seq[FiniteDuration],
                 downsampleSchema: Schema,
                 downsamplers: Seq[ChunkDownsampler],
                 partToDownsample: ReadablePartition,
                 blockHolder: BlockMemFactory,
                 bufferPool: WriteBufferPool,
                 nativeMemManager: MemFactory,
                 downsampleIngestionTime: Long,
                 userStartTime: Long,
                 userEndTime: Long): Map[FiniteDuration, TimeSeriesPartition] = {

    val resToPartition = resolutions.map { res =>
      val part = new TimeSeriesPartition(
        0,
        downsampleSchema,
        partToDownsample.partitionKey,
        0,
        bufferPool,
        shardStats,
        nativeMemManager,
        1)
      logger.trace(s"Creating new part=${part.hashCode}")
      res -> part
    }.toMap

    val chunksets = partToDownsample.infos(AllChunkScan)
    val timestampCol = 0
    // TODO create a rowReader that will not box the vals below
    val downsampleRow = new Array[Any](downsamplers.size)
    val downsampleRowReader = SeqRowReader(downsampleRow)

    while (chunksets.hasNext) {
      val chunkset = chunksets.nextInfo
      val startTime = chunkset.startTime
      val endTime = chunkset.endTime
      val vecPtr = chunkset.vectorPtr(timestampCol)
      val tsReader = partToDownsample.chunkReader(timestampCol, vecPtr).asLongReader

      // for each downsample resolution
      resToPartition.foreach { case (resolution, part) =>
        val resMillis = resolution.toMillis
        var pStart = ((startTime - 1) / resMillis) * resMillis + 1 // inclusive startTime for downsample period
        var pEnd = pStart + resMillis // end is inclusive
        // for each downsample period
        while (pStart <= endTime) {
          if (pEnd >= userStartTime && pEnd <= userEndTime) {
            // fix the boundary row numbers for the downsample period by looking up the timestamp column
            val startRowNum = tsReader.binarySearch(vecPtr, pStart) & 0x7fffffff
            val endRowNum = Math.min(tsReader.ceilingIndex(vecPtr, pEnd), chunkset.numRows - 1)

            // for each downsampler, add downsample column value
            for {col <- downsamplers.indices optimized} {
              val downsampler = downsamplers(col)
              downsampler match {
                case d: TimeChunkDownsampler =>
                  downsampleRow(col) = d.downsampleChunk(partToDownsample, chunkset, startRowNum, endRowNum)
                case d: DoubleChunkDownsampler =>
                  downsampleRow(col) = d.downsampleChunk(partToDownsample, chunkset, startRowNum, endRowNum)
                case h: HistChunkDownsampler =>
                  downsampleRow(col) = h.downsampleChunk(partToDownsample, chunkset, startRowNum, endRowNum)
                    .serialize()
              }
            }
            logger.trace(s"Ingesting into part=${part.hashCode}: $downsampleRow")
            part.ingest(downsampleIngestionTime, downsampleRowReader, blockHolder)
          }
          pStart += resMillis
          pEnd += resMillis
        }
      }
    }
    resToPartition

  }
}

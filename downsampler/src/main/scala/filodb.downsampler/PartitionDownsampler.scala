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

  // scalastyle:off method.length parameter.number
  def downsample(shardStats: TimeSeriesShardStats,
                 downsampleSchema: Schema,
                 downsamplers: Seq[ChunkDownsampler],
                 partToDownsample: ReadablePartition,
                 blockHolder: BlockMemFactory,
                 bufferPools: Map[FiniteDuration, WriteBufferPool],
                 nativeMemManager: MemFactory,
                 downsampleIngestionTime: Long,
                 userStartTime: Long,
                 userEndTime: Long): Map[FiniteDuration, TimeSeriesPartition] = {

    val resToPartition = bufferPools.map { case (res, bufPool) =>
      val part = new TimeSeriesPartition(
        0,
        downsampleSchema,
        partToDownsample.partitionKey,
        0,
        bufPool,
        shardStats,
        nativeMemManager,
        1)
      logger.trace(s"Creating new part=${part.hashCode}")
      res -> part
    }

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

package filodb.core.downsample

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesShardStats}
import filodb.core.metadata.Schema
import filodb.core.store.ChunkInfoIterator
import filodb.memory.MemFactory

final case class DownsampleRecords(resolution: Int, builder: RecordBuilder)

object ShardDownsampler extends StrictLogging {
  /**
    * Allocates record builders to store downsample records for chunksets that
    * are going to be newly encoded.
    *
    * One DownsampleRecords is returned for each resolution.
    *
    * CAREFUL: Should not reuse downsampling records in a flush period without being sure
    * that flush tasks running in parallel will not use the same object.
    */
  def newEmptyDownsampleRecords(resolutions: Seq[Int], enabled: Boolean): Seq[DownsampleRecords] = {
    if (enabled) {
      resolutions.map { res =>
        DownsampleRecords(res, new RecordBuilder(MemFactory.onHeapFactory))
      }
    } else {
      Seq.empty
    }
  }

}

/**
  * This class takes the responsibility of carrying out the
  * downsampling algorithms and publish to another dataset given the chunksets.
  * One ShardDownsampler exist for each source schema to target schema.
  */
class ShardDownsampler(datasetName: String,
                       shardNum: Int,
                       sourceSchema: Schema,
                       targetSchema: Schema,
                       enabled: Boolean,
                       stats: TimeSeriesShardStats) extends StrictLogging {
  private val downsamplers = sourceSchema.data.downsamplers

  if (enabled) {
    logger.info(s"Downsampling enabled for dataset=$datasetName shard=$shardNum with " +
      s"following downsamplers: ${downsamplers.map(_.encoded)}")
    logger.info(s"From source schema $sourceSchema to target schema $targetSchema")
  } else {
    logger.info(s"Downsampling disabled for dataset=$datasetName shard=$shardNum")
  }

  /**
    * Populates the builders in the DownsampleRecords objects with downsample data for the
    * chunkset passed in.
    *
    * Typically called for each chunkset at encoding time.
    */
  def populateDownsampleRecords(part: TimeSeriesPartition,
                                chunksets: ChunkInfoIterator,
                                records: Seq[DownsampleRecords]): Unit = {
    if (enabled) {
      val downsampleTrace = Kamon.spanBuilder("memstore-downsample-records-trace")
        .asChildOf(Kamon.currentSpan())
        .tag("dataset", datasetName)
        .tag("shard", shardNum).start()
      while (chunksets.hasNext) {
        val chunkset = chunksets.nextInfoReader
        val startTime = chunkset.startTime
        val endTime = chunkset.endTime
        val tsPtr = chunkset.vectorAddress(0)
        val tsAcc = chunkset.vectorAccessor(0)
        val tsReader = part.chunkReader(0, tsAcc, tsPtr).asLongReader
        // for each downsample resolution
        records.foreach { case DownsampleRecords(resolution, builder) =>
          var pStart = ((startTime - 1) / resolution) * resolution + 1 // inclusive startTime for downsample period
          var pEnd = pStart + resolution // end is inclusive
          // for each downsample period
          while (pStart <= endTime) {
            // fix the boundary row numbers for the downsample period by looking up the timestamp column
            val startRowNum = tsReader.binarySearch(tsAcc, tsPtr, pStart) & 0x7fffffff
            val endRowNum = Math.min(tsReader.ceilingIndex(tsAcc, tsPtr, pEnd), chunkset.numRows - 1)
            builder.startNewRecord(targetSchema)
            // for each downsampler, add downsample column value
            downsamplers.foreach {
              case d: TimeChunkDownsampler =>
                builder.addLong(d.downsampleChunk(part, chunkset, startRowNum, endRowNum))
              case d: DoubleChunkDownsampler =>
                builder.addDouble(d.downsampleChunk(part, chunkset, startRowNum, endRowNum))
              case h: HistChunkDownsampler =>
                builder.addBlob(h.downsampleChunk(part, chunkset, startRowNum, endRowNum).serialize())
            }
            // add partKey finally
            builder.addPartKeyRecordFields(part.partKeyBase, part.partKeyOffset, sourceSchema.partKeySchema)
            builder.endRecord(true)
            stats.downsampleRecordsCreated.increment()
            pStart += resolution
            pEnd += resolution
          }
        }
      }
      downsampleTrace.finish()
    }
  }
}

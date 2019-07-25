package filodb.core.downsample

import scala.concurrent.{ExecutionContext, Future}

import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.core.{ErrorResponse, Response, Success}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.memstore.{TimeSeriesPartition, TimeSeriesShardStats}
import filodb.core.metadata.Dataset
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkInfoIterator
import filodb.memory.MemFactory

final case class DownsampleRecords(resolution: Int, builder: RecordBuilder)

/**
  * This class takes the responsibility of carrying out the
  * downsampling algorithms and publish to another dataset given the chunksets.
  */
class ShardDownsampler(dataset: Dataset,
                       shardNum: Int,
                       enabled: Boolean,
                       resolutions: Seq[Int],
                       publisher: DownsamplePublisher,
                       stats: TimeSeriesShardStats) extends StrictLogging {
  private val downsamplers = dataset.schema.data.downsamplers

  if (enabled) {
    logger.info(s"Downsampling enabled for dataset=${dataset.ref} shard=$shardNum with " +
      s"following downsamplers: ${downsamplers.map(_.encoded)} at resolutions: $resolutions")
  } else {
    logger.info(s"Downsampling disabled for dataset=${dataset.ref} shard=$shardNum")
  }

  private[downsample] val downsampleSchema = downsampleIngestSchema()

  /**
    * Allocates record builders to store downsample records for chunksets that
    * are going to be newly encoded.
    *
    * One DownsampleRecords is returned for each resolution.
    *
    * CAREFUL: Should not reuse downsampling records in a flush period without being sure
    * that flush tasks running in parallel will not use the same object.
    */
  def newEmptyDownsampleRecords: Seq[DownsampleRecords] = {
    if (enabled) {
      resolutions.map { res =>
        DownsampleRecords(res, new RecordBuilder(MemFactory.onHeapFactory, downsampleSchema))
      }
    } else {
      Seq.empty
    }
  }

  /**
    * Formulates downsample schema using the downsampler configuration for dataset
    */
  private[downsample] def downsampleIngestSchema(): RecordSchema = {
    // The name of the column in downsample record does not matter at the ingestion side. Type does matter.
    val downsampleCols = downsamplers.map { d => ColumnInfo(s"${d.name.entryName}", d.colType) }
    new RecordSchema(downsampleCols ++ dataset.partKeySchema.columns,
      Some(downsampleCols.size), dataset.ingestionSchema.predefinedKeys)
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
      val downsampleTrace = Kamon.buildSpan("memstore-downsample-records-trace")
        .withTag("dataset", dataset.name)
        .withTag("shard", shardNum).start()
      val timestampCol = dataset.timestampColID
      while (chunksets.hasNext) {
        val chunkset = chunksets.nextInfo
        val startTime = chunkset.startTime
        val endTime = chunkset.endTime
        val vecPtr = chunkset.vectorPtr(timestampCol)
        val tsReader = part.chunkReader(timestampCol, vecPtr).asLongReader
        // for each downsample resolution
        records.foreach { case DownsampleRecords(resolution, builder) =>
          var pStart = ((startTime - 1) / resolution) * resolution + 1 // inclusive startTime for downsample period
          var pEnd = pStart + resolution // end is inclusive
          // for each downsample period
          while (pStart <= endTime) {
            // fix the boundary row numbers for the downsample period by looking up the timestamp column
            val startRowNum = tsReader.binarySearch(vecPtr, pStart) & 0x7fffffff
            val endRowNum = Math.min(tsReader.ceilingIndex(vecPtr, pEnd), chunkset.numRows - 1)
            builder.startNewRecord()
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
            builder.addPartKeyRecordFields(part.partKeyBase, part.partKeyOffset, dataset.partKeySchema)
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

  /**
    * Publishes the current data in downsample builders, typically to Kafka
    */
  def publishToDownsampleDataset(dsRecords: Seq[DownsampleRecords])
                                (implicit sched: ExecutionContext): Future[Response] = {
    if (enabled) {
      val responses = dsRecords.map { rec =>
        val containers = rec.builder.optimalContainerBytes(true)
        logger.debug(s"Publishing ${containers.size} downsample record containers " +
          s"of dataset=${dataset.ref} shard=$shardNum for resolution ${rec.resolution}")
        publisher.publish(shardNum, rec.resolution, containers)
      }
      Future.sequence(responses).map(_.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success))
    } else Future.successful(Success)
  }

}

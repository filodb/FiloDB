package filodb.core.downsample

import scala.concurrent.{ExecutionContext, Future}

import com.typesafe.scalalogging.StrictLogging

import filodb.core.{DatasetRef, ErrorResponse, Response, Success}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.memstore.TimeSeriesPartition
import filodb.core.metadata.Dataset
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkInfoIterator
import filodb.memory.MemFactory

final case class DownsamplingState(resolution: Int, builder: RecordBuilder)

object DownsampleOps extends StrictLogging {

  def initializeDownsamplerStates(dataset: Dataset,
                                  resolutions: Seq[Int],
                                  memFactory: MemFactory): Seq[DownsamplingState] = {
    val schema = downsampleIngestSchema(dataset)
    resolutions.map { res =>
      DownsamplingState(res, new RecordBuilder(memFactory, schema))
    }
  }

  /**
    * Formulates downsample schema using the downsampler configuration for dataset
    */
  def downsampleIngestSchema(dataset: Dataset): RecordSchema = {
    // The name of the column in downsample record does not matter at the ingestion side. Type does matter.
    val downsampleCols = dataset.downsamplers.map { d => ColumnInfo(s"${d.name}", d.colType) }
    new RecordSchema(downsampleCols ++ dataset.partKeySchema.columns,
      Some(downsampleCols.size), dataset.ingestionSchema.predefinedKeys)
  }

  /**
    * Populates the builders in the DownsamplingState with downsample data for the
    * chunkset passed in.
    */
  def downsample(dataset: Dataset,
                 part: TimeSeriesPartition,
                 chunksets: ChunkInfoIterator,
                 dsStates: Seq[DownsamplingState]): Unit = {
    while (chunksets.hasNext) {
      val chunkset = chunksets.nextInfo
      val startTime = chunkset.startTime
      val endTime = chunkset.endTime
      // for each downsample resolution
      dsStates.foreach { case DownsamplingState(res, builder) =>
        var pStart = ( (startTime - 1) / res) * res + 1 // inclusive startTime for downsample period
      var pEnd = pStart + res // end is inclusive
        // for each downsample period
        while (pStart <= endTime) {
          var startRowNum = 0
          var endRowNum = 0
          builder.startNewRecord()
          // assume that first row key column is timestamp column, fix the row numbers for the downsample period
          val timestampCol = dataset.rowKeyColumns.head.id
          val vecPtr = chunkset.vectorPtr(timestampCol)
          val colReader = part.chunkReader(timestampCol, vecPtr)
          startRowNum = colReader.asLongReader.binarySearch(vecPtr, pStart) & 0x7fffffff
          endRowNum = colReader.asLongReader.ceilingIndex(vecPtr, pEnd)
          // for each downsampler, add downsample column value
          dataset.downsamplers.foreach {
            case d: TimestampChunkDownsampler =>
              builder.addLong(d.downsampleChunk(part, chunkset, startRowNum, endRowNum))
            case d: DoubleChunkDownsampler =>
              builder.addDouble(d.downsampleChunk(part, chunkset, startRowNum, endRowNum))
          }
          // add partKey finally
          builder.addPartKeyFromBr(part.partKeyBase, part.partKeyOffset, dataset.partKeySchema)
          builder.endRecord(true)
          pStart += res
          pEnd += res
        }
      }
    }
  }

  /**
    * Publishes the data in downsample builders to Kafka (or an alternate implementation)
    */
  def publishDownsampleBuilders(dataset: DatasetRef,
                                publisher: DownsamplePublisher,
                                shardNum: Int,
                                states: Seq[DownsamplingState])(implicit sched: ExecutionContext): Future[Response] = {
    val responses = states.map { h =>
      val records = h.builder.optimalContainerBytes(true)
      logger.debug(s"Publishing ${records.size} downsample record containers " +
        s"of dataset=$dataset shard=$shardNum for resolution ${h.resolution}")
      publisher.publish(shardNum, h.resolution, records)
    }
    Future.sequence(responses).map(_.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success))
  }

}

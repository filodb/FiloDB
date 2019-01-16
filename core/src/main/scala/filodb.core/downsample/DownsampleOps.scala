package filodb.core.downsample

import scala.concurrent.{ExecutionContext, Future}

import com.typesafe.scalalogging.StrictLogging

import filodb.core.{ErrorResponse, Response, Success}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.memstore.TimeSeriesPartition
import filodb.core.metadata.{DataColumn, Dataset}
import filodb.core.metadata.Column.ColumnType
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkInfoIterator
import filodb.memory.MemFactory

final case class DownsamplingState(resolution: Int, downsampler: Seq[Seq[ChunkDownsampler]], builder: RecordBuilder)

object DownsampleOps extends StrictLogging {

  def initializeDownsamplerStates(dataset: Dataset,
                                  resolutions: Seq[Int],
                                  memFactory: MemFactory): Seq[DownsamplingState] = {
    val schema = downsampleIngestSchema(dataset)
    resolutions.map { res =>
      val downsamplers = ChunkDownsampler.makeDownsamplers(dataset)
      DownsamplingState(res, downsamplers, new RecordBuilder(memFactory, schema))
    }
  }

  /**
    * Formulates downsample schema using the downsampler configuration for dataset
    */
  def downsampleIngestSchema(dataset: Dataset): RecordSchema = {
    val downsampleCols = dataset.dataColumns.flatMap(c => c.asInstanceOf[DataColumn].downsamplerTypes.map {dt =>
      // The column names here would not exactly match the column names in the destination dataset, but it is ok.
      // The ordering and data types is what really matters.
      ColumnInfo(dt.downsampleColName(c.name), c.columnType)
    })
    new RecordSchema(downsampleCols ++ dataset.partKeySchema.columns,
      Some(downsampleCols.size), dataset.ingestionSchema.predefinedKeys)
  }

  /**
    * Populates the builders in the DownsamplingState with downsample data for the
    * chunkset passed in.
    */
  // scalastyle:off method.length
  def downsample(dataset: Dataset,
                 part: TimeSeriesPartition,
                 chunksets: ChunkInfoIterator,
                 dsStates: Seq[DownsamplingState]): Unit = {
    while (chunksets.hasNext) {
      val chunkset = chunksets.nextInfo
      val startTime = chunkset.startTime
      val endTime = chunkset.endTime
      // for each downsample resolution
      dsStates.foreach { case DownsamplingState(res, downsamplers, builder) =>
        var pStart = ( (startTime - 1) / res) * res + 1 // inclusive startTime for downsample period
      var pEnd = pStart + res // end is inclusive
        // for each downsample period
        while (pStart <= endTime) {
          var startRowNum = 0
          var endRowNum = 0
          builder.startNewRecord()
          // for each column
          dataset.dataColumns.foreach { col =>
            val vecPtr = chunkset.vectorPtr(col.id)
            val colReader = part.chunkReader(col.id, vecPtr)
            col.columnType match {
              case ColumnType.TimestampColumn =>
                // timestamp column is the row key, so fix the row numbers for the downsample period
                startRowNum = colReader.asLongReader.binarySearch(vecPtr, pStart) & 0x7fffffff
                endRowNum = colReader.asLongReader.ceilingIndex(vecPtr, pEnd)
                // for each downsampler for the long column
                downsamplers(col.id).foreach { d =>
                  val downsampled = d.downsampleChunk(vecPtr, colReader.asLongReader, startRowNum, endRowNum)
                  builder.addLong(downsampled)
                }
              case ColumnType.DoubleColumn =>
                // for each downsampler for the double column
                downsamplers(col.id).foreach { d =>
                  val downsampled = d.downsampleChunk(vecPtr, colReader.asDoubleReader, startRowNum, endRowNum)
                  builder.addDouble(downsampled)
                }
              case ColumnType.LongColumn =>
                // for each downsampler for the long column
                downsamplers(col.id).foreach { d =>
                  val downsampled = d.downsampleChunk(vecPtr, colReader.asLongReader, startRowNum, endRowNum)
                  builder.addLong(downsampled)
                }
              case _ => ???
            }
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
  def publishDownsampleBuilders(publisher: DownsamplePublisher,
                                shardNum: Int,
                                states: Seq[DownsamplingState])(implicit sched: ExecutionContext): Future[Response] = {
    val responses = states.map { h =>
      val records = h.builder.optimalContainerBytes(true)
      logger.debug(s"Publishing ${records.size} downsample record containers " +
        s"to shard $shardNum for resolution ${h.resolution}")
      publisher.publish(shardNum, h.resolution, records)
    }
    Future.sequence(responses).map(_.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success))
  }

}

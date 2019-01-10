package filodb.core.downsample

import scala.concurrent.{ExecutionContext, Future}

import filodb.core.{ErrorResponse, Response, Success}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.memstore.TimeSeriesPartition
import filodb.core.metadata.{DataColumn, Dataset}
import filodb.core.metadata.Column.{ColumnType, DownsampleType}
import filodb.core.metadata.Column.DownsampleType._
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkInfoIterator
import filodb.memory.format.vectors.{DoubleVectorDataReader, LongVectorDataReader}

trait ChunkDownsampler {
  def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
  def downsampleChunk(reader: LongVectorDataReader, startRow: Int, endRow: Int): Long = ???
}

object MinDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

object MaxDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

object SumDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

object CountDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: DoubleVectorDataReader, startRow: Int, endRow: Int): Double = ???
}

/**
  * Emits the last value within the row range requested. Typically used for timestamp column.
  */
object LastValueDownsampler extends ChunkDownsampler {
  override def downsampleChunk(reader: LongVectorDataReader, startRow: Int, endRow: Int): Long = ???
}

object ChunkDownsampler {

  /**
    * Factory method for Downsampler from its type
    * @param typ
    * @return
    */
  def apply(typ: DownsampleType): ChunkDownsampler = {
    typ match {
      case MinDownsample       => MinDownsampler
      case MaxDownsample       => MaxDownsampler
      case SumDownsample       => SumDownsampler
      case CountDownsample     => CountDownsampler
      case LastValueDownsample => LastValueDownsampler
      case a => throw new UnsupportedOperationException(s"Invalid downsample type $a")
    }
  }

  /**
    * Formulates downsample schema using the downsampler configuration for dataset
    */
  def downsampleIngestSchema(dataset: Dataset): RecordSchema = {
    val downsampleCols = dataset.dataColumns.flatMap(c => c.asInstanceOf[DataColumn].downsamplerTypes.map {dt =>
      ColumnInfo(dt.downsampleColName(c.name), c.columnType)
    })
    new RecordSchema(dataset.partKeySchema.columns ++ downsampleCols, Some(0), dataset.ingestionSchema.predefinedKeys)
  }

  /**
    * Returns 2D collection. First dimension is columnId, second is downsampler-number.
    */
  def makeDownsamplers(dataset: Dataset): Seq[Seq[ChunkDownsampler]] = {
    val downsamplers = dataset.dataColumns.map { dc =>
      dc.columnType match {
        case ColumnType.DoubleColumn =>
          dc.asInstanceOf[DataColumn].downsamplerTypes.map(ChunkDownsampler(_))
        case _ => ??? // TODO not supported yet
      }
    }
    downsamplers
  }

  case class DownsamplingState(resolution: Int, downsampler: Seq[Seq[ChunkDownsampler]], builder: RecordBuilder)

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
      dsStates.foreach { case DownsamplingState(res, downsamplers, builder) =>
        var pStart = (startTime / res) * res
        var pEnd = pStart + res
        // for each downsample period
        while (pStart <= endTime) {
          val startRowNum = 0  // TODO calculate with binary search for pStart
          val endRowNum = 0 // TODO calculate with binary search for pEnd
          builder.startNewRecord()
          // add partKey
          builder.addFieldsFromBinRecord(part.partKeyBase, part.partKeyOffset, dataset.partKeySchema)
          // for each column
          dataset.dataColumns.foreach { col =>
            val vecPtr = chunkset.vectorPtr(col.id)
            val colReader = part.chunkReader(1, vecPtr)
            col.columnType match {
              case ColumnType.DoubleColumn =>
                // for each downsampler for the column
                downsamplers(col.id).foreach { d =>
                  val downsampled = d.downsampleChunk(colReader.asDoubleReader, startRowNum, endRowNum)
                  builder.addDouble(downsampled)
                }
              case ColumnType.TimestampColumn | ColumnType.LongColumn =>
                downsamplers(col.id).foreach { d =>
                  val downsampled = d.downsampleChunk(colReader.asLongReader, startRowNum, endRowNum)
                  builder.addLong(downsampled)
                }
              case _ => ???
            }
          }
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
      publisher.publish(shardNum, h.resolution, records)
    }
    Future.sequence(responses).map(_.find(_.isInstanceOf[ErrorResponse]).getOrElse(Success))
  }
}

object DownsamplePublisher {
  def apply(): DownsamplePublisher = ???
}

trait DownsamplePublisher {
  def publish(shardNum: Int, resolution: Int, records: Seq[Array[Byte]]): Future[Response]
}

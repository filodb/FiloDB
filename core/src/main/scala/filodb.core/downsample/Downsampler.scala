package filodb.core.downsample

import java.lang.{Double => JLDouble}

import scala.concurrent.{ExecutionContext, Future}

import filodb.core.{ErrorResponse, Response, Success}
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.memstore.TimeSeriesPartition
import filodb.core.metadata.{DataColumn, Dataset}
import filodb.core.metadata.Column.{ColumnType, DownsampleType}
import filodb.core.metadata.Column.DownsampleType._
import filodb.core.query.ColumnInfo
import filodb.core.store.ChunkInfoIterator
import filodb.memory.MemFactory
import filodb.memory.format.BinaryVector
import filodb.memory.format.vectors.{DoubleVectorDataReader, LongVectorDataReader}

trait ChunkDownsampler {
  def downsampleChunk(doubleVect: BinaryVector.BinaryVectorPtr,
                      reader: DoubleVectorDataReader,
                      startRow: Int,
                      endRow: Int): Double

  def downsampleChunk(longVect: BinaryVector.BinaryVectorPtr,
                      longReader: LongVectorDataReader,
                      startRow: Int,
                      endRow: Int): Long
}

object MinDownsampler extends ChunkDownsampler {
  override def downsampleChunk(doubleVect: BinaryVector.BinaryVectorPtr,
                               doubleReader: DoubleVectorDataReader,
                               startRow: Int,
                               endRow: Int): Double = {
    var min = Double.MaxValue
    var rowNum = startRow
    val it = doubleReader.iterate(doubleVect, startRow)
    while (rowNum <= endRow) {
      val nextVal = it.next
      if (!JLDouble.isNaN(nextVal)) min = Math.min(min, nextVal)
      rowNum += 1
    }
    min
  }
  override def downsampleChunk(longVect: BinaryVector.BinaryVectorPtr,
                               longReader: LongVectorDataReader,
                               startRow: Int,
                               endRow: Int): Long = {
    var min = Long.MaxValue
    var rowNum = startRow
    val it = longReader.iterate(longVect, startRow)
    while (rowNum <= endRow) {
      val nextVal = it.next
      min = Math.min(min, nextVal)
      rowNum += 1
    }
    min
  }
}

object MaxDownsampler extends ChunkDownsampler {
  override def downsampleChunk(doubleVect: BinaryVector.BinaryVectorPtr,
                               doubleReader: DoubleVectorDataReader,
                               startRow: Int,
                               endRow: Int): Double = {
    var max = Double.MinValue
    var rowNum = startRow
    val it = doubleReader.iterate(doubleVect, startRow)
    while (rowNum <= endRow) {
      val nextVal = it.next
      if (!JLDouble.isNaN(nextVal)) max = Math.max(max, nextVal)
      rowNum += 1
    }
    max
  }
  override def downsampleChunk(longVect: BinaryVector.BinaryVectorPtr,
                               longReader: LongVectorDataReader,
                               startRow: Int,
                               endRow: Int): Long = {
    var max = Long.MinValue
    var rowNum = startRow
    val it = longReader.iterate(longVect, startRow)
    while (rowNum <= endRow) {
      val nextVal = it.next
      max = Math.max(max, nextVal)
      rowNum += 1
    }
    max
  }
}

object SumDownsampler extends ChunkDownsampler {
  override def downsampleChunk(doubleVect: BinaryVector.BinaryVectorPtr,
                               doubleReader: DoubleVectorDataReader,
                               startRow: Int,
                               endRow: Int): Double = {
    doubleReader.sum(doubleVect, startRow, endRow)
  }

  override def downsampleChunk(longVect: BinaryVector.BinaryVectorPtr,
                      longReader: LongVectorDataReader,
                      startRow: Int,
                      endRow: Int): Long = {
    longReader.sum(longVect, startRow, endRow).toLong // FIXME why is sum call returning Double ?
  }
}

object CountDownsampler extends ChunkDownsampler {
  override def downsampleChunk(doubleVect: BinaryVector.BinaryVectorPtr,
                               doubleReader: DoubleVectorDataReader,
                               startRow: Int,
                               endRow: Int): Double = {
    doubleReader.count(doubleVect, startRow, endRow)
  }

  override def downsampleChunk(longVect: BinaryVector.BinaryVectorPtr,
                               reader: LongVectorDataReader,
                               startRow: Int,
                               endRow: Int): Long = {
    endRow - startRow + 1
  }

}

object AverageDownsampler extends ChunkDownsampler {
  override def downsampleChunk(doubleVect: BinaryVector.BinaryVectorPtr,
                               doubleReader: DoubleVectorDataReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val sum = doubleReader.sum(doubleVect, startRow, endRow)
    val count = doubleReader.count(doubleVect, startRow, endRow)
    // TODO We should use a special representation of NaN here instead of 0.
    // NaN is used for End-Of-Time-Series-Marker currently
    if (count == 0) 0 else sum / count
  }

  override def downsampleChunk(longVect: BinaryVector.BinaryVectorPtr,
                               reader: LongVectorDataReader,
                               startRow: Int,
                               endRow: Int): Long = {
    throw new UnsupportedOperationException("AverageDownsampler on a Long column " +
      "is not supported since Long result cannot represent correct average. " +
      "Fix downsampling configuration on dataset")
  }
}

/**
  * Emits the last value within the row range requested. Typically used for timestamp column.
  */
object TimeDownsampler extends ChunkDownsampler {
  override def downsampleChunk(longVect: BinaryVector.BinaryVectorPtr,
                               reader: LongVectorDataReader,
                               startRow: Int,
                               endRow: Int): Long = {
    reader.apply(longVect, endRow)
  }

  override def downsampleChunk(doubleVect: BinaryVector.BinaryVectorPtr,
                               doubleReader: DoubleVectorDataReader,
                               startRow: Int,
                               endRow: Int): Double = {
    throw new UnsupportedOperationException("TimeDownsampler should not be configured on double column")
  }
}

object ChunkDownsampler {

  /**
    * Factory method for Downsampler from its type
    * @param typ
    * @return
    */
  def apply(typ: DownsampleType): ChunkDownsampler = {
    typ match {
      case AverageDownsample   => AverageDownsampler
      case MinDownsample       => MinDownsampler
      case MaxDownsample       => MaxDownsampler
      case SumDownsample       => SumDownsampler
      case CountDownsample     => CountDownsampler
      case TimeDownsample      => TimeDownsampler
      case a => throw new UnsupportedOperationException(s"Invalid downsample type $a")
    }
  }

  def initializeDownsamplerStates(dataset: Dataset,
                                  resolutions: Seq[Int],
                                  memFactory: MemFactory): Seq[DownsamplingState] = {
    val downsampleIngestSchema = ChunkDownsampler.downsampleIngestSchema(dataset)
    resolutions.map { res =>
      val downsamplers = ChunkDownsampler.makeDownsamplers(dataset)
      DownsamplingState(res, downsamplers, new RecordBuilder(memFactory, downsampleIngestSchema))
    }
  }

  /**
    * Formulates downsample schema using the downsampler configuration for dataset
    */
  def downsampleIngestSchema(dataset: Dataset): RecordSchema = {
    val downsampleCols = dataset.dataColumns.flatMap(c => c.asInstanceOf[DataColumn].downsamplerTypes.map {dt =>
      // TODO should the names exactly match the column names in the destination dataset?
      ColumnInfo(dt.downsampleColName(c.name), c.columnType)
    })
    new RecordSchema(/* TODO dataset.partKeySchema.columns ++ */
      downsampleCols, Some(0), dataset.ingestionSchema.predefinedKeys)
  }

  /**
    * Returns 2D collection. First dimension is columnId, second is downsampler-number.
    */
  def makeDownsamplers(dataset: Dataset): Seq[Seq[ChunkDownsampler]] = {
    dataset.dataColumns.map { dc =>
      dc.asInstanceOf[DataColumn].downsamplerTypes.map(ChunkDownsampler(_))
    }
  }

  case class DownsamplingState(resolution: Int, downsampler: Seq[Seq[ChunkDownsampler]], builder: RecordBuilder)

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
          // add partKey
// TODO         builder.addFieldsFromBinRecord(part.partKeyBase, part.partKeyOffset, dataset.partKeySchema)
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

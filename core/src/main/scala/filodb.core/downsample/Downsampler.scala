package filodb.core.downsample

import java.lang.{Double => JLDouble}

import filodb.core.metadata.{DataColumn, Dataset}
import filodb.core.metadata.Column.DownsampleType
import filodb.core.metadata.Column.DownsampleType._
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

  /**
    * Returns 2D collection of downsamplers for dataset. First dimension is column, second is downsampler for column.
    */
  def makeDownsamplers(dataset: Dataset): Seq[Seq[ChunkDownsampler]] = {
    dataset.dataColumns.map { dc =>
      dc.asInstanceOf[DataColumn].downsamplerTypes.map(ChunkDownsampler(_))
    }
  }

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

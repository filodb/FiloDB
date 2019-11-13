package filodb.core.downsample

import java.lang.{Double => JLDouble}

import enumeratum.{Enum, EnumEntry}

import filodb.core.metadata.Column.ColumnType
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.memory.format.{vectors => bv}
/**
  * Enum of supported downsampling function names
  * @param entryName name of the function
  * @param downsamplerClass its corresponding ChunkDownsampler class used for instance construction
  */
sealed abstract class DownsamplerName (override val entryName: String, val downsamplerClass: Class[_])
  extends EnumEntry

object DownsamplerName extends Enum[DownsamplerName] {
  val values = findValues
  case object MinD extends DownsamplerName("dMin", classOf[MinDownsampler])
  case object MaxD extends DownsamplerName("dMax", classOf[MaxDownsampler])
  case object SumD extends DownsamplerName("dSum", classOf[SumDownsampler])
  case object SumH extends DownsamplerName("hSum", classOf[HistSumDownsampler])
  case object CountD extends DownsamplerName("dCount", classOf[CountDownsampler])
  case object AvgD extends DownsamplerName("dAvg", classOf[AvgDownsampler])
  case object AvgAcD extends DownsamplerName("dAvgAc", classOf[AvgAcDownsampler])
  case object AvgScD extends DownsamplerName("dAvgSc", classOf[AvgScDownsampler])
  case object TimeT extends DownsamplerName("tTime", classOf[TimeDownsampler])
}

/**
  * Common trait for implementations of a chunk downsampler
  */
trait ChunkDownsampler {
  /**
    * Ids of Data Columns the downsampler works on.
    * The column id values are fed in via downsampling configuration of the dataset
    */
  def colIds: Seq[Int]

  /**
    * Downsampler name
    */
  def name: DownsamplerName

  /**
    * Type of the downsampled value emitted by the downsampler.
    */
  def colType: ColumnType

  /**
    * String representation of the downsampler for human readability and string encoding.
    */
  def encoded: String = s"${name.entryName}(${colIds.mkString("@")})"
}

/**
  * Chunk downsampler that emits Double values
  */
trait DoubleChunkDownsampler extends ChunkDownsampler {
  override val colType: ColumnType = ColumnType.DoubleColumn

  /**
    * Downsamples Chunk using column Ids configured and emit double value
    * @param part Time series partition to extract data from
    * @param chunkset The chunksetInfo that needs to be downsampled
    * @param startRow The start row number for the downsample period (inclusive)
    * @param endRow The end row number for the downsample period (inclusive)
    * @return downsampled value to emit
    */
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): Double
}

/**
  * Chunk downsampler trait for downsampling timestamp columns; emits long timestamps
  */
trait TimeChunkDownsampler extends ChunkDownsampler {
  override val colType: ColumnType = ColumnType.TimestampColumn

  /**
    * Downsamples Chunk using timestamp column Ids configured and emit long value
    * @param part Time series partition to extract data from
    * @param chunkset The chunksetInfo that needs to be downsampled
    * @param startRow The start row number for the downsample period (inclusive)
    * @param endRow The end row number for the downsample period (inclusive)
    * @return downsampled value to emit
    */
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): Long
}

/**
 * Chunk downsampler trait for downsampling histogram columns -> histogram columns
 */
trait HistChunkDownsampler extends ChunkDownsampler {
  override val colType: ColumnType = ColumnType.HistogramColumn

  /**
    * Downsamples Chunk using histogram column Ids configured and emit histogram value
    * @param part Time series partition to extract data from
    * @param chunkset The chunksetInfo that needs to be downsampled
    * @param startRow The start row number for the downsample period (inclusive)
    * @param endRow The end row number for the downsample period (inclusive)
    * @return downsampled value to emit
    */
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): bv.Histogram
}

/**
  * Downsamples by calculating sum of values in one column
  */
case class SumDownsampler(override val colIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Sum downsample requires only one column. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.SumD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecAcc = chunkset.vectorAccessor(colIds(0))
    val vecPtr = chunkset.vectorAddress(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecAcc, vecPtr)
    colReader.asDoubleReader.sum(vecAcc, vecPtr, startRow, endRow)
  }
}

case class HistSumDownsampler(val colIds: Seq[Int]) extends HistChunkDownsampler {
  require(colIds.length == 1, s"Sum downsample requires only one column. Got ${colIds.length}")
  val name = DownsamplerName.SumH
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): bv.Histogram = {
    val vecAcc = chunkset.vectorAccessor(colIds(0))
    val vecPtr = chunkset.vectorAddress(colIds(0))
    val histReader = part.chunkReader(colIds(0), vecAcc, vecPtr).asHistReader
    histReader.sum(startRow, endRow)
  }
}

/**
  * Downsamples by calculating count of values in one column
  */
case class CountDownsampler(override val colIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Count downsample requires only one column. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.CountD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecAcc = chunkset.vectorAccessor(colIds(0))
    val vecPtr = chunkset.vectorAddress(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecAcc, vecPtr)
    colReader.asDoubleReader.count(vecAcc, vecPtr, startRow, endRow)
  }
}

/**
  * Downsamples by calculating min of values in one column
  */
case class MinDownsampler(override val colIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Min downsample requires only one column. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.MinD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    // TODO MinOverTimeChunkedFunctionD has same code.  There is scope for refactoring logic into the vector class.
    val vecAcc = chunkset.vectorAccessor(colIds(0))
    val vecPtr = chunkset.vectorAddress(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecAcc, vecPtr)
    var min = Double.MaxValue
    var rowNum = startRow
    val it = colReader.iterate(vecAcc, vecPtr, startRow).asDoubleIt
    while (rowNum <= endRow) {
      val nextVal = it.next
      if (!JLDouble.isNaN(nextVal)) min = Math.min(min, nextVal)
      rowNum += 1
    }
    min
  }
}

/**
  * Downsamples by calculating max of values in one column
  */
case class MaxDownsampler(override val colIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Max downsample requires only one column. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.MaxD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    // TODO MaxOverTimeChunkedFunctionD has same code.  There is scope for refactoring logic into the vector class.
    val vecAcc = chunkset.vectorAccessor(colIds(0))
    val vecPtr = chunkset.vectorAddress(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecAcc, vecPtr)
    var max = Double.MinValue
    var rowNum = startRow
    val it = colReader.iterate(vecAcc, vecPtr, startRow).asDoubleIt
    while (rowNum <= endRow) {
      val nextVal = it.next
      if (!JLDouble.isNaN(nextVal)) max = Math.max(max, nextVal)
      rowNum += 1
    }
    max
  }
}

/**
  * Downsamples by calculating average from average and count columns
  */
case class AvgAcDownsampler(override val colIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 2, s"AvgAc downsample requires column ids of avg and count. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.AvgAcD
  val avgCol = colIds(0)
  val countCol = colIds(1)
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val avgVecAcc = chunkset.vectorAccessor(avgCol)
    val avgVecPtr = chunkset.vectorAddress(avgCol)
    val avgColReader = part.chunkReader(avgCol, avgVecAcc, avgVecPtr)
    val cntVecAcc = chunkset.vectorAccessor(countCol)
    val cntVecPtr = chunkset.vectorAddress(countCol)
    val cntColReader = part.chunkReader(countCol, cntVecAcc, cntVecPtr)
    var rowNum = startRow
    val avgIt = avgColReader.iterate(avgVecAcc, avgVecPtr, startRow).asDoubleIt
    val cntIt = cntColReader.iterate(cntVecAcc, cntVecPtr, startRow).asDoubleIt
    var avg = 0d
    var cnt = 0d
    while (rowNum <= endRow) {
      val nextAvg = avgIt.next
      val nextCnt = cntIt.next
      avg = (avg * cnt + nextAvg * nextCnt) / (nextCnt + cnt)
      cnt = cnt + nextCnt
      rowNum += 1
    }
    avg
  }
}

/**
  * Downsamples by calculating average from sum and count columns
  */
case class AvgScDownsampler(override val colIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 2, s"AvgSc downsample requires column ids of sum and count. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.AvgScD
  val sumCol = colIds(0)
  val countCol = colIds(1)
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val sumVecAcc = chunkset.vectorAccessor(sumCol)
    val sumVecPtr = chunkset.vectorAddress(sumCol)
    val sumColReader = part.chunkReader(sumCol, sumVecAcc, sumVecPtr)
    val cntVecAcc = chunkset.vectorAccessor(countCol)
    val cntVecPtr = chunkset.vectorAddress(countCol)
    val cntColReader = part.chunkReader(countCol, cntVecAcc, cntVecPtr)
    val sumSum = sumColReader.asDoubleReader.sum(sumVecAcc, sumVecPtr, startRow, endRow)
    val sumCount = cntColReader.asDoubleReader.sum(cntVecAcc, cntVecPtr, startRow, endRow)
    sumSum / sumCount
  }

}

/**
  * Downsamples by calculating average of values from one column
  */
case class AvgDownsampler(override val colIds: Seq[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Avg downsample requires one column id with data to average. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.AvgD
  override def downsampleChunk(part: ReadablePartition,
                               chunkset: ChunkSetInfoReader,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecPtr = chunkset.vectorAddress(colIds(0))
    val vecAcc = chunkset.vectorAccessor(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecAcc, vecPtr)
    val sum = colReader.asDoubleReader.sum(vecAcc, vecPtr, startRow, endRow)
    val count = colReader.asDoubleReader.count(vecAcc, vecPtr, startRow, endRow)
    sum / count
  }

}

/**
  * Downsamples by selecting the last timestamp in the downsample period.
  */
case class TimeDownsampler(override val colIds: Seq[Int]) extends TimeChunkDownsampler {
  require(colIds.length == 1, s"Time downsample requires only one column. Got ${colIds.length}")
  override val name: DownsamplerName = DownsamplerName.TimeT
  def downsampleChunk(part: ReadablePartition,
                      chunkset: ChunkSetInfoReader,
                      startRow: Int,
                      endRow: Int): Long = {
    val vecPtr = chunkset.vectorAddress(colIds(0))
    val vecAcc = chunkset.vectorAccessor(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecAcc, vecPtr).asLongReader
    colReader.apply(vecAcc, vecPtr, endRow)
  }

}

object ChunkDownsampler {

  /**
    * Parses single downsampler from string notation such as
    * "dAvgAc(4@1)" where "dAvgAc" is the downsampler name, 4 & 1 are the column IDs to be used by the function
    */
  def downsampler(strNotation: String): ChunkDownsampler = {
    val parts = strNotation.split("[(@)]")
    // TODO possibly better validation of string notation
    require(parts.size >= 2, s"Downsampler '$strNotation' does not have downsampler name and column id. ")
    val name = parts(0)
    val colIds = parts.drop(1).map(_.toInt)
    DownsamplerName.withNameOption(name) match {
      case None    => throw new IllegalArgumentException(s"Unsupported downsampling function $name")
      case Some(d) => d.downsamplerClass.getConstructor(classOf[Seq[Int]])
                            .newInstance(colIds.toSeq).asInstanceOf[ChunkDownsampler]
    }
  }

  def downsamplers(str: Seq[String]): Seq[ChunkDownsampler] = str.map(downsampler(_))
}
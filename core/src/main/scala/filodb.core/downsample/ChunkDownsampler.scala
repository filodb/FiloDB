package filodb.core.downsample

import java.lang.{Double => JLDouble}

import filodb.core.memstore.TimeSeriesPartition
import filodb.core.metadata.Column.ColumnType
import filodb.core.store.ChunkSetInfo

/**
  * Common trait for implementations of a chunk downsampler
  */
trait ChunkDownsampler {
  /**
    * Ids of Data Columns the downsampler works on.
    * Fed in via downsampling configuration
    */
  def colIds: Array[Int]

  /**
    * Downsampler name
    */
  def name: String

  /**
    * Type of the downsampled value emitted by the downsampler.
    */
  def colType: ColumnType

  /**
    * String representation of the downsampler for human readability and encoding.
    */
  override def toString: String = s"${name}(${colIds.mkString("@")})"
}

/**
  * Chunk Downsampler that emits Double values
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
  def downsampleChunk(part: TimeSeriesPartition,
                      chunkset: ChunkSetInfo,
                      startRow: Int,
                      endRow: Int): Double
}

trait TimestampChunkDownsampler extends ChunkDownsampler {
  override val colType: ColumnType = ColumnType.TimestampColumn

  /**
    * Downsamples Chunk using timestamp column Ids configured and emit long value
    * @param part Time series partition to extract data from
    * @param chunkset The chunksetInfo that needs to be downsampled
    * @param startRow The start row number for the downsample period (inclusive)
    * @param endRow The end row number for the downsample period (inclusive)
    * @return downsampled value to emit
    */
  def downsampleChunk(part: TimeSeriesPartition,
                      chunkset: ChunkSetInfo,
                      startRow: Int,
                      endRow: Int): Long
}

/**
  * Downsamples by calculating sum of values in one column
  */
case class SumDownsampler(override val colIds: Array[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Sum downsample requires only one column. Got ${colIds.length}")
  override val name: String = "dSum"
  override def downsampleChunk(part: TimeSeriesPartition,
                               chunkset: ChunkSetInfo,
                               startRow: Int,
                               endRow: Int): Double = {

    val vecPtr = chunkset.vectorPtr(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecPtr)
    colReader.asDoubleReader.sum(vecPtr, startRow, endRow)
  }
}

/**
  * Downsamples by calculating count of values in one column
  */
case class CountDownsampler(override val colIds: Array[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Count downsample requires only one column. Got ${colIds.length}")
  override val name: String = "dCount"
  override def downsampleChunk(part: TimeSeriesPartition,
                               chunkset: ChunkSetInfo,
                               startRow: Int,
                               endRow: Int): Double = {

    val vecPtr = chunkset.vectorPtr(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecPtr)
    colReader.asDoubleReader.count(vecPtr, startRow, endRow)
  }
}

/**
  * Downsamples by calculating min of values in one column
  */
case class MinDownsampler(override val colIds: Array[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Min downsample requires only one column. Got ${colIds.length}")
  override val name: String = "dMin"
  override def downsampleChunk(part: TimeSeriesPartition,
                               chunkset: ChunkSetInfo,
                               startRow: Int,
                               endRow: Int): Double = {

    val vecPtr = chunkset.vectorPtr(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecPtr)

    var min = Double.MaxValue
    var rowNum = startRow
    val it = colReader.iterate(vecPtr, startRow).asDoubleIt
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
case class MaxDownsampler(override val colIds: Array[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Max downsample requires only one column. Got ${colIds.length}")
  override val name: String = "dMax"
  override def downsampleChunk(part: TimeSeriesPartition,
                               chunkset: ChunkSetInfo,
                               startRow: Int,
                               endRow: Int): Double = {

    val vecPtr = chunkset.vectorPtr(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecPtr)

    var max = Double.MinValue
    var rowNum = startRow
    val it = colReader.iterate(vecPtr, startRow).asDoubleIt
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
case class AvgAcDownsampler(override val colIds: Array[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 2, s"AvgAc downsample requires column ids of avg and count. Got ${colIds.length}")
  override val name: String = "dAvgAc"
  val avgCol = colIds(0)
  val countCol = colIds(1)
  override def downsampleChunk(part: TimeSeriesPartition,
                               chunkset: ChunkSetInfo,
                               startRow: Int,
                               endRow: Int): Double = {
    val avgVecPtr = chunkset.vectorPtr(avgCol)
    val avgColReader = part.chunkReader(avgCol, avgVecPtr)
    val cntVecPtr = chunkset.vectorPtr(countCol)
    val cntColReader = part.chunkReader(countCol, cntVecPtr)
    var rowNum = startRow
    val avgIt = avgColReader.iterate(avgVecPtr, startRow).asDoubleIt
    val cntIt = cntColReader.iterate(cntVecPtr, startRow).asDoubleIt
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
case class AvgScDownsampler(override val colIds: Array[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 2, s"AvgSc downsample requires column ids of sum and count. Got ${colIds.length}")
  override val name: String = "dAvgSc"
  val sumCol = colIds(0)
  val countCol = colIds(1)
  override def downsampleChunk(part: TimeSeriesPartition,
                               chunkset: ChunkSetInfo,
                               startRow: Int,
                               endRow: Int): Double = {
    val sumVecPtr = chunkset.vectorPtr(sumCol)
    val sumColReader = part.chunkReader(sumCol, sumVecPtr)
    val cntVecPtr = chunkset.vectorPtr(countCol)
    val cntColReader = part.chunkReader(countCol, cntVecPtr)
    val sumSum = sumColReader.asDoubleReader.sum(sumVecPtr, startRow, endRow)
    val sumCount = cntColReader.asDoubleReader.sum(cntVecPtr, startRow, endRow)
    sumSum / sumCount
  }
}

/**
  * Downsamples by calculating average of values from one column
  */
case class AvgDownsampler(override val colIds: Array[Int]) extends DoubleChunkDownsampler {
  require(colIds.length == 1, s"Avg downsample requires one column id with data to average. Got ${colIds.length}")
  override val name: String = "dAvg"
  override def downsampleChunk(part: TimeSeriesPartition,
                               chunkset: ChunkSetInfo,
                               startRow: Int,
                               endRow: Int): Double = {
    val vecPtr = chunkset.vectorPtr(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecPtr)
    val sum = colReader.asDoubleReader.sum(vecPtr, startRow, endRow)
    val count = colReader.asDoubleReader.count(vecPtr, startRow, endRow)
    sum / count
  }
}

/**
  * Downsamples by selecting the last timestamp in the downsample period.
  */
case class TimeDownsampler(override val colIds: Array[Int]) extends TimestampChunkDownsampler {
  require(colIds.length == 1, s"Time downsample requires only one column. Got ${colIds.length}")
  override val name: String = "tTime"
  def downsampleChunk(part: TimeSeriesPartition,
                      chunkset: ChunkSetInfo,
                      startRow: Int,
                      endRow: Int): Long = {
    val vecPtr = chunkset.vectorPtr(colIds(0))
    val colReader = part.chunkReader(colIds(0), vecPtr).asLongReader
    colReader.apply(vecPtr, endRow)
  }
}

object ChunkDownsampler {

  /**
    * Parses single downsampler from string notation such as
    * "name(4@1)" where "name" is the downsampler name, 4 & 1 are the column IDs to be used by the function
    */
  def downsampler(strNotation: String): ChunkDownsampler = {
    val parts = strNotation.split("[(@)]")
    // TODO possibly better validation of string notation
    require(parts.size >= 2, s"Downsampler '$strNotation' does not have downsampler name and column id. ")
    val name = parts(0)
    val colIds = parts.drop(1).map(_.toInt)
    name match {
      case "dMin"   => MinDownsampler(colIds)
      case "dMax"   => MaxDownsampler(colIds)
      case "dSum"   => SumDownsampler(colIds)
      case "dCount" => CountDownsampler(colIds)
      case "dAvgAc"  => AvgAcDownsampler(colIds)
      case "dAvgSc"  => AvgScDownsampler(colIds)
      case "dAvg"  => AvgDownsampler(colIds)
      case "tTime"   => TimeDownsampler(colIds)
      case unknown => throw new IllegalArgumentException(s"Unsupported downsampling function $unknown")
    }
  }

  def downsamplers(str: Seq[String]): Seq[ChunkDownsampler] = str.map(downsampler(_))
}
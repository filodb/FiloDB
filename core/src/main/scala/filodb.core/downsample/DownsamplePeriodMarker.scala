package filodb.core.downsample

import debox.Buffer
import enumeratum.{Enum, EnumEntry}
import java.util
import scalaxy.loops._

import filodb.core.metadata.DataSchema
import filodb.core.store.{ChunkSetInfoReader, ReadablePartition}
import filodb.memory.format.PrimitiveVectorReader
import filodb.memory.format.vectors.{CorrectingDoubleVectorReader, DoubleVectorDataReader}

/**
  * Enum of supported downsample period marker names
  * @param entryName name of the marker
  * @param periodMarkerClass its corresponding marker class used for instance construction
  */
sealed abstract class PeriodMarkerName(override val entryName: String, val periodMarkerClass: Class[_])
  extends EnumEntry

object PeriodMarkerName extends Enum[PeriodMarkerName] {
  val values = findValues
  case object Time extends PeriodMarkerName("time", classOf[TimeDownsamplePeriodMarker])
  case object Counter extends PeriodMarkerName("counter", classOf[CounterDownsamplePeriodMarker])
}

/**
  * Implementations of the trait identify row numbers that
  * demark the boundaries of downsampling periods
  */
trait DownsamplePeriodMarker {

  def name: PeriodMarkerName

  /**
    * Id of Data Column the marker works on.
    * The column id value is fed in via downsampling configuration of the dataset
    */
  def inputColId: Int

  /**
    * Returns sorted collection of row numbers for the given chunkset that mark the
    * periods to downsample. startRow and endRow are inclusive
    */
  def periods(part: ReadablePartition,
              chunkset: ChunkSetInfoReader,
              resMillis: Long,
              startRow: Int,
              endRow: Int): Buffer[Int]
}

/**
  * Time based downsample period marker, which returns row numbers of the last sample for each downsample period
  * @param inputColId requires the timestamp column 0
  */
class TimeDownsamplePeriodMarker(val inputColId: Int) extends DownsamplePeriodMarker {
  require(inputColId == DataSchema.timestampColID)
  override def periods(part: ReadablePartition,
                       chunkset: ChunkSetInfoReader,
                       resMillis: Long,
                       startRow: Int,
                       endRow: Int): Buffer[Int] = {
    require(startRow <= endRow, s"startRow $startRow > endRow $endRow, " +
      s"chunkset: ${chunkset.debugString(part.schema)}")
    val tsAcc = chunkset.vectorAccessor(DataSchema.timestampColID)
    val tsPtr = chunkset.vectorAddress(DataSchema.timestampColID)
    val tsReader = part.chunkReader(DataSchema.timestampColID, tsAcc, tsPtr).asLongReader

    val startTime = tsReader.apply(tsAcc, tsPtr, startRow)
    val endTime = tsReader.apply(tsAcc, tsPtr, endRow)

    val result = debox.Buffer.empty[Int]
    // A sample exactly for 5pm downsampled 5-minutely should fall in the period 4:55:00:001pm to 5:00:00:000pm.
    // Hence subtract - 1 below from chunk startTime to find the first downsample period.
    // + 1 is needed since the startTime is inclusive. We don't want pStart to be 4:55:00:000;
    // instead we want 4:55:00:001
    var pStart = ((startTime - 1) / resMillis) * resMillis + 1
    var pEnd = pStart + resMillis // end is inclusive
    // for each downsample period
    while (pStart <= endTime) {
      // fix the boundary row numbers for the downsample period by looking up the timestamp column
      val endRowNum = Math.min(tsReader.ceilingIndex(tsAcc, tsPtr, pEnd), chunkset.numRows - 1)
      result += endRowNum
      pStart += resMillis
      pEnd += resMillis
    }
    result
  }
  override def name: PeriodMarkerName = PeriodMarkerName.Time
}

/**
  * Returns union of the following:
  * (a) the results from TimeDownsamplePeriodMarker.
  * (b) the first sample of chunk. This is needed to cover for drop detection across chunks
  * (c) row numbers when counter drops. This is needed to account for highest correction value before drop
  * (d) last row numbers before counter drops. This is needed for downsample queries to detect drop
  *
  * @param inputColId requires the counter column id
  */
class CounterDownsamplePeriodMarker(val inputColId: Int) extends DownsamplePeriodMarker {
  override def name: PeriodMarkerName = PeriodMarkerName.Counter
  override def periods(part: ReadablePartition,
                       chunkset: ChunkSetInfoReader,
                       resMillis: Long,
                       startRow: Int,
                       endRow: Int): Buffer[Int] = {
    require(startRow <= endRow, s"startRow $startRow > endRow $endRow, " +
      s"chunkset: ${chunkset.debugString(part.schema)}")
    val result = debox.Set.empty[Int]
    result += startRow // need to add start of every chunk
    result ++= DownsamplePeriodMarker.timeDownsamplePeriodMarker
      .periods(part, chunkset, resMillis, startRow + 1, endRow)
    val ctrVecAcc = chunkset.vectorAccessor(inputColId)
    val ctrVecPtr = chunkset.vectorAddress(inputColId)
    val ctrReader = part.chunkReader(inputColId, ctrVecAcc, ctrVecPtr)
    ctrReader match {
      case r: DoubleVectorDataReader =>
        if (PrimitiveVectorReader.dropped(ctrVecAcc, ctrVecPtr)) { // counter dip detected
          val drops = r.asInstanceOf[CorrectingDoubleVectorReader].dropPositions(ctrVecAcc, ctrVecPtr)
          for {i <- 0 until drops.length optimized} {
            if (drops(i) <= endRow) {
              result += drops(i) - 1
              result += drops(i)
            }
          }
        }
      case _ =>
        throw new IllegalStateException("Did not get a double column - cannot apply counter period marking strategy")
    }

    // Note: following alternative avoids copies, but results in spire library version conflicts with Spark. :(
//    import spire.std.int._
//    result.toSortedBuffer

    val res = result.toArray()
    util.Arrays.sort(res)
    debox.Buffer.fromArray(res)
  }
}

object DownsamplePeriodMarker {

  /**
    * Parses single downsampler from string notation such as
    * "counter(2)" where "counter" is the downsample period marker name, 2 is the column IDs to be used by the function
    */
  def downsamplePeriodMarker(strNotation: String): DownsamplePeriodMarker = {
    val parts = strNotation.split("[()]")
    // TODO possibly better validation of string notation
    require(parts.length == 2, s"DownsamplePeriodMarker '$strNotation' needs a name and a column id")
    val name = parts(0)
    val colId = parts(1).toInt
    PeriodMarkerName.withNameOption(name) match {
      case None    => throw new IllegalArgumentException(s"Unsupported downsample period marker $name")
      case Some(d) => d.periodMarkerClass.getConstructor(classOf[Int])
        .newInstance(Integer.valueOf(colId)).asInstanceOf[DownsamplePeriodMarker]
    }
  }

  val timeDownsamplePeriodMarker = new TimeDownsamplePeriodMarker(DataSchema.timestampColID)
}

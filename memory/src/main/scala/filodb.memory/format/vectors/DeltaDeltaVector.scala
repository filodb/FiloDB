package filodb.memory.format.vectors

import java.nio.ByteBuffer

import scalaxy.loops._

import filodb.memory.MemFactory
import filodb.memory.format._


/**
 * The Delta-Delta Vector represents an efficient encoding of a sloped line where in general values are
 * expected to stay close to such a line, defined with an initial offset and a slope or delta per element.
 * Examples of data that fits this well are timestamps and counters that increment regularly.
 * This can also be used to encode data with an offset even if the slope is zero.
 *
 * What is stored:
 *   base    Long/8 bytes  - the base or initial value
 *   slope   Int /4 bytes  - the delta or increment per value
 *
 * This is based upon the similar concept used in the Facebook Gorilla TSDB and Prometheus TSDB to compact
 * timestamp storage.  See:
 *   https://promcon.io/2016-berlin/talks/the-prometheus-time-series-database/
 *   http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 */
object DeltaDeltaVector {
  /**
   * Creates a non-growing DeltaDeltaAppendingVector based on an initial value and slope and nbits.
   * Really meant to be called from the optimize method of a LongAppendingVector, although you could
   * initialize a fresh one if you are relatively sure about the initial value and slope parameters.
   */
  def appendingVector(memFactory: MemFactory,
                      maxElements: Int,
                      initValue: Long,
                      slope: Int,
                      nbits: Short,
                      signed: Boolean): DeltaDeltaAppendingVector = {
    val bytesRequired = 12 + IntBinaryVector.noNAsize(maxElements, nbits)
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(bytesRequired)
    val dispose = () => memFactory.freeMemory(off)
    new DeltaDeltaAppendingVector(base, off, nBytes, initValue, slope, nbits, signed, dispose)
  }

  /**
   * Creates a DeltaDeltaAppendingVector from a source AppendableVector[Long], filling in all
   * the values as well.  Tries not to create intermediate vectors by figuring out size of deltas from the source.
   * Eligibility is pretty simple right now:
   * 1) Vectors with 2 or less elements are excluded. Just doesn't make sense.
   * 1a) Vectors with any NAs are not eligible
   * 2) If the deltas end up taking more than maxNBits.  Default is 32, but can be adjusted.
   *
   * NOTE: no need to get min max before calling this function.  We figure out min max of deltas, much more important
   * @return Some(vector) if the input vect is eligible, or None if it is not eligible
   */
  def fromLongVector(memFactory: MemFactory,
                     inputVect: BinaryAppendableVector[Long],
                     maxNBits: Short = 32): Option[BinaryVector[Long]] =
    if (inputVect.noNAs && inputVect.length > 2) {
      for { slope    <- getSlope(inputVect(0), inputVect(inputVect.length - 1), inputVect.length)
            minMax   <- getDeltasMinMax(inputVect, slope)
            nbitsSigned <- getNbitsSignedFromMinMax(minMax, maxNBits)
      } yield {
        if (minMax._1 == minMax._2) {
          const(memFactory, inputVect.length, inputVect(0), slope)
        } else {
          val vect = appendingVector(memFactory, inputVect.length, inputVect(0), slope, nbitsSigned._1, nbitsSigned._2)
          vect.addVector(inputVect)
          vect.freeze(None)
        }
      }
    } else { None }

  def apply(buffer: ByteBuffer): BinaryVector[Long] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    DeltaDeltaVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def const(buffer: ByteBuffer): BinaryVector[Long] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    DeltaDeltaConstVector(base, off, len, BinaryVector.NoOpDispose)
  }

  def const(memFactory: MemFactory, numElements: Int, initValue: Long, slope: Int): DeltaDeltaConstVector = {
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(16)
    val dispose = () => memFactory.freeMemory(off)
    UnsafeUtils.setLong(base, off, initValue)
    UnsafeUtils.setInt(base, off + 8, slope)
    UnsafeUtils.setInt(base, off + 12, numElements)
    new DeltaDeltaConstVector(base, off, nBytes, dispose)
  }

  /**
   * Returns the incremental slope from first to last over numElements.
   * If the slope is greater than Int.MaxValue, then None is returned.
   */
  def getSlope(first: Long, last: Long, numElements: Int): Option[Int] = {
    val slope = (last - first) / (numElements - 1)
    if (slope < Int.MaxValue && slope > Int.MinValue) Some(slope.toInt) else None
  }

  // Returns min and max of deltas computed from original input.  Just for sizing nbits for final DDV
  def getDeltasMinMax(inputVect: BinaryAppendableVector[Long], slope: Int): Option[(Int, Int)] = {
    var baseValue: Long = inputVect(0)
    var max = Int.MinValue
    var min = Int.MaxValue
    for { i <- 1 until inputVect.length optimized } {
      baseValue += slope
      val delta = inputVect(i) - baseValue
      if (delta > Int.MaxValue || delta < Int.MinValue) return None   // will not fit in 32 bits, just quit
      max = Math.max(max, delta.toInt)
      min = Math.min(min, delta.toInt)
    }
    Some((min, max))
  }

  def getNbitsSignedFromMinMax(minMax: (Int, Int), maxNBits: Short): Option[(Short, Boolean)] = {
    val (newNbits, newSigned) = IntBinaryVector.minMaxToNbitsSigned(minMax._1, minMax._2)
    if (newNbits <= maxNBits) Some((newNbits, newSigned)) else None
  }
}

final case class DeltaDeltaVector(base: Any, offset: Long,
                                  numBytes: Int,
                                  val dispose: () => Unit) extends BinaryVector[Long] {
  val vectMajorType = WireFormat.VECTORTYPE_DELTA2
  val vectSubType = WireFormat.SUBTYPE_INT_NOMASK
  final def maybeNAs: Boolean = false
  private final val initValue = UnsafeUtils.getLong(base, offset)
  private final val slope     = UnsafeUtils.getInt(base, offset + 8)
  private final val inner     = IntBinaryVector(base, offset + 12, numBytes - 12, dispose)

  override def length: Int = inner.length
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = initValue + slope * index + inner(index)

}

/**
 * A special case of the DeltaDelta where all the values are exactly on the sloped line.
 * (ie all the deltas are const=0)
 * This can also approximately represent (at great savings) timestamps close to the slope if we agree we are OK
 * losing the exact values when they are really close anyways.
 */
final case class DeltaDeltaConstVector(base: Any, offset: Long, numBytes: Int,
                                       val dispose: () => Unit) extends BinaryVector[Long] {
  val vectMajorType = WireFormat.VECTORTYPE_DELTA2
  val vectSubType = WireFormat.SUBTYPE_REPEATED
  final def maybeNAs: Boolean = false
  private final val initValue = UnsafeUtils.getLong(base, offset)
  private final val slope     = UnsafeUtils.getInt(base, offset + 8)
  override final def length: Int = UnsafeUtils.getInt(base, offset + 12)
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = initValue + slope * index
}

// TODO: validate args, esp base offset etc, somehow.  Need to think about this for the many diff classes.
class DeltaDeltaAppendingVector(val base: Any,
                                val offset: Long,
                                val maxBytes: Int,
                                initValue: Long,
                                slope: Int,
                                nbits: Short,
                                signed: Boolean,
                                val dispose: () => Unit) extends BinaryAppendableVector[Long] {
  val isAllNA = false
  val noNAs = true
  val maybeNAs = false
  val vectMajorType = WireFormat.VECTORTYPE_DELTA2
  val vectSubType = WireFormat.SUBTYPE_INT_NOMASK

  private val deltas = IntBinaryVector.appendingVectorNoNA(base, offset + 12, maxBytes - 12, nbits, signed, dispose)
  private var expected = initValue

  UnsafeUtils.setLong(base, offset, initValue)
  UnsafeUtils.setInt(base, offset + 8, slope)

  override def length: Int = deltas.length
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = initValue + slope * index + deltas(index)
  final def numBytes: Int = 12 + deltas.numBytes

  final def addNA(): AddResponse = ???   // NAs are not supported for delta delta for now
  final def addData(data: Long): AddResponse = {
    val innerValue = data - expected
    deltas.addData(innerValue.toInt) match {
      case Ack =>
        expected += slope
        Ack
      case other: AddResponse => other
    }
  }

  final def addFromReaderNoNA(reader: RowReader, col: Int): AddResponse = addData(reader.getLong(col))

  def reset(): Unit = {
    expected = initValue
    deltas.reset()
  }

  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Long] =
    DeltaDeltaVector(newBase, newOff, numBytes, dispose)
}

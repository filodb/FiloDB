package filodb.memory.format.vectors

import java.nio.ByteBuffer

import scala.util.Try
import scalaxy.loops._

import filodb.memory.MemFactory
import filodb.memory.format.{BinaryAppendableVector, BinaryVector, UnsafeUtils, WireFormat}

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
   * the values as well.  Checks eligibility first based on sampling the input vector.
   * Determines if the input values kinda look like a slope/line and might benefit from delta-delta
   * encoding.  The heuristic is extremely simple for now and looks at the first, last, and sample
   * of other values, unless the vector is really small then we look at everything.
   * @return Some(vector) if the input vect is eligible, or None if it is not eligible
   */
  def fromLongVector(memFactory: MemFactory,
                     inputVect: BinaryAppendableVector[Long],
                     min: Long, max: Long,
                     samples: Int = 4): Option[DeltaDeltaAppendingVector] = {
    val indexDelta = inputVect.length / (samples + 1)
    if (inputVect.noNAs && indexDelta > 0) {
      val diffs = (0 until samples).map { n => inputVect((n + 1) * indexDelta) - inputVect(n * indexDelta) }
      // Good: all diffs positive, min=first elem, max=last elem
      if (min == inputVect(0) && max == inputVect(inputVect.length - 1) && diffs.forall(_ > 0)) {
        for { slope <- getSlope(min, max, inputVect.length)
              deltaVect = appendingVector(memFactory, inputVect.length, min, slope, 32, true)
              appended <- Try(deltaVect.addVector(inputVect)).toOption
        } yield { deltaVect }
      // TODO(velvia): Maybe add less stringent case of slope fitting, flat or negative slope good too.
      } else { None }
    } else { None }
  }

  def apply(buffer: ByteBuffer): BinaryVector[Long] = {
    val (base, off, len) = UnsafeUtils.BOLfromBuffer(buffer)
    DeltaDeltaVector(base, off, len, BinaryVector.NoOpDispose)
  }

  /**
   * Returns the incremental slope from first to last over numElements.
   * If the slope is greater than Int.MaxValue, then None is returned.
   */
  def getSlope(first: Long, last: Long, numElements: Int): Option[Int] = {
    val slope = (last - first) / (numElements - 1)
    if (slope < Int.MaxValue && slope > Int.MinValue) Some(slope.toInt) else None
  }
}

final case class DeltaDeltaVector(base: Any, offset: Long,
                                  numBytes: Int,
                                  val dispose: () => Unit) extends BinaryVector[Long] {
  val vectMajorType = WireFormat.VECTORTYPE_DELTA2
  val vectSubType = WireFormat.SUBTYPE_INT_NOMASK
  val maybeNAs = false
  private final val initValue = UnsafeUtils.getLong(base, offset)
  private final val slope     = UnsafeUtils.getInt(base, offset + 8)
  private final val inner     = IntBinaryVector(base, offset + 12, numBytes - 12, dispose)

  override val length: Int = inner.length
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = initValue + slope * index + inner(index)

}

final case class DeltaTooLarge(value: Long, expected: Long) extends
  IllegalArgumentException(s"Delta too large for value $value")

import filodb.memory.format.Encodings._

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
  private var innerMin: Int = Int.MaxValue
  private var innerMax: Int = Int.MinValue

  UnsafeUtils.setLong(base, offset, initValue)
  UnsafeUtils.setInt(base, offset + 8, slope)

  override def length: Int = deltas.length
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = initValue + slope * index + deltas(index)
  final def numBytes: Int = 12 + deltas.numBytes

  final def addNA(): Unit = ???   // NAs are not supported for delta delta for now
  final def addData(data: Long): Unit = {
    val innerValue = data - expected
    if (innerValue <= Int.MaxValue && innerValue >= Int.MinValue) { deltas.addData(innerValue.toInt) }
    else {
      dispose()
      throw DeltaTooLarge(data, expected)
    }
    innerMin = Math.min(innerMin, innerValue.toInt)
    innerMax = Math.max(innerMax, innerValue.toInt)
    expected += slope
  }


  def reset(): Unit = {
    expected = initValue
    innerMin = Int.MaxValue
    innerMax = Int.MinValue
    deltas.reset()
  }

  def addInnerVectors(other: IntAppendingVector): Unit = {
    for { i <- 0 until other.length optimized } {
      val orig = other(i)
      deltas.addData(orig)
      innerMin = Math.min(innerMin, orig)
      innerMax = Math.max(innerMax, orig)
    }
    expected = initValue + length * slope
  }

  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[Long] =
    DeltaDeltaVector(newBase, newOff, numBytes, dispose)

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[Long] = {
    // Just optimize nbits.
    val (newNbits, newSigned) = IntBinaryVector.minMaxToNbitsSigned(innerMin, innerMax)
    if (newNbits < nbits) {
      val newVect = DeltaDeltaVector.appendingVector(memFactory, deltas.length,
                                                     initValue, slope,
                                                     newNbits, newSigned)
      newVect.addInnerVectors(deltas)
      if (hint == AutoDetectDispose) dispose()
      newVect.freeze(None) // already writing new vector
    } else {
      freeze(memFactory)
    }
  }
}
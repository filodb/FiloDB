package filodb.memory.format.vectors

import debox.Buffer
import spire.syntax.cfor._

import filodb.memory.{BinaryRegion, MemFactory}
import filodb.memory.format._
import filodb.memory.format.BinaryVector.BinaryVectorPtr
import filodb.memory.format.MemoryReader._

final case class MinMax(min: Int, max: Int)

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
    val bytesRequired = 20 + IntBinaryVector.noNAsize(maxElements, nbits)
    val addr = memFactory.allocateOffheap(bytesRequired)
    val dispose = () => memFactory.freeMemory(addr)
    new DeltaDeltaAppendingVector(addr, bytesRequired, initValue, slope, nbits, signed, dispose)
  }

  val MaxApproxDelta = 250   // approx const DDV accepts +/-250 from delta =~ 250 ms
  val MinApproxDelta = -250

  /**
   * Creates a DeltaDeltaAppendingVector from a source AppendableVector[Long], filling in all
   * the values as well.  Tries not to create intermediate vectors by figuring out size of deltas from the source.
   * Eligibility is pretty simple right now:
   * 1) Vectors with 2 or less elements are excluded. Just doesn't make sense.
   * 1a) Vectors with any NAs are not eligible
   * 2) If the deltas end up taking more than maxNBits.  Default is 32, but can be adjusted.
   *
   * @param maxNBits the maximum number of bits for the output DeltaDeltaVector per element
   * @param approxConst if true, and the samples don't vary much from the deltas, go ahead and use a const vector
   *
   * NOTE: no need to get min max before calling this function.  We figure out min max of deltas, much more important
   * @return Some(vector) if the input vect is eligible, or None if it is not eligible
   */
  def fromLongVector(memFactory: MemFactory,
                     inputVect: BinaryAppendableVector[Long],
                     maxNBits: Short = 32,
                     approxConst: Boolean = false): Option[BinaryVectorPtr] =
    if (inputVect.noNAs && inputVect.length > 2) {
      for { slope    <- getSlope(inputVect(0), inputVect(inputVect.length - 1), inputVect.length)
            minMax   <- getDeltasMinMax(inputVect, slope)
            nbitsSigned <- getNbitsSignedFromMinMax(minMax, maxNBits)
      } yield {
        // Min and max == 0 for constant slope otherwise we can have some funny edge cases such as the first value
        // being diff from all others which are the same (eg 55, 60, 60, ....).  That results in erroneous Const
        // encoding.
        if (minMax.min == 0 && minMax.max == 0) {
          const(memFactory, inputVect.length, inputVect(0), slope)
        } else if (approxConst && minMax.min >= MinApproxDelta && minMax.max <= MaxApproxDelta) {
          const(memFactory, inputVect.length, inputVect(0), slope)
        } else {
          val vect = appendingVector(memFactory, inputVect.length, inputVect(0), slope, nbitsSigned._1, nbitsSigned._2)
          vect.addVector(inputVect)
          vect.freeze(None)
        }
      }
    } else { None }

  import WireFormat._

  /**
   * Creates a "constant" DDV.  Layout:
   * +0000 length bytes
   * +0004 WireFormat
   * +0008 logical length / number of elements
   * +0012 initial Long value
   * +0020 slope (int)
   * Total bytes: 24
   */
  def const(memFactory: MemFactory, numElements: Int, initValue: Long, slope: Int): BinaryVectorPtr = {
    val addr = memFactory.allocateOffheap(24)
    UnsafeUtils.setInt(addr, 20)
    UnsafeUtils.setInt(addr + 4, WireFormat(VECTORTYPE_DELTA2, SUBTYPE_REPEATED))
    UnsafeUtils.setInt(addr + 8, numElements)
    UnsafeUtils.setLong(addr + 12, initValue)
    UnsafeUtils.setInt(addr + 20, slope)
    addr
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
  def getDeltasMinMax(inputVect: BinaryAppendableVector[Long], slope: Int): Option[MinMax] = {
    var baseValue: Long = inputVect(0)
    var max = Int.MinValue
    var min = Int.MaxValue
    cforRange { 1 until inputVect.length } { i =>
      baseValue += slope
      val delta = inputVect(i) - baseValue
      if (delta > Int.MaxValue || delta < Int.MinValue) return None   // will not fit in 32 bits, just quit
      max = Math.max(max, delta.toInt)
      min = Math.min(min, delta.toInt)
    }
    Some(MinMax(min, max))
  }

  def getNbitsSignedFromMinMax(minMax: MinMax, maxNBits: Short): Option[(Short, Boolean)] = {
    val (newNbits, newSigned) = IntBinaryVector.minMaxToNbitsSigned(minMax.min, minMax.max)
    if (newNbits <= maxNBits) Some((newNbits, newSigned)) else None
  }
}

/**
 * A normal DeltaDeltaVector has the following layout:
 * +0000 length bytes
 * +0004 WireFormat
 * +0008 initial value (long)
 * +0016 slope (int)
 * +0020 inner IntBinaryVector (including length, wireformat, etc.)
 * Thus overall header for DDV = 28 bytes
 */
object DeltaDeltaDataReader extends LongVectorDataReader {
  val InnerVectorOffset = 20
  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int =
    IntBinaryVector.simple(acc, vector + InnerVectorOffset).length(acc, vector + InnerVectorOffset)
  final def initValue(acc: MemoryReader, vector: BinaryVectorPtr): Long = acc.getLong(vector + 8)
  final def slope(acc: MemoryReader, vector: BinaryVectorPtr): Int = acc.getInt(vector + 16)
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Long = {
    val inner = vector + InnerVectorOffset
    initValue(acc, vector) + slope(acc, vector).toLong * n + IntBinaryVector.simple(acc, inner)(acc, inner, n)
  }

  // Should be close to O(1), initial guess should be almost spot on
  def binarySearch(acc: MemoryReader, vector: BinaryVectorPtr, item: Long): Int = {
    val _slope = slope(acc, vector).toLong
    val _len   = length(acc, vector)
    var elemNo = if (_slope == 0) { if (item <= initValue(acc, vector)) 0 else length(acc, vector) }
                 else             { ((item - initValue(acc, vector) + (_slope - 1)) / _slope).toInt }
    if (elemNo < 0) elemNo = 0
    if (elemNo >= _len) elemNo = _len - 1
    var curBase = initValue(acc, vector) + _slope * elemNo
    val inner = vector + InnerVectorOffset
    val inReader = IntBinaryVector.simple(acc, inner)

    // Back up while we are less than current value until we can't back up no more
    while (elemNo >= 0 && item < (curBase + inReader(acc, inner, elemNo))) {
      elemNo -= 1
      curBase -= _slope
    }

    if (elemNo >= 0 && item == (curBase + inReader(acc, inner, elemNo))) return elemNo

    elemNo += 1
    curBase += _slope

    // Increase while we are greater than current value until past end
    while (elemNo < _len && item > (curBase + inReader(acc, inner, elemNo))) {
      elemNo += 1
      curBase += _slope
    }

    if (elemNo < _len && item == (curBase + inReader(acc, inner, elemNo))) elemNo else elemNo | 0x80000000
  }

  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Double = {
    val inner = vector + InnerVectorOffset
    DeltaDeltaConstDataReader.slopeSum(initValue(acc, vector), slope(acc, vector), start, end) +
      IntBinaryVector.simple(acc, inner).sum(acc, inner, start, end)
  }

  // Efficient iterator as we keep track of current value and inner iterator
  class DeltaDeltaIterator(innerIt: IntIterator, slope: Int, var curBase: Long) extends LongIterator {
    final def next: Long = {
      val out: Long = curBase + innerIt.next
      curBase += slope
      out
    }
  }

  final def iterate(acc: MemoryReader, vector: BinaryVectorPtr, startElement: Int = 0): LongIterator = {
    val inner = vector + InnerVectorOffset
    val innerIt = IntBinaryVector.simple(acc, inner).iterate(acc, inner, startElement)
    new DeltaDeltaIterator(innerIt, slope(acc, vector), initValue(acc, vector) +
      startElement * slope(acc, vector).toLong)
  }

  def changes(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int,
              prev: Long, ignorePrev: Boolean = false): (Long, Long) = {
      require(start >= 0 && end < length(acc, vector), s"($start, $end) is " +
        s"out of bounds, length=${length(acc, vector)}")
      val itr = iterate(acc, vector, start)
      var prevVector: Long = prev
      var changes = 0
      cforRange { start until end + 1 } { i =>
        val cur = itr.next
        if (i == start && ignorePrev) //Initialize prev
          prevVector = cur
        if (prevVector != cur)
          changes += 1
        prevVector = cur
      }
      (changes, prevVector)
  }
}

/**
 * A special case of the DeltaDelta where all the values are exactly on the sloped line.
 * (ie all the deltas are const=0)
 * This can also approximately represent (at great savings) timestamps close to the slope if we agree we are OK
 * losing the exact values when they are really close anyways.
 */
object DeltaDeltaConstDataReader extends LongVectorDataReader {
  override def length(acc: MemoryReader, vector: BinaryVectorPtr): Int = acc.getInt(vector + 8)
  final def initValue(acc: MemoryReader, vector: BinaryVectorPtr): Long = acc.getLong(vector + 12)
  final def slope(acc: MemoryReader, vector: BinaryVectorPtr): Int = acc.getInt(vector + 20)
  final def apply(acc: MemoryReader, vector: BinaryVectorPtr, n: Int): Long =
    initValue(acc, vector) + slope(acc, vector) * n

  // This is O(1) since we can find exactly where on line it is
  final def binarySearch(acc: MemoryReader, vector: BinaryVectorPtr, item: Long): Int = {
    val _slope = slope(acc, vector).toLong
    val guess = if (_slope == 0) { if (item <= initValue(acc, vector)) 0 else length(acc, vector) }
                else             { ((item - initValue(acc, vector) + (_slope - 1)) / _slope).toInt }
    if (guess < 0)                         { 0x80000000 }
    else if (guess >= length(acc, vector))      { 0x80000000 | length(acc, vector) }
    else if (item != apply(acc, vector, guess)) { 0x80000000 | guess }
    else                                   { guess }
  }

  // Formula for sum of items on a sloped line:
  // let len = end - start + 1
  //   = initVal + start*slope + initVal + (start+1)*slope + .... + initVal + end*slope
  //   = len * initVal + len*start*slope + ((end-start)*len/2) * slope
  final def sum(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int): Double = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is " +
      s"out of bounds, length=${length(acc, vector)}")
    slopeSum(initValue(acc, vector), slope(acc, vector), start, end)
  }

  private[memory] def slopeSum(initVal: Long, slope: Int, start: Int, end: Int): Double = {
    val len = end - start + 1
    len.toDouble * (initVal + start * slope.toLong) + ((end-start)*len/2) * slope.toLong
  }

  final def iterate(acc: MemoryReader, vector: BinaryVectorPtr,
                    startElement: Int = 0): LongIterator = new LongIterator {
    private final var curBase = initValue(acc, vector) + startElement * slope(acc, vector).toLong
    final def next: Long = {
      val out = curBase
      curBase += slope(acc, vector)
      out
    }
  }

  def changes(acc: MemoryReader, vector: BinaryVectorPtr, start: Int, end: Int,
              prev: Long, ignorePrev: Boolean = false): (Long, Long) = {
    require(start >= 0 && end < length(acc, vector), s"($start, $end) is out " +
      s"of bounds, length=${length(acc, vector)}")
    val firstValue = apply(acc, vector, start)
    val lastValue = apply(acc, vector, end)
    // compare current element with last element(prev) of previous chunk
    val changes = if (!ignorePrev && prev != firstValue) 1 else 0
    if (slope(acc, vector) == 0) (changes, lastValue) else (end - start + changes, lastValue)
  }
}

// TODO: validate args, esp base offset etc, somehow.  Need to think about this for the many diff classes.
class DeltaDeltaAppendingVector(val addr: BinaryRegion.NativePointer,
                                val maxBytes: Int,
                                initValue: Long,
                                slope: Int,
                                nbits: Short,
                                signed: Boolean,
                                val dispose: () => Unit) extends BinaryAppendableVector[Long] {
  def isAllNA: Boolean = false
  def noNAs: Boolean = true

  private val deltas = IntBinaryVector.appendingVectorNoNA(addr + 20, maxBytes - 20, nbits, signed, dispose)
  private var expected = initValue

  UnsafeUtils.setInt(addr, maxBytes)
  UnsafeUtils.setInt(addr + 4, WireFormat(WireFormat.VECTORTYPE_DELTA2, WireFormat.SUBTYPE_INT_NOMASK))
  UnsafeUtils.setLong(addr + 8, initValue)
  UnsafeUtils.setInt(addr + 16, slope)

  override def length: Int = deltas.length
  final def isAvailable(index: Int): Boolean = true
  final def apply(index: Int): Long = initValue + slope.toLong * index + deltas(index)
  final def numBytes: Int = 20 + deltas.numBytes
  final def reader: VectorDataReader = DeltaDeltaDataReader
  final def copyToBuffer: Buffer[Long] = DeltaDeltaDataReader.toBuffer(nativePtrReader, addr)

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

  def finishCompaction(newAddr: BinaryRegion.NativePointer): BinaryVectorPtr = {
    UnsafeUtils.setInt(newAddr, numBytes - 4)
    newAddr
  }
}

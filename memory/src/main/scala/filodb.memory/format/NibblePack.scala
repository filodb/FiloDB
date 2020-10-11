package filodb.memory.format

import java.nio.ByteOrder.LITTLE_ENDIAN

import org.agrona.{DirectBuffer, MutableDirectBuffer}
import spire.syntax.cfor._

/**
 * An implementation of the NibblePack algorithm for efficient encoding, see [compression.md](doc/compression.md).
 * Works with a predictor that maximizes zero bits/words of floating point or integer data.
 */
object NibblePack {
  /**
   * Packs Long values directly using NibblePack.  Internally uses pack8.  Inputs are not transformed.
   */
  final def packNonIncreasing(input: Array[Long], buf: MutableDirectBuffer, bufindex: Int): Int = {
    val inputArray = tempArray
    var i = 0
    var pos = bufindex
    while (i < input.size) {
      inputArray(i % 8) = input(i)
      i += 1
      if (i % 8 == 0) {
        pos = pack8(inputArray, buf, pos)
      }
    }

    // Flush remainder - if any left
    packRemainder(inputArray, buf, pos, i)
  }

  /**
   * Packs Long values which must be positive and increasing.  If the next value is lower than the previous value
   * then a 0 is packed -- negative deltas are not allowed.
   * @return the final position within the buffer after packing
   */
  final def packDelta(input: Array[Long], buf: MutableDirectBuffer, bufindex: Int): Int = {
    val inputArray = tempArray
    var last = 0L
    var i = 0
    var pos = bufindex
    while (i < input.size) {
      val delta = if (input(i) >= last) input(i) - last else 0L
      last = input(i)
      inputArray(i % 8) = delta
      i += 1
      if (i % 8 == 0) {
        pos = pack8(inputArray, buf, pos)
      }
    }

    // Flush remainder - if any left
    packRemainder(inputArray, buf, pos, i)
  }

  @inline
  private def packRemainder(input: Array[Long], buf: MutableDirectBuffer, pos: Int, i: Int): Int =
    if (i % 8 != 0) {
      cforRange { (i % 8) until 8 } { j => input(j) = 0 }
      pack8(input, buf, pos)
    } else {
      pos
    }

  /**
   * Packs Double values using XOR encoding to find minimal # of bits difference between successive values.
   * Initial Double value is written first.
   * @return the final position within the buffer after packing
   */
  final def packDoubles(inputs: Array[Double], buf: MutableDirectBuffer, bufindex: Int): Int = {
    require(inputs.size > 0)
    buf.putDouble(bufindex, inputs(0), LITTLE_ENDIAN)
    var pos = bufindex + 8

    val inputArray = tempArray
    var last = java.lang.Double.doubleToLongBits(inputs(0))
    var i = 0
    while (i < (inputs.size - 1)) {
      val bits = java.lang.Double.doubleToLongBits(inputs(i + 1))
      inputArray(i % 8) = bits ^ last
      last = bits
      i += 1
      if (i % 8 == 0) {
        pos = pack8(inputArray, buf, pos)
      }
    }

    // Flush remainder - if any left
    if (i % 8 != 0) {
      cforRange { (i % 8) until 8 } { j => inputArray(j) = 0 }
      pos = pack8(inputArray, buf, pos)
    }

    pos
  }

  /**
   * Packs 8 input values into a buffer using NibblePacking. Returns ending buffer position.
   * This is an internal method, usually one wants to use one of the other pack* methods.
   * @param buf the MutableDirectBuffer into which to write.  Recommended is to use ExpandableArrayBuffer or
   *            ExpandableDirectByteBuffer so that it can grow as needed.
   * @param bufindex the starting index of the output buffer into which to write
   * @return the ending MutableDirectBuffer position
   */
  final def pack8(input: Array[Long], buf: MutableDirectBuffer, bufindex: Int): Int = {
    var bufpos = bufindex
    require(input.size >= 8)

    var bitmask = 0
    // Figure out which words are nonzero, pack bitmask
    cforRange { 0 until 8 } { i =>
      if (input(i) != 0) bitmask |= 1 << i
    }
    buf.putByte(bufpos, bitmask.toByte)
    bufpos += 1

    if (bitmask != 0) {
      // figure out min # of nibbles to represent nonzero words
      var minLeadingZeros = 64
      var minTrailingZeros = 64
      cforRange { 0 until 8 } { i =>
        minLeadingZeros = Math.min(minLeadingZeros, java.lang.Long.numberOfLeadingZeros(input(i)))
        minTrailingZeros = Math.min(minTrailingZeros, java.lang.Long.numberOfTrailingZeros(input(i)))
      }

      val trailingNibbles = minTrailingZeros / 4
      val numNibbles = 16 - (minLeadingZeros / 4) - trailingNibbles
      val nibbleWord = (((numNibbles - 1) << 4) | trailingNibbles)
      buf.putByte(bufpos, nibbleWord.toByte)
      bufpos += 1

      // Decide which packer to use
      bufpos = packUniversal(input, buf, bufpos, numNibbles, trailingNibbles)
    }

    bufpos
  }

  // Returns the final bufpos
  private def packUniversal(inputs: Array[Long], buf: MutableDirectBuffer, bufindex: Int,
                    numNibbles: Int, trailingZeroNibbles: Int): Int = {
    var bufpos = bufindex
    val trailingShift = trailingZeroNibbles * 4
    val numBits = numNibbles * 4
    var outWord = 0L
    var bitCursor = 0

    cforRange { 0 until 8 } { i =>
      val input = inputs(i)
      if (input != 0) {
        val remaining = 64 - bitCursor
        val shiftedInput = input >>> trailingShift

        // This is least significant portion of input
        outWord |= shiftedInput << bitCursor

        // Write out current word if we've used up all 64 bits
        if (remaining <= numBits) {
          buf.putLong(bufpos, outWord, LITTLE_ENDIAN)
          bufpos += 8
          if (remaining < numBits) {
              // Most significant portion left over from previous word
              outWord = shiftedInput >>> remaining
          } else {
              outWord = 0    // reset for 64-bit input case
          }
        }

        bitCursor = (bitCursor + numBits) % 64
      }
    }

    // Write remainder word if there are any bits remaining, and only advance buffer right # of bytes
    if (bitCursor > 0) {
        buf.putLong(bufpos, outWord, LITTLE_ENDIAN)
        bufpos += ((bitCursor + 7) / 8)
    }

    bufpos
  }

  private val tlTempArray = new ThreadLocal[Array[Long]]()
  def tempArray: Array[Long] = tlTempArray.get match {
    case UnsafeUtils.ZeroPointer => val newArray = new Array[Long](8)
                                    tlTempArray.set(newArray)
                                    newArray
    case a: Array[Long]          => a
  }

  sealed trait UnpackResult
  case object Ok extends UnpackResult
  case object InputTooShort extends UnpackResult

  val empty = Array.empty[Byte]

  val zeroOutput = new Array[Long](8)

  trait Sink {
    /**
     * Process 8 u64's/Longs of output at a time
     */
    def process(data: Array[Long]): Unit
  }

  final case class DeltaSink(outArray: Array[Long]) extends Sink {
    private var current: Long = 0L
    private var i: Int = 0
    final def process(data: Array[Long]): Unit = {
      // It's necessary to ignore "extra" elements because NibblePack always unpacks in multiples of 8, but the
      // user might not intuitively allocate output arrays in elements of 8.
      val numElems = Math.min(outArray.size - i, 8)
      require(numElems > 0)
      cforRange { 0 until numElems } { n =>
        current += data(n)
        outArray(i + n) = current
      }
      i += 8
    }
    def reset(): Unit = {
      i = 0
      current = 0L
    }
  }

  final case class DoubleXORSink(outArray: Array[Double], initial: Long) extends Sink {
    private var lastBits = initial
    private var pos: Int = 1
    def process(data: Array[Long]): Unit = {
      val numElems = Math.min(outArray.size - pos, 8)
      cforRange { 0 until numElems } { n =>
        val nextBits = lastBits ^ data(n)
        outArray(pos + n) = java.lang.Double.longBitsToDouble(nextBits)
        lastBits = nextBits
      }
      pos += 8
    }
  }

  /**
   * A sink used for increasing histogram counters.  In one shot:
   * - Unpacks a delta-encoded NibblePack compressed Histogram
   * - Subtracts the values from lastHistValues, noting if the difference is not >= 0 (means counter reset)
   * - Packs the subtracted values
   * - Updates lastHistValues to the latest unpacked values so this sink can be used again
   *
   * Basically the delta from the last histogram is stored. This is great for compression, but not so great at
   * query time as it means arriving at the original value requires adding up most of the histograms in a section.
   *
   * It is designed to be reused for delta repacking successive histograms.  Thus, the initial write position
   * pos is not reset after finish(), instead it keeps increasing.  writePos may be adjusted by users, make sure
   * you know what you are doing.
   *
   * For more details, see the "2D Delta" section in [compression.md](doc/compression.md)
   *
   * @param lastHistDeltas last histogram deltas as they arrive "over the wire"
   * @param outBuf the MutableDirectBuffer to write the compressed 2D delta histogram to
   * @param pos the initial write position for the outBuf
   */
  final case class DeltaDiffPackSink(lastHistDeltas: Array[Long], outBuf: MutableDirectBuffer, pos: Int = 0)
  extends Sink {
    // True if a packed value dropped
    var valueDropped: Boolean = false
    private var i: Int = 0
    private val packArray = new Array[Long](8)
    var writePos: Int = pos

    def reset(): Unit = {
      i = 0
      valueDropped = false
    }

    final def process(data: Array[Long]): Unit = {
      val numElems = Math.min(lastHistDeltas.size - i, 8)
      cforRange { 0 until numElems } { n =>
        val diff = data(n) - lastHistDeltas(i + n)
        if (diff < 0) valueDropped = true
        packArray(n) = diff
      }
      // Copy in bulk current data to lastHistDeltas
      System.arraycopy(data, 0, lastHistDeltas, i, numElems)
      // if numElems < 8, zero out remainder of packArray
      if (numElems < 8) java.util.Arrays.fill(packArray, numElems, 8, 0L)
      writePos = pack8(packArray, outBuf, writePos)
      i += 8
    }
  }

  /**
   * Very similar to the DeltaDiffPackSink, except that instead of storing deltas from the last histogram, it
   * stores the delta from the originalDeltas.  This allows any original histogram to be reconstructed just by adding
   * originalDeltas to the stored value.  Doesn't compress quite as well but much less work needs to be done
   * at query time.
   * It still checks for any drops.
   * This sink is initialized with the number of buckets and fed the FIRST histogram.  After that reset() is called.
   * If a new section is to be started or after first histogram then setOriginal() should be called.
   */
  final class DeltaSectDiffPackSink(numBuckets: Int, outBuf: MutableDirectBuffer, pos: Int = 0)
  extends Sink {
    val originalDeltas = new Array[Long](numBuckets)
    val lastHistDeltas = new Array[Long](numBuckets)
    var valueDropped: Boolean = false
    private var i: Int = 0
    private val packArray = new Array[Long](8)
    var writePos: Int = pos

    def reset(): Unit = {
      i = 0
      valueDropped = false
    }

    final def process(data: Array[Long]): Unit = {
      val numElems = Math.min(numBuckets - i, 8)
      cforRange { 0 until numElems } { n =>
        if (data(n) < lastHistDeltas(i + n)) valueDropped = true
        packArray(n) = data(n) - originalDeltas(i + n)
      }
      System.arraycopy(data, 0, lastHistDeltas, i, numElems)
      // if numElems < 8, zero out remainder of packArray
      if (numElems < 8) java.util.Arrays.fill(packArray, numElems, 8, 0L)
      writePos = pack8(packArray, outBuf, writePos)
      i += 8
    }

    def debugString: String =
      s"numBuckets=$numBuckets i=$i pos=$pos writePos=$writePos valueDropped=$valueDropped " +
      s"lastHistDeltas=${lastHistDeltas.toList} originalDeltas=${originalDeltas.toList}"

    /**
     * After process(), call this method if it was the start of a new section or first histogram
     */
    def setOriginal(): Unit = {
      System.arraycopy(lastHistDeltas, 0, originalDeltas, 0, numBuckets)
    }
 }

  /**
   * Generic unpack function which outputs values to a Sink which can process raw 64-bit values from unpack8.
   * @param compressed a DirectBuffer wrapping the compressed bytes. Position 0 must be the beginning of the buffer
   *                   to unpack.  NOTE: the passed in DirectBuffer will be mutated to wrap the NEXT bytes that
   *                   can be unpacked.
   */
  final def unpackToSink(compressed: DirectBuffer, sink: Sink, numValues: Int): UnpackResult = {
    var res: UnpackResult = Ok
    var valuesLeft = numValues
    while (valuesLeft > 0 && res == Ok && compressed.capacity > 0) {
      res = unpack8(compressed, sink)
      valuesLeft -= 8
    }
    res
  }

  final def unpackDoubleXOR(compressed: DirectBuffer, outArray: Array[Double]): UnpackResult = {
    if (compressed.capacity < 8) {
      InputTooShort
    } else {
      val initVal = readLong(compressed, 0)
      val sink = DoubleXORSink(outArray, initVal)
      outArray(0) = java.lang.Double.longBitsToDouble(initVal)
      subslice(compressed, 8)
      unpackToSink(compressed, sink, outArray.size - 1)
    }
  }

  /**
   * Unpacks 8 words of input at a time.
   * @param compressed a DirectBuffer wrapping the compressed bytes. Position 0 must be the beginning of the buffer
   *                   to unpack.  NOTE: the passed in DirectBuffer will be mutated to wrap the NEXT bytes that
   *                   can be unpacked.
   * @param sink a Sink for processing output values.  8 Longs will be passed to it.
   * @return an UnpackResult
   */
  //scalastyle:off method.length
  final def unpack8(compressed: DirectBuffer, sink: Sink): UnpackResult = {
    val nonzeroMask = compressed.getByte(0)
    if (nonzeroMask == 0) {
      sink.process(zeroOutput)
      subslice(compressed, 1)
      Ok
    } else {
      val numNibblesU8 = compressed.getByte(1) & 0x00ff     // Make sure this is unsigned 8 bits!
      val numBits = ((numNibblesU8 >>> 4) + 1) * 4
      val trailingZeroes = (numNibblesU8 & 0x0f) * 4
      val totalBytes = 2 + (numBits * java.lang.Integer.bitCount(nonzeroMask & 0x0ff) + 7) / 8
      val mask = if (numBits >= 64) -1L else (1L << numBits) - 1
      var bufIndex = 2
      var bitCursor = 0
      val outArray = tempArray

      var inWord = readLong(compressed, bufIndex)
      bufIndex += 8

      cforRange { 0 until 8 } { bit =>
        if ((nonzeroMask & (1 << bit)) != 0) {
          val remaining = 64 - bitCursor

          // Shift and read in LSB
          val shiftedIn = inWord >>> bitCursor
          var outWord = shiftedIn & mask

          // If remaining bits are in next word, read next word, unless no more space
          if (remaining <= numBits && bufIndex < totalBytes) {
            if (bufIndex < compressed.capacity) {
              inWord = readLong(compressed, bufIndex)
              bufIndex += 8
              if (remaining < numBits) {
                outWord |= (inWord << remaining) & mask
              }
            } else {
              return InputTooShort
            }
          }

          outArray(bit) = outWord << trailingZeroes
          bitCursor = (bitCursor + numBits) % 64
        } else {
          outArray(bit) = 0
        }
      }

      sink.process(outArray)
      // Return the "remaining slice" for easy and clean chaining of nibble_unpack8 calls with no mutable state
      subslice(compressed, totalBytes)
      Ok
    }
  }
  //scalastyle:on method.length

  private def subslice(buffer: DirectBuffer, start: Int): Unit =
    if (buffer.capacity > start) {
      buffer.wrap(buffer, start, buffer.capacity - start)
    } else {
      buffer.wrap(empty)
    }

  // Method to read a Long but ensure we don't step out of bounds at the end if we don't have 8 bytes left
  private def readLong(inbuf: DirectBuffer, index: Int): Long = {
    if ((index + 8) <= inbuf.capacity) {
      inbuf.getLong(index, LITTLE_ENDIAN)
    } else {
      var i = 0
      var outWord = 0L
      while (index + i < inbuf.capacity) {
        outWord |= (inbuf.getByte(index + i) & 0x00ffL) << (8 * i)
        i += 1
      }
      outWord
    }
  }
}

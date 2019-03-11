package filodb.memory.format

import java.nio.ByteOrder.LITTLE_ENDIAN

import org.agrona.{DirectBuffer, MutableDirectBuffer}
import scalaxy.loops._

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
    if (i % 8 != 0) {
      for { j <- (i % 8) until 8 optimized } { inputArray(j) = 0 }
      pos = pack8(inputArray, buf, pos)
    }

    pos
  }

  /**
   * Packs Long values which must be positive and increasing.  If the next value is lower than the previous value
   * then a 0 is packed -- negative deltas are not allowed.
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
    if (i % 8 != 0) {
      for { j <- (i % 8) until 8 optimized } { inputArray(j) = 0 }
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
    for { i <- 0 until 8 optimized } {
      if (input(i) != 0) bitmask |= 1 << i
    }
    buf.putByte(bufpos, bitmask.toByte)
    bufpos += 1

    if (bitmask != 0) {
      // figure out min # of nibbles to represent nonzero words
      var minLeadingZeros = 64
      var minTrailingZeros = 64
      for { i <- 0 until 8 optimized } {
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

    for { i <- 0 until 8 optimized } {
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

  trait Sink {
    def process(data: Long): Unit
  }

  final case class DeltaSink(outArray: Array[Long]) extends Sink {
    private var current: Long = 0L
    private var pos: Int = 0
    def process(data: Long): Unit = {
      // It's necessary to ignore "extra" elements because NibblePack always unpacks in multiples of 8, but the
      // user might not intuitively allocate output arrays in elements of 8.
      if (pos < outArray.size) {
        current += data
        outArray(pos) = current
        pos += 1
      }
    }
  }

  /**
   * Generic unpack function which outputs values to a Sink which can process raw 64-bit values from unpack8.
   * @param compressed a DirectBuffer wrapping the compressed bytes. Position 0 must be the beginning of the buffer
   *                   to unpack.  NOTE: the passed in DirectBuffer will be mutated to wrap the NEXT bytes that
   *                   can be unpacked.
   */
  final def unpackToSink(compressed: DirectBuffer, sink: Sink, numValues: Int): UnpackResult = {
    val array = tempArray
    var res: UnpackResult = Ok
    var valuesLeft = numValues
    while (valuesLeft > 0 && res == Ok && compressed.capacity > 0) {
      res = unpack8(compressed, array)
      if (res == Ok) {
        for { i <- 0 until 8 optimized } {
          sink.process(array(i))
        }
        valuesLeft -= 8
      }
    }
    res
  }

  /**
   * Unpacks 8 words of input at a time.
   * @param compressed a DirectBuffer wrapping the compressed bytes. Position 0 must be the beginning of the buffer
   *                   to unpack.  NOTE: the passed in DirectBuffer will be mutated to wrap the NEXT bytes that
   *                   can be unpacked.
   * @param outArray an output array with at least 8 slots for Long values
   * @return an UnpackResult
   */
  final def unpack8(compressed: DirectBuffer, outArray: Array[Long]): UnpackResult = {
    val nonzeroMask = compressed.getByte(0)
    if (nonzeroMask == 0) {
      java.util.Arrays.fill(outArray, 0L)
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

      var inWord = readLong(compressed, bufIndex)
      bufIndex += 8

      for { bit <- 0 until 8 optimized } {
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

      // Return the "remaining slice" for easy and clean chaining of nibble_unpack8 calls with no mutable state
      subslice(compressed, totalBytes)
      Ok
    }
  }

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
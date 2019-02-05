package filodb.memory.format

import java.nio.ByteOrder

import org.agrona.concurrent.UnsafeBuffer
import scalaxy.loops._

/**
 * An implementation of the NibblePack algorithm for efficient encoding, see [[doc/compression.md]]
 */
object NibblePack {
  /**
   * Packs 8 input values into a buffer. Returns # of bytes encoded.
   */
  def pack8(input: Array[Long], buf: UnsafeBuffer): Int = {
    var bufpos = 0
    require(input.size >= 8)

    var bitmask = 0
    // Figure out which words are nonzero, pack bitmask
    for { i <- 0 until 8 optimized } {
      if (input(i) != 0) bitmask |= 1 << i
    }
    buf.putByte(0, bitmask.toByte)
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
      val nibbleWord = ((numNibbles << 4) | trailingNibbles)
      buf.putByte(1, nibbleWord.toByte)
      bufpos += 1

      // Decide which packer to use
      bufpos = if (numNibbles % 2 == 0) {
        packToEvenNibbles(input, buf, numNibbles, trailingNibbles)
      } else {
        packToOddNibbles1(input, buf, numNibbles, trailingNibbles)
      }
    }

    bufpos
  }

  // Retuerns the final bufpos
  def packToEvenNibbles(inputs: Array[Long], buf: UnsafeBuffer, numNibbles: Int, trailingZeroNibbles: Int): Int = {
    var bufpos = 2
    val shift = trailingZeroNibbles * 4
    val numBytesEach = numNibbles / 2
    for { i <- 0 until 8 optimized } {
      if (inputs(i) != 0) {
        buf.putLong(bufpos, inputs(i) >> shift, ByteOrder.LITTLE_ENDIAN)
        bufpos += numBytesEach
      }
    }
    bufpos
  }

  // import scala.util.control.Breaks._

  def packToOddNibbles1(inputs: Array[Long], buf: UnsafeBuffer, numNibbles: Int, trailingZeroNibbles: Int): Int = {
    val shift = trailingZeroNibbles * 4
    var i = 0
    var bufpos = 2
    while (i < 8) {
        while (i < 8 && inputs(i) == 0) i += 1
        if (i < 8) {
          // if nonzero, shift first value into place
          var packedword = inputs(i) >> shift
          var bytespacked = (numNibbles + 1) / 2

          // find second value, shift into upper place
          i += 1
          while (i < 8 && inputs(i) == 0) i += 1
          if (i < 8) {
            packedword |= (inputs(i) >> shift) << (numNibbles * 4)
            bytespacked = numNibbles
          }
          // write out both values together
          buf.putLong(bufpos, packedword)
          bufpos += bytespacked
          i += 1
        }
    }
    bufpos
  }
}
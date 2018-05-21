package filodb.core.binaryrecord2

import filodb.memory.{BinaryRegionConsumer, BinaryRegionLarge}
import filodb.memory.format.UnsafeUtils

/**
 * A RecordContainer is a binary, wire-compatible container for BinaryRecords V2.
 * Example uses: for holding/batching multiple BinaryRecords for Kafka, or as a set of partition keys.
 * It is also the container to which the RecordBuilder writes into.
 *
 * A RecordContainer is compatible with BinaryRegionLarge.
 *
 * +0000 4 bytes  length header - number of bytes following this
 *
 * @param base null for offheap, or an Array[Byte] for onheap
 * @param offset 64-bit native pointer or offset into Array[Byte] object memory
 * @param maxLength the maximum allocated size the container can grow to, including the initial length bytes
 */
final class RecordContainer(val base: Any, val offset: Long, maxLength: Int) {
  def numBytes: Int =  UnsafeUtils.getInt(base, offset)

  /**
   * Used only by the RecordBuilder to update the length field.
   */
  private[binaryrecord2] def updateLengthWithOffset(endOffset: Long): Unit = {
    val newLength = endOffset - offset - 4
    require(newLength >= 0 && newLength <= (maxLength - 4))
    UnsafeUtils.setInt(base, offset, newLength.toInt)
  }

  /**
   * Iterates through each BinaryRecord location, passing it to the Consumer so that no object allocations
   * are done.  Method returns when we have iterated through all the records.
   */
  def consumeRecords(consumer: BinaryRegionConsumer): Unit = {
    val endOffset = offset + 4 + numBytes
    var curOffset = offset + 4
    while (curOffset < endOffset) {
      val recordLen = BinaryRegionLarge.numBytes(base, curOffset)
      consumer.onNext(base, curOffset)
      curOffset += (recordLen + 7) & ~3   // +4, then aligned/rounded up to next 4 bytes
    }
  }

  /**
   * Returns true if this RecordContainer is backed by a byte array object on heap.
   */
  def hasArray: Boolean = base match {
    case UnsafeUtils.ZeroPointer => false
    case b: Array[Byte]          => true
    case other: Any              => false
  }

  /**
   * IF this is an on-heap container, return the Array[Byte] object AS IS (meaning might not be full)
   * Otherwise throw an Exception
   */
  def array: Array[Byte] =
    if (hasArray) base.asInstanceOf[Array[Byte]] else throw new UnsupportedOperationException("Not an array container")

  /**
   * Returns a NEW byte array which is of minimal size by copying only the filled bytes.
   * Can also be used to copy offheap containers to onheap byte array.
   */
  def trimmedArray: Array[Byte] = {
    val newBytes = new Array[Byte](numBytes + 4)
    UnsafeUtils.unsafe.copyMemory(base, offset, newBytes, UnsafeUtils.arayOffset, numBytes + 4)
    newBytes
  }
}

object RecordContainer {
  /**
   * Returns a read-only container; updateLength will never work.
   */
  def apply(array: Array[Byte]): RecordContainer = new RecordContainer(array, UnsafeUtils.arayOffset, 0)
}
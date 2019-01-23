package filodb.core.binaryrecord2

import java.nio.ByteBuffer

import scala.reflect.ClassTag

import debox.Buffer

import filodb.memory.{BinaryRegionConsumer, BinaryRegionLarge}
import filodb.memory.format.{RowReader, UnsafeUtils}

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
final class RecordContainer(val base: Any, val offset: Long, maxLength: Int,
                            // INTERNAL variable placed here to avoid Scalac using a bitfield and slowing things down
                            var numRecords: Int = 0) {
  import RecordBuilder._

  @inline final def numBytes: Int =  UnsafeUtils.getInt(base, offset)
  @inline final def isEmpty: Boolean = numBytes <= 4

  /**
   * Used only by the RecordBuilder to update the length field.
   */
  private[binaryrecord2] def updateLengthWithOffset(endOffset: Long): Unit = {
    val newLength = endOffset - offset - 4
    require(newLength >= 0 && newLength <= (maxLength - 4))
    UnsafeUtils.setInt(base, offset, newLength.toInt)
  }

  private[binaryrecord2] def writeVersionWord(): Unit = {
    val word = Version << 24
    UnsafeUtils.setInt(base, offset + 4, word)
  }

  /**
   * Iterates through each BinaryRecord location, passing it to the Consumer so that no object allocations
   * are done.  Method returns when we have iterated through all the records.
   */
  final def consumeRecords(consumer: BinaryRegionConsumer): Unit = {
    val endOffset = offset + 4 + numBytes
    var curOffset = offset + ContainerHeaderLen
    while (curOffset < endOffset) {
      val recordLen = BinaryRegionLarge.numBytes(base, curOffset)
      consumer.onNext(base, curOffset)
      curOffset += (recordLen + 7) & ~3   // +4, then aligned/rounded up to next 4 bytes
    }
  }

  /**
   * Iterates through each BinaryRecord as a RowReader.  Results in two allocations: the Iterator
   * as well as a BinaryRecordRowReader.
   */
  final def iterate(schema: RecordSchema): Iterator[RowReader] = new Iterator[RowReader] {
    val reader = new BinaryRecordRowReader(schema, base)
    val endOffset = offset + 4 + numBytes
    var curOffset = offset + ContainerHeaderLen

    final def hasNext: Boolean = curOffset < endOffset
    final def next: RowReader = {
      val recordLen = BinaryRegionLarge.numBytes(base, curOffset)
      reader.recordOffset = curOffset
      curOffset += (recordLen + 7) & ~3   // +4, then aligned/rounded up to next 4 bytes
      reader
    }
  }

  class FunctionalConsumer(func: (Any, Long) => Unit) extends BinaryRegionConsumer {
    def onNext(base: Any, offset: Long): Unit = func(base, offset)
  }

  /**
   * A convenience method to iterate through each BinaryRecord using a function.  Probably not as efficient
   * as directly calling consumeRecords, but easier to write.
   * TODO: make this a MACRO and rewrite it as a custom BinaryRegionConsumer.
   */
  def foreach(func: (Any, Long) => Unit): Unit = consumeRecords(new FunctionalConsumer(func))

  /**
   * Convenience functional method to map the BinaryRecords to something else and returns a Buffer which
   * can be unboxed and efficient.  If you want performance, consider using consumeRecords instead.
   */
  def map[@specialized B: ClassTag](func: (Any, Long) => B): Buffer[B] = {
    val result = Buffer.empty[B]
    foreach { case (b, o) => result += func(b, o) }
    result
  }

  /**
   * A convenient method to return all of the record offsets (or native pointers if offheap) for this container.
   * @return a debox.Buffer[Long], which conveniently does not box, and minimizes memory use.
   */
  final def allOffsets: Buffer[Long] = map { case (b, o) => o }

  class CountingConsumer(var count: Int = 0) extends BinaryRegionConsumer {
    final def onNext(base: Any, offset: Long): Unit = count += 1
  }

  /**
   * Uses consumeRecords with a consumer that counts the # of records
   */
  final def countRecords(): Int = {
    val counter = new CountingConsumer()
    consumeRecords(counter)
    counter.count
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

  /**
   * Returns a read-only container given a ByteBuffer wrapper
   */
  def apply(buf: ByteBuffer): RecordContainer = {
    val (base, offset, _) = UnsafeUtils.BOLfromBuffer(buf)
    new RecordContainer(base, offset, 0)
  }
}

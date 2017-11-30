package filodb.memory.format.vectors

import filodb.memory.MemFactory
import filodb.memory.format.{BinaryAppendableVector, BinaryVector, UnsafeUtils, WireFormat}
import filodb.memory.format.Encodings._

object ConstVector {
  /**
   * Allocates and returns bytes for a ConstVector.
   * @param len the logical length or # of repeats
   * @param neededBytes the bytes needed for one element
   * @param fillBytes a function to fill out bytes at given base and offset
   * @return the (base, offset, numBytes) for the ConstVector
   */
  def make(memFactory: MemFactory,len: Int, neededBytes: Int)(fillBytes: (Any, Long) => Unit): (Any, Long, Int) = {
    val (base, off, nBytes) = memFactory.allocateWithMagicHeader(4 + neededBytes)
    UnsafeUtils.setInt(base, off, len)
    fillBytes(base, off + 4)
    (base, off, nBytes)
  }
}

trait ConstVectorType {
  val vectMajorType = WireFormat.VECTORTYPE_BINSIMPLE
  val vectSubType   = WireFormat.SUBTYPE_REPEATED
}

/**
 * A vector which holds the value of one element repeated n times.
 */
abstract class ConstVector[A](val base: Any, val offset: Long, val numBytes: Int) extends
BinaryVector[A] with ConstVectorType {
  override val length = UnsafeUtils.getInt(base, offset)
  protected val dataOffset = offset + 4
  final def isAvailable(i: Int): Boolean = true
  val maybeNAs = false
}

/**
 * An AppendingVector-API compatible class for situations (such as fixed partition keys) where you know
 * the values will be constant and just need an Appender.  All this class really does is count up however
 * many instances to repeat, then generates the ConstVector. It also ensures that that field really is
 * a constant.
 */
abstract class ConstAppendingVector[@specialized(Int, Long, Double) A](value: A,
                                                                       neededBytes: Int,
                                                                       initLen: Int = 0)
extends BinaryAppendableVector[A] with ConstVectorType {
  private var len = initLen
  // The code to store the value
  def fillBytes(base: Any, offset: Long): Unit

  final def apply(index: Int): A = value
  final def addData(data: A): Unit = { len += 1 }
  final def addNA(): Unit = { len += 1 }
  final def isAvailable(index: Int): Boolean = true
  final def base: Any = this
  final def numBytes: Int = frozenSize
  final def offset: Long = 0
  final override def length: Int = len

  val maxBytes = frozenSize
  val isAllNA = false
  val noNAs = true
  val maybeNAs = false
  override def frozenSize: Int = 4 + neededBytes
  final def reset(): Unit = { len = 0 }
  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[A] = {
    val (b, o, l) = ConstVector.make(memFactory, len, neededBytes)(fillBytes)
    finishCompaction(b, o)
  }
}
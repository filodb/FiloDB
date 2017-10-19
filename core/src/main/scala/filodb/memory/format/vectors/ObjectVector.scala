package filodb.memory.format.vectors

import filodb.memory.format.{BinaryAppendableVector, BinaryVector, UnsafeUtils}

object ObjectVector {
  val objectRefSize = UnsafeUtils.unsafe.arrayIndexScale(classOf[Array[Any]])
}

import filodb.memory.format.Encodings._

/**
 * Technically not a binary/serializable vector, rather one that holds object references.
 * However it conforms to the BinaryVector API for convenience.  Probably faster than a normal Scala Seq.
 * Should never be serialized -- and this is enforced by not implementing a few types
 */
abstract class ObjectVector[T](val base: Any,
                      val offset: Long,
                      val maxBytes: Int,
                      perElem: Int = ObjectVector.objectRefSize) extends BinaryAppendableVector[T] {
  def numBytes: Int = (writeOffset - offset).toInt
  override final def length: Int = numBytes / perElem

  private var writeOffset = offset
  private var numNAs = 0
  private var maxStrLen = 0

  final def reset(): Unit = {
    writeOffset = offset
  }

  val maybeNAs = true
  // scalastyle:off
  final def isAvailable(index: Int): Boolean =
    UnsafeUtils.unsafe.getObject(base, offset + perElem * index) != null
  final def apply(index: Int): T =
    UnsafeUtils.unsafe.getObject(base, offset + perElem * index).asInstanceOf[T]

  def addData(data: T): Unit = {
    checkSize(numBytes, maxBytes)
    UnsafeUtils.unsafe.putObject(base, writeOffset, data)
    writeOffset += perElem
  }

  def addNA(): Unit = {
    checkSize(numBytes, maxBytes)
    UnsafeUtils.unsafe.putObject(base, writeOffset, null)
    writeOffset += perElem
    numNAs += 1
  }
  // scalastyle:on
  final def isAllNA: Boolean = numNAs == length
  final def noNAs: Boolean = numNAs == 0

  def vectMajorType: Int = ???
  def vectSubType: Int = ???
  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[T] =
    throw new RuntimeException("Cannot finalize an ObjectVector")

  override def optimize(hint: EncodingHint = AutoDetect): BinaryVector[T] = suboptimize(hint)
  def suboptimize(hint: EncodingHint = AutoDetect): BinaryVector[T]
}
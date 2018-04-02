package filodb.memory.format.vectors

import filodb.memory.MemFactory
import filodb.memory.format._
import filodb.memory.format.Encodings._

object ObjectVector {
  val objectRefSize = UnsafeUtils.unsafe.arrayIndexScale(classOf[Array[Any]])
}

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

  def addData(data: T): AddResponse = checkSize(numBytes, maxBytes) match {
    case Ack =>
      checkSize(numBytes, maxBytes)
      UnsafeUtils.unsafe.putObject(base, writeOffset, data)
      writeOffset += perElem
      Ack
    case other: AddResponse => other
  }

  def addNA(): AddResponse = checkSize(numBytes, maxBytes) match {
    case Ack =>
      UnsafeUtils.unsafe.putObject(base, writeOffset, null)
      writeOffset += perElem
      numNAs += 1
      Ack
    case other: AddResponse => other
  }
  // scalastyle:on
  final def isAllNA: Boolean = numNAs == length
  final def noNAs: Boolean = numNAs == 0

  def vectMajorType: Int = ???
  def vectSubType: Int = ???
  def finishCompaction(newBase: Any, newOff: Long): BinaryVector[T] =
    throw new RuntimeException("Cannot finalize an ObjectVector")

  override def optimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[T]
  = suboptimize(MemFactory.onHeapFactory, hint)

  def suboptimize(memFactory: MemFactory, hint: EncodingHint = AutoDetect): BinaryVector[T]
}
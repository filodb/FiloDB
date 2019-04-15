package filodb.memory

import net.jpountz.xxhash.XXHashFactory

import filodb.memory.format.UnsafeUtils

/**
 * A BinaryRegion is just an area of memory (heap or offheap) with a length prefix.
 * There are different implementations depending on the size of the length prefix.
 * This design allows us to reference an offheap BinaryRegion and operate/compare them with nothing more than
 * a Long native pointer, avoiding expensive heap/object allocations.
 * Examples of their use are for UTF8Strings and BinaryRecords.
 */
object BinaryRegion {
  import UnsafeUtils._

  // NOTE: fastestInstance sometimes returns JNI lib, which seems much slower for shorter strings
  // NOTE2: According to XXHash documentation the hash method is thread safe.
  val xxhashFactory = XXHashFactory.fastestJavaInstance
  val hasher32 = xxhashFactory.hash32
  val hasher64 = xxhashFactory.hash64
  val Seed = 0x9747b28c

  /**
    * A memory has a base, offset and a length. An off-heap memory usually has a null base.
    * An array backed memory has the array object as the base. Offset is a Long indicating the
    * location in native memory. For an array it is retrieved using Unsafe.arrayBaseOffset
    * Length is the length of memory in bytes
    */
  type Memory = Tuple3[Any, Long, Int]

  def hash32(bytes: Array[Byte]): Int = hasher32.hash(bytes, 0, bytes.size, Seed)

  // TODO: Can we PLEASE implement our own Unsafe XXHash which does not require creating a DirectBuffer?
  def hash32(base: Any, offset: Long, len: Int): Int = base match {
    case a: Array[Byte]          => hasher32.hash(a, offset.toInt - UnsafeUtils.arayOffset, len, Seed)
    case UnsafeUtils.ZeroPointer => hasher32.hash(UnsafeUtils.asDirectBuffer(offset, len), Seed)
  }

  /**
   * Returns true if the source byte array is equal to the destination byte array, at the given
   * index and # of bytes into the source array.  Destination is compared whole.
   */
  def equalBytes(source: Array[Byte], srcIndex: Int, srcNumBytes: Int, dest: Array[Byte]): Boolean =
    dest.size == srcNumBytes && equate(dest, arayOffset, source, srcIndex + arayOffset, srcNumBytes)

  def copyArray(source: Array[Byte], dest: Array[Byte], destOffset: Int): Unit =
    System.arraycopy(source, 0, dest, destOffset, source.size)

  // 64-bit pointer to native/offheap memory.  NOTE: instead of using this, please use the Ptr*
  // value classes as they are much more type safe
  type NativePointer = Long
}

trait BinaryRegion {
  import format.UnsafeUtils
  import BinaryRegion._

  // Returns the length from the initial bytes of the region
  def numBytes(base: Any, offset: Long): Int
  final def numBytes(address: Long): Int = numBytes(UnsafeUtils.ZeroPointer, address)

  // The number of bytes used up by the length header
  def lenBytes: Int

  /**
   * Returns 1 if region1 (base1, offset1) > region2 (base2, offset2), 0 if equal, -1 if region1 is less
   * Compares byte by byte.  The minimum of the lengths of two regions are compared.
   * Note: for equality the equals() method is probably faster.
   */
  final def compare(base1: Any, offset1: Long, base2: Any, offset2: Long): Int = {
    val numBytes1 = numBytes(base1, offset1)
    val numBytes2 = numBytes(base2, offset2)
    UnsafeUtils.compare(base1, offset1 + lenBytes, numBytes1, base2, offset2 + lenBytes, numBytes2)
  }

  final def compare(address1: NativePointer, address2: NativePointer): Int =
    compare(UnsafeUtils.ZeroPointer, address1, UnsafeUtils.ZeroPointer, address2)

  final def asNewByteArray(base: Any, offset: Long): Array[Byte] = {
    val numBytes1 = numBytes(base, offset)
    val bytes = new Array[Byte](numBytes1 + lenBytes)
    UnsafeUtils.unsafe.copyMemory(base, offset, bytes, UnsafeUtils.arayOffset, numBytes1 + lenBytes)
    bytes
  }

  final def asNewByteArray(addr: NativePointer): Array[Byte] = asNewByteArray(UnsafeUtils.ZeroPointer, addr)

  /**
   * Allocates from the MemFactory and copies to the allocated space the entire BinaryRegion
   */
  final def allocateAndCopy(base: Any, offset: Long, factory: MemFactory): Memory = {
    val numBytes1 = numBytes(base, offset)
    val memory = factory.allocate(numBytes1 + lenBytes)
    UnsafeUtils.unsafe.copyMemory(base, offset, memory._1, memory._2, numBytes1 + lenBytes)
    memory
  }

  /**
   * Returns true if both regions are byte for byte equal and the same length.
   * Uses Long compares for speed.
   * Note: does not use hashcode, because we are not caching the hashcode.
   */
  final def equals(base1: Any, offset1: Long, base2: Any, offset2: Long): Boolean = {
    val numBytes1 = numBytes(base1, offset1)
    val numBytes2 = numBytes(base2, offset2)
    (numBytes1 == numBytes2) &&
    UnsafeUtils.equate(base1, offset1 + lenBytes, base2, offset2 + lenBytes, numBytes1)
  }

  final def equals(address1: NativePointer, address2: NativePointer): Boolean =
    equals(UnsafeUtils.ZeroPointer, address1, UnsafeUtils.ZeroPointer, address2)

  final def hashCode(base: Any, offset: Long): Int = {
    val numBytes1 = numBytes(base, offset)
    base match {
      case a: Array[Byte] => hasher32.hash(a, offset.toInt - UnsafeUtils.arayOffset + lenBytes, numBytes1, Seed)
      case UnsafeUtils.ZeroPointer => ???   // offheap.  This is not supported yet
      case other: Any => throw new UnsupportedOperationException(s"Cannot compute hash for base $base")
    }
  }

  final def hashCode64(base: Any, offset: Long): Long = {
    val numBytes1 = numBytes(base, offset)
    base match {
      case a: Array[Byte] => hasher64.hash(a, offset.toInt - UnsafeUtils.arayOffset + lenBytes, numBytes1, Seed)
      case UnsafeUtils.ZeroPointer => ???   // offheap.  This is not supported yet
      case other: Any => throw new UnsupportedOperationException(s"Cannot compute hash for base $base")
    }
  }
}

/**
 * A BinaryRegionMedium uses two bytes to store the length prefix, thus a region can be up to 64KB in size.
 */
object BinaryRegionMedium extends BinaryRegion {
  import format.UnsafeUtils

  final def numBytes(base: Any, offset: Long): Int = UnsafeUtils.getShort(base, offset) & 0x0FFFF

  val lenBytes = 2
}

/**
 * A BinaryRegionLarge uses four bytes to store the length prefix, thus a region can be up to 2GB in size.
 */
object BinaryRegionLarge extends BinaryRegion {
  import format.UnsafeUtils

  final def numBytes(base: Any, offset: Long): Int = UnsafeUtils.getInt(base, offset)

  val lenBytes = 4
}

/**
 * A simple Consumer so we can consume regions without on heap allocation
 */
trait BinaryRegionConsumer {
  def onNext(base: Any, offset: Long): Unit
}
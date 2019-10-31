package filodb.memory.format

// A value class for unsigned 8-bit numbers, intended for safe byte/low-level access
final case class IntU8 private(n: Int) extends AnyVal

/**
 * Strongly typed native memory pointer types / value classes.
 * Do not incur allocations as long as value class rules are followed.
 * Separate mutable and immutable types.
 * Helps prevent accidental mixing of Int/Long types and pointers, which lead to SEGVs.
 * Modeled after the equivalent Ptr types in Rust.
 */
object Ptr {
  // A read-only pointer to bytes. Equivalent to *const u8 in Rust/C.  No length safety, but much better than
  // type equivalency to Long's.  Use it to prevent type errors where one accidentally substitutes ints or longs.
  final case class U8(addr: Long) extends AnyVal {
    // Simple pointer math, by # of U8 elements (bytes)
    final def add(offset: Int): U8 = U8(addr + offset)
    //scalastyle:off method.name
    final def +(offset: Int): U8 = U8(addr + offset)

    final def getU8(acc: MemoryReader): Int = acc.getByte(addr) & 0x00ff

    final def asU16: U16 = U16(addr)
    final def asI32: I32 = I32(addr)
    final def asMut: MutU8 = MutU8(addr)
  }

  final case class MutU8(addr: Long) extends AnyVal {
    final def set(acc: MemoryAccessor, num: Int): Unit = acc.setByte(addr, num.toByte)
  }

  final case class U16(addr: Long) extends AnyVal {
    final def add(offset: Int): U16 = U16(addr + offset * 2)
    final def +(offset: Int): U16 = U16(addr + offset * 2)

    final def getU16(acc: MemoryReader): Int = acc.getShort(addr) & 0x00ffff

    final def asMut: MutU16 = MutU16(addr)
  }

  final case class MutU16(addr: Long) extends AnyVal {
    final def set(acc: MemoryAccessor, num: Int): Unit = acc.setShort(addr, num.toShort)
  }

  final case class I32(addr: Long) extends AnyVal {
    final def add(offset: Int): I32 = I32(addr + offset * 4)
    final def +(offset: Int): I32 = I32(addr + offset * 4)

    final def getI32(acc: MemoryReader): Int = acc.getInt(addr)

    final def asMut: MutI32 = MutI32(addr)
  }

  final case class MutI32(addr: Long) extends AnyVal {
    final def set(acc: MemoryAccessor, num: Int): Unit = acc.setInt(addr, num)
  }
}
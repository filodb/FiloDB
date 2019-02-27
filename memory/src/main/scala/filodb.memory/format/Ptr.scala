package filodb.memory.format

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

    final def get: Byte = UnsafeUtils.getByte(addr)
  }
}
package filodb.memory.format

/**
 * Filo wire format definitions - especially for the header bytes.
 * See [wire_format.md] for details.
 */
object WireFormat {
  val VECTORTYPE_EMPTY = 0x01
  val VECTORTYPE_SIMPLE = 0x02
  val VECTORTYPE_DICT = 0x03
  val VECTORTYPE_CONST = 0x04
  val VECTORTYPE_DIFF = 0x05
  val VECTORTYPE_BINSIMPLE = 0x06
  val VECTORTYPE_BINDICT = 0x07
  val VECTORTYPE_DELTA2 = 0x08    // Delta-delta encoded

  def majorVectorType(headerBytes: Int): Int = headerBytes & 0x00ff
  def emptyVectorLen(headerBytes: Int): Int = {
    require(majorVectorType(headerBytes) == VECTORTYPE_EMPTY)
    java.lang.Integer.rotateRight(headerBytes & 0xffffff00, 8)
  }

  val SUBTYPE_PRIMITIVE = 0x00
  val SUBTYPE_STRING = 0x01
  val SUBTYPE_UTF8 = 0x02
  val SUBTYPE_FIXEDMAXUTF8 = 0x03    // fixed max size per blob, length byte
  val SUBTYPE_DATETIME = 0x04
  val SUBTYPE_PRIMITIVE_NOMASK = 0x05
  val SUBTYPE_REPEATED = 0x06        // vectors.ConstVector
  val SUBTYPE_INT = 0x07             // Int gets special type because Longs and Doubles may be encoded as Int
  val SUBTYPE_INT_NOMASK = 0x08

  def vectorSubType(headerBytes: Int): Int = (headerBytes & 0x00ff00) >> 8

  val MaxEmptyVectorLen = 0x00ffffff

  def emptyVector(len: Int): Int = {
    require(len <= MaxEmptyVectorLen, "Vector len too long")
    (len << 8) | VECTORTYPE_EMPTY
  }

  def apply(majorVectorType: Int, subType: Int): Int =
    ((subType & 0x00ff) << 8) | (majorVectorType & 0x00ff)
}
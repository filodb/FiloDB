package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets
import java.util

import filodb.memory.UTF8StringMedium
import filodb.memory.UTF8StringShort
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.UnsafeUtils.{arayOffset, setByte}

// scalastyle:off null
/**
 * Pre-encoded tag map in RecordBuilder's exact wire format, including predefined key handling.
 *
 * Stores tags as a single byte[] using the same encoding as RecordBuilder.addMapKeyValue():
 *   Predefined key: [0xC0 | predefKeyNum (1B)][valLen (2B native-endian)][valBytes]
 *   Full key:       [keyLen (1B)][keyBytes UTF-8][valLen (2B native-endian)][valBytes UTF-8]
 *
 * The byte[] stores entries only — no 2-byte map length header (RecordBuilder adds that).
 * Immutable after construction. Thread-safe for reads.
 *
 * IMPORTANT: The encoding in PreEncodedMap.encode() must exactly match
 * RecordBuilder.addMapKeyValue(). If the wire format changes in either place,
 * both must be updated together.
 */
final class PreEncodedMap private(private val data: Array[Byte], val schema: RecordSchema) {

  /** Raw entry bytes. */
  def bytes(): Array[Byte] = data

  /** Number of bytes in the encoded data. */
  def size(): Int = data.length

  /** Whether this map has no entries. */
  def isEmpty: Boolean = data.length == 0

  /**
   * Decode all entries into a sorted Java Map.
   *
   * Uses the exact same encoding as RecordBuilder.addMapKeyValue:
   * - Predefined key lookup via RecordSchema.makeKeyKey + predefKeyNumMap
   * - UTF8StringShort for non-predefined keys (1B length + bytes)
   * - UTF8StringMedium for values (2B length + bytes, native endian)
   */
  def toJavaMap(): java.util.Map[String, String] = {
    val map = new util.TreeMap[String, String]()
    if (data.length == 0) return map

    var pos = 0
    while (pos < data.length) {
      val firstByte = data(pos) & 0xFF
      val predefIndex = firstByte ^ 0xC0

      if (predefIndex < 64) {
        // Predefined key — decode from schema
        val keyOff = schema.predefKeyOffsets(predefIndex)
        val keyLen = UTF8StringShort.numBytes(schema.predefKeyBytes, keyOff)
        val keyStr = new String(schema.predefKeyBytes,
          (keyOff + 1 - UnsafeUtils.arayOffset).toInt, keyLen, StandardCharsets.UTF_8)

        val valLen = UnsafeUtils.getShort(data, UnsafeUtils.arayOffset + pos + 1) & 0xFFFF
        val valStr = new String(data, pos + 3, valLen, StandardCharsets.UTF_8)
        map.put(keyStr, valStr)
        pos += 3 + valLen
      } else {
        // Full key
        val keyLen = firstByte
        val keyStr = new String(data, pos + 1, keyLen, StandardCharsets.UTF_8)

        val valLenPos = pos + 1 + keyLen
        val valLen = UnsafeUtils.getShort(data, UnsafeUtils.arayOffset + valLenPos) & 0xFFFF
        val valStr = new String(data, valLenPos + 2, valLen, StandardCharsets.UTF_8)
        map.put(keyStr, valStr)
        pos += 1 + keyLen + 2 + valLen
      }
    }
    map
  }

  override def toString: String = toJavaMap().toString
}

object PreEncodedMap {
  private val EMPTY_DATA = new Array[Byte](0)

  /**
   * Encode a sorted tag map into RecordBuilder's wire format with predefined key handling.
   * The input map MUST be sorted by key (e.g. TreeMap) to match RecordBuilder's convention.
   *
   * Uses the exact same encoding as RecordBuilder.addMapKeyValue:
   * - Predefined key lookup via RecordSchema.makeKeyKey + predefKeyNumMap
   * - UTF8StringShort for non-predefined keys (1B length + bytes)
   * - UTF8StringMedium for values (2B length + bytes, native endian)
   */
  def encode(tags: java.util.Map[String, String], schema: RecordSchema): PreEncodedMap = {
    if (tags == null || tags.isEmpty) return empty(schema)

    // Upper bound: each entry at most 1 + keyLen*3 + 2 + valLen*3 (UTF-8 worst case)
    var upperBound = 0
    val iter = tags.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      upperBound += 1 + entry.getKey.length * 3 + 2 + entry.getValue.length * 3
    }
    val buf = new Array[Byte](upperBound)
    var pos = 0

    val iter2 = tags.entrySet().iterator()
    while (iter2.hasNext) {
      val entry = iter2.next()
      val keyBytes = entry.getKey.getBytes(StandardCharsets.UTF_8)
      val valBytes = entry.getValue.getBytes(StandardCharsets.UTF_8)

      require(keyBytes.length < 192, s"Key too long: ${entry.getKey} (${keyBytes.length} bytes, max 191)")
      require(valBytes.length < 65536, s"Value too long: ${entry.getValue} (${valBytes.length} bytes)")

      // Predefined key lookup
      val keyKey = RecordSchema.makeKeyKey(keyBytes, 0, keyBytes.length, 7)
      val predefNum = schema.predefKeyNumMap.getOrElse(keyKey, -1)

      if (predefNum >= 0) {
        // Predefined key: 1-byte code
        setByte(buf, arayOffset + pos, (0xC0 | predefNum).toByte)
        pos += 1
      } else {
        // Full key: UTF8StringShort (1B length + bytes)
        UTF8StringShort.copyByteArrayTo(keyBytes, buf, arayOffset + pos)
        pos += 1 + keyBytes.length
      }

      // Value: UTF8StringMedium (2B length + bytes, native endian)
      UTF8StringMedium.copyByteArrayTo(valBytes, buf, arayOffset + pos)
      pos += 2 + valBytes.length
    }

    new PreEncodedMap(util.Arrays.copyOf(buf, pos), schema)
  }

  /** Wrap raw pre-encoded bytes from deserialization. Zero-copy. */
  def fromBytes(data: Array[Byte], schema: RecordSchema): PreEncodedMap = {
    if (data == null || data.length == 0) empty(schema)
    else new PreEncodedMap(data, schema)
  }

  /** Create an empty PreEncodedMap. */
  def empty(schema: RecordSchema): PreEncodedMap = new PreEncodedMap(EMPTY_DATA, schema)
}

package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets
import java.util

import filodb.memory.UTF8StringMedium
import filodb.memory.UTF8StringShort
import filodb.memory.format.UnsafeUtils
import filodb.memory.format.UnsafeUtils.{arayOffset, setByte}

/**
 * Callback for iterating over encoded map entries via MapEncoder.forEach.
 */
trait MapEntryConsumer {
  def consume(key: String, value: String): Unit
}

// scalastyle:off null
/**
 * Stateless utility for encoding/decoding tag maps in RecordBuilder's exact wire format.
 *
 * Wire format per entry:
 *   Predefined key: [0xC0 | predefKeyNum (1B)][valLen (2B native-endian)][valBytes]
 *   Full key:       [keyLen (1B)][keyBytes UTF-8][valLen (2B native-endian)][valBytes UTF-8]
 *
 * The byte[] contains entries only — no 2-byte map length header (RecordBuilder adds that
 * via addPreEncodedMap).
 *
 * IMPORTANT: The encoding in MapEncoder.encode() must exactly match
 * RecordBuilder.addMapKeyValue(). If the wire format changes in either place,
 * both must be updated together.
 */
object MapEncoder {

  val EMPTY: Array[Byte] = new Array[Byte](0)

  /**
   * Encode a tag map into RecordBuilder's wire format with predefined key handling.
   * TreeMap guarantees keys are sorted, matching RecordBuilder's convention.
   *
   * @param tags TreeMap of tag key-value pairs (sorted by key)
   * @param schema the RecordSchema with predefined key definitions for encoding
   * @return encoded byte[] in RecordBuilder wire format
   */
  def encode(tags: java.util.TreeMap[String, String], schema: RecordSchema): Array[Byte] = {
    if (tags == null || tags.isEmpty) return EMPTY

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

      // Predefined key lookup — matches RecordBuilder.addMapKeyValue exactly
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

    util.Arrays.copyOf(buf, pos)
  }

  /**
   * Decode encoded map bytes into a sorted Java Map.
   *
   * @param data encoded byte[] in RecordBuilder wire format
   * @param schema the RecordSchema with predefined key definitions for decoding key codes
   * @return sorted TreeMap of decoded key-value pairs
   */
  def toJavaMap(data: Array[Byte], schema: RecordSchema): java.util.Map[String, String] = {
    val map = new util.TreeMap[String, String]()
    if (data == null || data.length == 0) return map

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

  /** Check if encoded map bytes are empty. */
  def isEmpty(data: Array[Byte]): Boolean = data == null || data.length == 0

  /** Return the number of entries in encoded map bytes. */
  def size(data: Array[Byte]): Int = {
    if (data == null || data.length == 0) return 0
    var n = 0
    var pos = 0
    while (pos < data.length) {
      pos += entryByteLength(data, pos)
      n += 1
    }
    n
  }

  /**
   * Look up a single key's value in encoded bytes without decoding the entire map.
   * Returns null if the key is not found.
   */
  def getValue(data: Array[Byte], schema: RecordSchema, key: String): String = {
    if (data == null || data.length == 0 || key == null) return null

    val keyBytes = key.getBytes(StandardCharsets.UTF_8)
    val keyKey = RecordSchema.makeKeyKey(keyBytes, 0, keyBytes.length, 7)
    val predefNum = schema.predefKeyNumMap.getOrElse(keyKey, -1)

    if (predefNum >= 0) {
      val expectedByte = (0xC0 | predefNum).toByte
      var pos = 0
      while (pos < data.length) {
        if (data(pos) == expectedByte) return decodeValueAtPos(data, pos)
        pos += entryByteLength(data, pos)
      }
    } else {
      var pos = 0
      while (pos < data.length) {
        val firstByte = data(pos) & 0xFF
        if ((firstByte ^ 0xC0) >= 64 && keyMatchesAtPos(data, pos, schema, keyBytes)) {
          return decodeValueAtPos(data, pos)
        }
        pos += entryByteLength(data, pos)
      }
    }
    null
  }

  /**
   * Iterate all entries, invoking consumer with decoded key and value Strings.
   * No Map allocation — entries are streamed one at a time.
   */
  def forEach(data: Array[Byte], schema: RecordSchema, consumer: MapEntryConsumer): Unit = {
    if (data == null || data.length == 0) return

    var pos = 0
    while (pos < data.length) {
      consumer.consume(resolveKeyAtPos(data, pos, schema), decodeValueAtPos(data, pos))
      pos += entryByteLength(data, pos)
    }
  }

  /**
   * Return new encoded bytes containing only entries whose key is in keysToKeep.
   * Entry bytes are copied directly — values are never decoded to String.
   */
  def retain(data: Array[Byte], schema: RecordSchema,
             keysToKeep: java.util.Set[String]): Array[Byte] = {
    filterEntries(data, schema, keysToKeep, keep = true)
  }

  /**
   * Return new encoded bytes with entries whose key is in keysToRemove removed.
   * Entry bytes are copied directly — values are never decoded to String.
   */
  def remove(data: Array[Byte], schema: RecordSchema,
             keysToRemove: java.util.Set[String]): Array[Byte] = {
    filterEntries(data, schema, keysToRemove, keep = false)
  }

  /**
   * Overload of retain that accepts pre-encoded UTF-8 key byte arrays.
   * Both sides compared as raw bytes — zero per-entry String allocation.
   * Caller should convert keys to bytes once and reuse across calls.
   */
  def retain(data: Array[Byte], schema: RecordSchema,
             keysToKeep: Array[Array[Byte]]): Array[Byte] = {
    filterEntriesByBytes(data, schema, keysToKeep, keep = true)
  }

  /**
   * Overload of remove that accepts pre-encoded UTF-8 key byte arrays.
   * Both sides compared as raw bytes — zero per-entry String allocation.
   * Caller should convert keys to bytes once and reuse across calls.
   */
  def remove(data: Array[Byte], schema: RecordSchema,
             keysToRemove: Array[Array[Byte]]): Array[Byte] = {
    filterEntriesByBytes(data, schema, keysToRemove, keep = false)
  }

  /**
   * Shared implementation for retain/remove (String version). Walks encoded bytes,
   * resolves each key to a String, and checks set membership.
   *
   * @param keep if true, keeps entries IN the set (retain). If false, keeps entries NOT in the set (remove).
   */
  private def filterEntries(data: Array[Byte], schema: RecordSchema,
                            keySet: java.util.Set[String], keep: Boolean): Array[Byte] = {
    if (data == null || data.length == 0) return EMPTY
    if (keySet == null || keySet.isEmpty) {
      return if (keep) EMPTY else util.Arrays.copyOf(data, data.length)
    }

    val buf = new Array[Byte](data.length)
    var outPos = 0
    var pos = 0

    while (pos < data.length) {
      val keyStr = resolveKeyAtPos(data, pos, schema)
      val entryLen = entryByteLength(data, pos)

      val inSet = keySet.contains(keyStr)
      if ((keep && inSet) || (!keep && !inSet)) {
        System.arraycopy(data, pos, buf, outPos, entryLen)
        outPos += entryLen
      }
      pos += entryLen
    }

    if (outPos == 0) EMPTY
    else util.Arrays.copyOf(buf, outPos)
  }

  /**
   * Shared implementation for retain/remove (byte array version). Walks encoded
   * bytes, matches each key via byte comparison — zero per-entry String allocation.
   *
   * @param keep if true, keeps entries IN the set (retain). If false, keeps entries NOT in the set (remove).
   */
  private def filterEntriesByBytes(data: Array[Byte], schema: RecordSchema,
                                   keys: Array[Array[Byte]], keep: Boolean): Array[Byte] = {
    if (data == null || data.length == 0) return EMPTY
    if (keys == null || keys.length == 0) {
      return if (keep) EMPTY else util.Arrays.copyOf(data, data.length)
    }

    val buf = new Array[Byte](data.length)
    var outPos = 0
    var pos = 0

    while (pos < data.length) {
      val entryLen = entryByteLength(data, pos)
      val inSet = entryKeyMatchesAny(data, pos, schema, keys)

      if ((keep && inSet) || (!keep && !inSet)) {
        System.arraycopy(data, pos, buf, outPos, entryLen)
        outPos += entryLen
      }
      pos += entryLen
    }

    if (outPos == 0) EMPTY
    else util.Arrays.copyOf(buf, outPos)
  }

  private def entryKeyMatchesAny(data: Array[Byte], pos: Int, schema: RecordSchema,
                                 keys: Array[Array[Byte]]): Boolean = {
    var i = 0
    while (i < keys.length) {
      if (keyMatchesAtPos(data, pos, schema, keys(i))) return true
      i += 1
    }
    false
  }

  /** Check if the encoded key at pos matches keyBytes without allocating a String. */
  private def keyMatchesAtPos(data: Array[Byte], pos: Int, schema: RecordSchema,
                              keyBytes: Array[Byte]): Boolean = {
    val firstByte = data(pos) & 0xFF
    val predefIndex = firstByte ^ 0xC0
    if (predefIndex < 64) {
      val keyOff = schema.predefKeyOffsets(predefIndex)
      val keyLen = UTF8StringShort.numBytes(schema.predefKeyBytes, keyOff)
      if (keyLen != keyBytes.length) return false
      val startIdx = (keyOff + 1 - UnsafeUtils.arayOffset).toInt
      var i = 0
      while (i < keyLen) {
        if (keyBytes(i) != schema.predefKeyBytes(startIdx + i)) return false
        i += 1
      }
      true
    } else {
      val keyLen = firstByte
      if (keyLen != keyBytes.length) return false
      var i = 0
      while (i < keyLen) {
        if (keyBytes(i) != data(pos + 1 + i)) return false
        i += 1
      }
      true
    }
  }

  /** Resolve the key String at the given position in encoded data. */
  private def resolveKeyAtPos(data: Array[Byte], pos: Int, schema: RecordSchema): String = {
    val firstByte = data(pos) & 0xFF
    val predefIndex = firstByte ^ 0xC0
    if (predefIndex < 64) {
      val keyOff = schema.predefKeyOffsets(predefIndex)
      val keyLen = UTF8StringShort.numBytes(schema.predefKeyBytes, keyOff)
      new String(schema.predefKeyBytes,
        (keyOff + 1 - UnsafeUtils.arayOffset).toInt, keyLen, StandardCharsets.UTF_8)
    } else {
      val keyLen = firstByte
      new String(data, pos + 1, keyLen, StandardCharsets.UTF_8)
    }
  }

  /** Decode the value String at the given entry position. */
  private def decodeValueAtPos(data: Array[Byte], pos: Int): String = {
    val firstByte = data(pos) & 0xFF
    val predefIndex = firstByte ^ 0xC0
    if (predefIndex < 64) {
      val valLen = UnsafeUtils.getShort(data, UnsafeUtils.arayOffset + pos + 1) & 0xFFFF
      new String(data, pos + 3, valLen, StandardCharsets.UTF_8)
    } else {
      val keyLen = firstByte
      val valLenPos = pos + 1 + keyLen
      val valLen = UnsafeUtils.getShort(data, UnsafeUtils.arayOffset + valLenPos) & 0xFFFF
      new String(data, valLenPos + 2, valLen, StandardCharsets.UTF_8)
    }
  }

  /** Compute the total byte length of the entry at the given position. */
  private def entryByteLength(data: Array[Byte], pos: Int): Int = {
    val firstByte = data(pos) & 0xFF
    val predefIndex = firstByte ^ 0xC0
    if (predefIndex < 64) {
      val valLen = UnsafeUtils.getShort(data, UnsafeUtils.arayOffset + pos + 1) & 0xFFFF
      3 + valLen
    } else {
      val keyLen = firstByte
      val valLenPos = pos + 1 + keyLen
      val valLen = UnsafeUtils.getShort(data, UnsafeUtils.arayOffset + valLenPos) & 0xFFFF
      1 + keyLen + 2 + valLen
    }
  }
}

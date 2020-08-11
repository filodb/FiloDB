package filodb.gateway.conversion

import com.typesafe.scalalogging.StrictLogging
import debox.Buffer
import org.jboss.netty.buffer.ChannelBuffer
import spire.syntax.cfor._

import filodb.core.binaryrecord2.RecordBuilder
import filodb.memory.format.UnsafeUtils

trait KVVisitor {
  def apply(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueIndex: Int, valueLen: Int): Unit
}

class StringKVVisitor extends KVVisitor {
  val keyValues = new collection.mutable.ArrayBuffer[(String, String)]
  def apply(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueIndex: Int, valueLen: Int): Unit =
    keyValues += new String(bytes, keyIndex, keyLen) -> new String(bytes, valueIndex, valueLen)
}

/**
 * A KVVisitor that parses Influx format field values into the right types and invokes the right callback
 * based on the type of the detected field.
 */
trait InfluxFieldVisitor extends KVVisitor with StrictLogging {
  def doubleValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, value: Double): Unit
  def stringValue(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueOffset: Int, valueLen: Int): Unit
  final def apply(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueIndex: Int, valueLen: Int): Unit = {
    // Empty value :(  don't even try to parse
    if (valueLen < 1) {
      stringValue(bytes, keyIndex, keyLen, valueIndex, 0)
    // If the first value char is a quote, then assume end is also a quote and only pass what's within quotes
    } else if (bytes(valueIndex) == InfluxProtocolParser.Quote && valueLen >= 2) {
      stringValue(bytes, keyIndex, keyLen, valueIndex + 1, valueLen - 2)
    // Convert to string then parse as a Double.
    // If it ends in i then it is an integer
    } else {
      val numBytesToParse = if (bytes(valueIndex + valueLen - 1) == 'i') valueLen - 1 else valueLen
      try {
        doubleValue(bytes, keyIndex, keyLen, InfluxProtocolParser.parseDouble(bytes, valueIndex, numBytesToParse))
      } catch {
        case e: Exception =>
          logger.error(s"Could not parse [${new String(bytes, valueIndex, numBytesToParse)}] as number!", e)
          stringValue(bytes, keyIndex, keyLen, valueIndex, valueLen)
      }
    }
  }
}

/**
 * A KVVisitor that adds key-value pairs to a map. Must have called startRecord() & startMap() already.
 */
class MapBuilderVisitor(builder: RecordBuilder) extends KVVisitor {
  final def apply(bytes: Array[Byte], keyIndex: Int, keyLen: Int, valueIndex: Int, valueLen: Int): Unit =
    builder.addMapKeyValue(bytes, keyIndex, keyLen, bytes, valueIndex, valueLen)
}

/**
 * A parser for the InfluxDB Line Protocol
 * https://docs.influxdata.com/influxdb/v1.6/write_protocols/line_protocol_tutorial/
 *
 * NOTE: the Telegraf Influx output format always seems to sort tags by key.  Thus we save time and don't sort them.
 * NOTE2: we truncate the Influx nanosecond precision timestamp to millisecond Unix epoch timestamp.
 */
object InfluxProtocolParser extends StrictLogging {
  val Comma = ','.toByte
  val Space = ' '.toByte
  val Escape = '\\'.toByte
  val Equals = '='.toByte
  val Quote = '"'.toByte

  val CounterKey = "counter".getBytes

  /**
   * Read from a Netty ChannelBuffer and write an escape-corrected array of bytes,
   * noting down where delim/commas occurs, and stopping at spaces.
   * Basically parses a single section (either tags or fields) of the Influx protocol.
   * buffer will be left at the byte after stopper unless all bytes are read.
   * This parser does delimitation and un-escaping in one pass.  Avoids extra heap allocations.
   *
   * The offsets buffer integer has the following format:
   *   - lower 16 bits: array index offset
   *   - upper 16 bits: offset of the equals sign if any
   * @return the index within destArray AFTER last char written.  Note the stopper char is also copied.
   */
  def parseInner(buffer: ChannelBuffer,
                 destArray: Array[Byte],
                 destOffset: Int,
                 delimOffsets: Buffer[Int]): Int = {
    var togo = buffer.readableBytes
    var destOff = destOffset
    while (togo > 0) {
      val byteToCopy = buffer.readByte() match {
        case Escape if togo > 1 =>      // escape char.  Get the next byte from buffer and copy that as is
          togo -= 1
          buffer.readByte()
        case Space              =>      // copy it ourselves then quit
          destArray(destOff) = Space
          return destOff + 1
        case Comma              =>      // delimiter.  Copy byte but add next offset to offsets buffer
          delimOffsets += destOff + 1
          Comma
        case Equals             =>      // set equals offset in upper 16 bits
          val curIndex = delimOffsets.length - 1
          if (curIndex >= 0) {
            delimOffsets(curIndex) = delimOffsets(curIndex) | (destOff << 16)
          }
          Equals
        case b                  => b
      }
      destArray(destOff) = byteToCopy
      destOff += 1
      togo -= 1
    }
    destOff
  }

  def keyOffset(offsetInt: Int): Int = offsetInt & 0x0ffff
  def keyLen(offsetInt: Int): Int = ((offsetInt >> 16) & 0x0ffff) - (offsetInt & 0x0ffff)
  def valOffset(offsetInt: Int): Int = ((offsetInt >> 16) & 0x0ffff) + 1

  def parse(buffer: ChannelBuffer): Option[InfluxRecord] = {
    val bytes = new Array[Byte](buffer.readableBytes)   // max parsed size is original bytes
    val tagOffsets = debox.Buffer.empty[Int]            // array indices of each tag k=v pair.

    // parse measurement (KPI name) and tags
    val fieldSetIndex = parseInner(buffer, bytes, 0, tagOffsets)
    val measurementLen = if (tagOffsets.isEmpty) fieldSetIndex - 1 else keyOffset(tagOffsets(0)) - 1
    if (measurementLen == 0) {
      logger.info(s"Empty measurement value!!")
      return None
    }

    // parse fields.  Note that a field takes minimum three chars a=b
    val fieldOffsets = Buffer(fieldSetIndex)   // should be at least one field
    val timeIndex = parseInner(buffer, bytes, fieldSetIndex, fieldOffsets)
    if (timeIndex < (fieldSetIndex + 3)) {
      logger.info(s"No fields in Influx record!  Line=${new String(bytes)}")
      return None
    }

    // copy and parse timestamp.  Use system time if no timestamp field
    val tsLen = buffer.readableBytes
    val timestamp = if (tsLen > 0) {
      buffer.readBytes(bytes, timeIndex, tsLen)
      val ts = parseUnixTime(bytes, timeIndex, tsLen)
      if (ts < 0) {
        logger.info(s"Could not parse timestamp!  Line=${new String(bytes)}")
        return None
      }
      ts
    } else {
      System.currentTimeMillis
    }

    // Create the right InfluxRecord depending on # of fields
    if (fieldOffsets.length == 1) {
      Some(InfluxPromSingleRecord(bytes, measurementLen, tagOffsets,
                                  fieldOffsets, timeIndex - 1, timestamp))
    } else {
      Some(InfluxHistogramRecord(bytes, measurementLen, tagOffsets,
                                 fieldOffsets, timeIndex - 1, timestamp))
    }
  }

  // Parses key=value comma separated lists by calling the visitor for each pair
  // relies on offsets being in the same order as keys
  def parseKeyValues(bytes: Array[Byte], offsets: Buffer[Int], endOffset: Int, visitor: KVVisitor): Unit = {
    val last = offsets.length - 1
    cforRange { 0 to last } { i =>
      val fieldEnd = if (i < last) keyOffset(offsets(i + 1)) - 1 else endOffset
      val curOffsetInt = offsets(i)
      val valueOffset = valOffset(curOffsetInt)
      visitor(bytes, keyOffset(curOffsetInt), keyLen(curOffsetInt), valueOffset, fieldEnd - valueOffset)
    }
  }

  // Compares the FIRST key to some bytes
  def firstKeyEquals(bytes: Array[Byte], offsets: Buffer[Int], compareTo: Array[Byte]): Boolean = {
    require(offsets.nonEmpty)
    val firstKeyOffset = keyOffset(offsets(0))
    val firstKeyLen = valOffset(offsets(0)) - 1 - firstKeyOffset
    UnsafeUtils.equate(bytes, UnsafeUtils.arayOffset + firstKeyOffset,
                       compareTo, UnsafeUtils.arayOffset, firstKeyLen)
  }

  def debugKeyValues(bytes: Array[Byte], offsets: Buffer[Int], endOffset: Int): String = {
    val visitor = new StringKVVisitor
    parseKeyValues(bytes, offsets, endOffset, visitor)
    visitor.keyValues.map { case (k, v) => s"\t$k: $v"}.mkString("\n")
  }

  // Parses the Influx timestamp to a millisecond-based Long timestamp, or -1 if we cannot parse
  def parseUnixTime(bytes: Array[Byte], offset: Int, len: Int): Long =
    // Simple trick: just drop the last 6 digits/chars to convert from ns to ms
    parseLong(bytes, offset, len - 6)

  final def parseLong(bytes: Array[Byte], index: Int, len: Int): Long = {
    var curIndex = index
    var longNum: Long = 0L
    while (curIndex < (index + len)) {
      bytes(curIndex) match {
        case b if b >= 0x30 && b <= 0x39 =>
          longNum = longNum * 10 + (b - 0x30)
        case other =>   // not a digit
          return -1
      }
      curIndex += 1
    }
    longNum
  }

  // Quick parsing of doubles directly from bytes.  Skips need to convert to String first and saves that allocation.
  // Falls back to string parsing if needed (for things like NaN, E, etc. which are very small minority)
  final def parseDouble(bytes: Array[Byte], index: Int, len: Int): Double = {
    var curIndex = index
    var dblNum: Double = 0.0
    var fractionMultiplier = 0.0
    while (curIndex < (index + len)) {
      bytes(curIndex) match {
        case b if fractionMultiplier != 0.0 && b >= 0x30 && b <= 0x39 =>
          dblNum += fractionMultiplier * (b - 0x30)
          fractionMultiplier = fractionMultiplier * 0.1
        case b if b >= 0x30 && b <= 0x39 =>
          dblNum = dblNum * 10 + (b - 0x30)
        case '.'   => fractionMultiplier = 0.1
        case other =>   // not a digit. Convert whole thing to string and let Double parser handle it.
          return java.lang.Double.parseDouble(new String(bytes, index, len))
      }
      curIndex += 1
    }
    dblNum
  }
}

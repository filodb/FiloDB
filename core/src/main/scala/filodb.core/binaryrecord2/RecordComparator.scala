package filodb.core.binaryrecord2

import spire.syntax.cfor._

import filodb.memory.format.UnsafeUtils

/**
 * A fast, no-heap-allocation comparator and copier specifically designed for two tasks:
 * 1) Compare an ingestion-based BinaryRecord2 with a partition key only BinaryRecord2
 * 2) Copy an ingestion BinaryRecord2 to a partition key BinaryRecord2
 *
 * One instance of these should exist for each unique ingestSchema.  The methods are to be shared for all BRs.
 *
 * An "ingestion" BinaryRecord2 will have data fields before the partition key fields.
 * A partition key BinaryRecord2 will not have the data fields but have the SAME partition key fields.
 * Thus we can make some optimizations when comparing and copying; namely, for the tags/labels/MapColumn, we can
 * simply compare the bytes (since all Map fields are sorted by key).  In fact we can simply compare the
 * variable field bytes and the fixed field bytes.
 */
final class RecordComparator(ingestSchema: RecordSchema) {
  require(ingestSchema.partitionFieldStart.isDefined)
  require(ingestSchema.columnTypes.length > ingestSchema.partitionFieldStart.get, "no partition fields")

  val partitionKeySchema = new RecordSchema(ingestSchema.columns.drop(ingestSchema.partitionFieldStart.get),
                                            Some(0),
                                            ingestSchema.predefinedKeys)
  // NOTE: remember that private final val results in a Java field, much much faster
  // the ingest BR offset of the first partition fixed area field
  private final val ingestPartOffset = ingestSchema.fieldOffset(ingestSchema.partitionFieldStart.get)

  // the offsets of the variable areas for partition keys (from beginning of BR)
  private final val partVarAreaOffset = partitionKeySchema.variableAreaStart

  // The number of bytes of the partition fields fixed area
  private final val fixedAreaNumBytes = ingestSchema.fieldOffset(ingestSchema.numFields) - ingestPartOffset
  private final val fixedAreaNumWords = fixedAreaNumBytes / 4

  require(fixedAreaNumWords < 32, "Too many partition key fields to use RecordComparator")

  // Make a bitmap for which 32-bit words need to be compared (int, long, bool fields)
  private final val compareBitmap: Int = {
    import filodb.core.metadata.Column.ColumnType._
    var bitmap = 0
    partitionKeySchema.columnTypes.reverse.foreach {
      case IntColumn    => bitmap <<= 1;  bitmap |= 0x01
      case DoubleColumn => bitmap <<= 2;  bitmap |= 0x03
      case LongColumn   => bitmap <<= 2;  bitmap |= 0x03
      case _            => bitmap <<= 1   // both MapColumn and StringColumn use 4-byte fields
    }
    bitmap
  }

  private final val anyPrimitiveFieldsToCompare = compareBitmap != 0

  // Find the fixed area of the first non-primitive (variable area pointer) partition key field
  private val ingestPartNonPrimOffset =
    if (!anyPrimitiveFieldsToCompare) ingestPartOffset else {
      // NOTE: this algorithm fails if the partition key has all primitive fields.  This should not happen.
      var nonPrimOffset = ingestPartOffset
      var wordsBitmap = compareBitmap
      while ((wordsBitmap & 0x01) == 1) { nonPrimOffset += 4; wordsBitmap >>= 1 }
      nonPrimOffset
    }

  // Finds the offset from beginning of ingest record to the var area of first partition field.
  // We have to take into account that data columns might have variable areas too.
  private def ingestVarOffset(ingestBase: Any, ingestOffset: Long): Int =
    UnsafeUtils.getInt(ingestBase, ingestOffset + ingestPartNonPrimOffset)

  /**
   * Returns true if the partition part of the ingest BinaryRecord matches (all partition fields match identically)
   * to the partition key BR.  Uses optimized byte comparisons which is faster than field by field comparison.
   * @param ingestBase the base (null if offheap) of the BinaryRecord built using the ingestSchema
   * @param ingestOffset the offset or native address of the BinaryRecord built using the ingestSchema
   * @param partKeyBase the base (null if offheap) of the BinaryRecord built using partitionKeySchema
   * @param partKeyOffset the offset (null if offheap) of the BinaryRecord built using partitionKeySchema
   */
  final def partitionMatch(ingestBase: Any, ingestOffset: Long, partKeyBase: Any, partKeyOffset: Long): Boolean = {
    // comparison: first get lengths of two BRs
    val ingestNumBytes = UnsafeUtils.getInt(ingestBase, ingestOffset)
    val partKeyNumBytes = UnsafeUtils.getInt(partKeyBase, partKeyOffset)

    // schemaIDs should match
    if (RecordSchema.schemaID(ingestBase, ingestOffset) !=
        RecordSchema.schemaID(partKeyBase, partKeyOffset)) return false

    // compare lengths of variable areas (map tags & strings)
    val ingestVarAreaOffset = ingestVarOffset(ingestBase, ingestOffset)
    val ingestVarSize = ingestNumBytes + 4 - ingestVarAreaOffset
    val partKeyVarSize = partKeyNumBytes + 4 - partVarAreaOffset
    if (ingestVarSize != partKeyVarSize) return false

    // compare variable area bytes
    if (!UnsafeUtils.equate(ingestBase, ingestOffset + ingestVarAreaOffset,
                            partKeyBase, partKeyOffset + partVarAreaOffset,
                            ingestVarSize)) return false

    // finally compare primitive fields if needed
    if (anyPrimitiveFieldsToCompare) {
      var ingestPtr = ingestOffset + ingestPartOffset
      var partKeyPtr = partKeyOffset + partitionKeySchema.fixedStart
      var bitmap = compareBitmap
      while (bitmap != 0) {
        if (((bitmap & 0x01) == 1) &&
            UnsafeUtils.getInt(ingestBase, ingestPtr) != UnsafeUtils.getInt(partKeyBase, partKeyPtr)) return false
        ingestPtr += 4   // advance one 32-bit word at a time
        partKeyPtr += 4
        bitmap >>= 1
      }
    }
    return true
  }

  /**
   * Efficiently builds a partition key BinaryRecord from an ingest BinaryRecord.  This is done by copying the
   * variable and fixed area bytes directly, and adjusting the offsets.
   * @param ingestBase the base (null if offheap) of the BinaryRecord built using the ingestSchema
   * @param ingestOffset the offset or native address of the BinaryRecord built using the ingestSchema
   * @param builder a RecordBuilder which uses the partitionKeySchema
   * @return the Long offset or native address of the new partition key BR
   */
  final def buildPartKeyFromIngest(ingestBase: Any, ingestOffset: Long, builder: RecordBuilder): Long = {
    builder.setSchema(partitionKeySchema)

    // Copy fixed area + hash over, then variable areas
    val ingestNumBytes = UnsafeUtils.getInt(ingestBase, ingestOffset)
    val ingestVarAreaOffset = ingestVarOffset(ingestBase, ingestOffset)
    builder.copyFixedAreasFrom(ingestBase, ingestOffset, ingestPartOffset, fixedAreaNumBytes + 4)
    builder.copyVarAreasFrom(ingestBase, ingestOffset + ingestVarAreaOffset, ingestNumBytes + 4 - ingestVarAreaOffset)

    // adjust offsets to var fields
    val adjustment = partVarAreaOffset - ingestVarAreaOffset
    cforRange { 0 until fixedAreaNumWords } { i =>
      if ((compareBitmap & (1 << i)) == 0) {    // not a primitive field, but an offset to String or Map
        builder.adjustFieldOffset(i, adjustment)
      }
    }

    builder.endRecord(writeHash = false)
  }
}

package filodb.core.binaryrecord2

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.StrictLogging
import org.agrona.DirectBuffer
import spire.syntax.cfor.cforRange

import filodb.core.binaryrecord2.RecordBuilder.{combineHash, HASH_INIT}
import filodb.core.metadata.{Column, PartitionSchema, Schema}
import filodb.core.metadata.Column.ColumnType._
import filodb.core.query.ColumnInfo
import filodb.memory.{BinaryRegion, BinaryRegionLarge, UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{RowReader, SeqRowReader, UnsafeUtils, ZeroCopyUTF8String => ZCUTF8}
import filodb.memory.format.UnsafeUtils._
import filodb.memory.format.vectors.Histogram

abstract class RecordBuilderBase extends StrictLogging {

  var schema: RecordSchema = _

  protected var curBase: Any = UnsafeUtils.ZeroPointer
  protected var fieldNo: Int = -1
  protected var curRecordOffset: Long = -1L
  protected var curRecEndOffset: Long = -1L
  protected var maxOffset: Long = -1L
  protected var mapOffset: Long = -1L
  protected var recHash: Int = -1

  var firstPartField = Int.MaxValue
  private var hashOffset: Int = 0

  private def updateFieldPointerAndLens(varFieldLen: Int): Unit = {
    // update fixed field area, which is a 4-byte offset to the var field
    setInt(curBase, curRecordOffset + schema.fieldOffset(fieldNo), (curRecEndOffset - curRecordOffset).toInt)
    curRecEndOffset += varFieldLen

    // update BinaryRecord length header as well
    setInt(curBase, curRecordOffset, (curRecEndOffset - curRecordOffset).toInt - 4)
  }

  /**
   * Called when current writing position is close to the end of the current container, and more bytes are
   * needed to continue finishing current record. Implementation should allocate a new memory region,
   * and set the offset and maxOffset to the new region. It should also copy the partially written record from the old
   * region to the new region, so that the caller can continue writing the record.  If the new region cannot fit
   * the partially written record and the new data, then an exception should be thrown.
   * @param numBytes
   */
  protected def requireBytes(numBytes: Int): Unit

  private def checkFieldAndMemory(bytesRequired: Int): Unit = {
    checkFieldNo()
    requireBytes(bytesRequired)
  }


  // startNewRecord when one uses a RecordSchema for say query results, or where schemaID is not needed.
  final def startNewRecord(schema: RecordSchema): Unit = {
    // TODO: use types to eliminate this check?
    require(schema.partitionFieldStart.isEmpty, s"Cannot use schema $schema with no schemaID")
    startNewRecord(schema, 0)
  }

  // startNewRecord for an ingestion schema.  Use this if creating an ingestion record, ensures right ID is used.
  final def startNewRecord(schema: Schema): Unit =
    startNewRecord(schema.ingestionSchema, schema.schemaHash)

  final def startNewRecord(partSchema: PartitionSchema, schemaID: Int): Unit =
    startNewRecord(partSchema.binSchema, schemaID)

  /**
   * Start building a new BinaryRecord with a possibly new schema.
   * This must be called after a previous endRecord() or when the builder just started.
   * NOTE: it's probably better to use an alternative startNewRecord with one of the schema types.
   * @param recSchema the RecordSchema to use for this record
   * @param schemaID the schemaID to use.  It may not be the same as the schema of the recSchema - for example
   *        for partition keys.  However for ingestion records it would be the same.
   */
  private[core] final def startNewRecord(recSchema: RecordSchema, schemaID: Int): Unit = {
    checkPointers()

    // Set schema, hashoffset, and write schema ID if needed
    setSchema(recSchema)
    requireBytes(schema.variableAreaStart)

    if (recSchema.partitionFieldStart.isDefined) { setShort(curBase, curRecordOffset + 4, schemaID.toShort) }

    // write length header and update RecEndOffset
    setInt(curBase, curRecordOffset, schema.variableAreaStart - 4)
    curRecEndOffset = curRecordOffset + schema.variableAreaStart

    fieldNo = 0
    recHash = HASH_INIT
  }

  private[binaryrecord2] def setSchema(newSchema: RecordSchema): Unit = if (newSchema != schema) {
    schema = newSchema
    hashOffset = newSchema.fieldOffset(newSchema.numFields)
    firstPartField = schema.partitionFieldStart.getOrElse(Int.MaxValue)
  }


  /**
   * If somehow the state is inconsistent, and only a partial record is written,
   * rewind the curRecordOffset back to the curRecEndOffset.  In other words, rewind the write pointer
   * back to the end of previous record.  Partially written data is lost, but state is consistent again.
   */
  def rewind(): Unit = {
    curRecEndOffset = curRecordOffset
  }

  // Check that we are at end of a record.  If a partial record is written, just rewind so state is not inconsistent.
  private def checkPointers(): Unit = {
    if (curRecEndOffset != curRecordOffset) {
      logger.warn(s"Partial record was written, perhaps exception occurred.  Rewinding to end of previous record.")
      rewind()
    }
  }


  protected def checkFieldNo(): Unit = require(fieldNo >= 0 && fieldNo < schema.numFields)

  /**
   * Adds an integer to the record.  This must be called in the right order or the data might be corrupted.
   * Also this must be called between startNewRecord and endRecord.
   * Calling this method after all fields of a record has been filled will lead to an error.
   */
  final def addInt(data: Int): Unit = {
    checkFieldNo()
    setInt(curBase, curRecordOffset + schema.fieldOffset(fieldNo), data)
    fieldNo += 1
  }

  /**
   * Adds a Long to the record.  This must be called in the right order or the data might be corrupted.
   * Also this must be called between startNewRecord and endRecord.
   * Calling this method after all fields of a record has been filled will lead to an error.
   */
  final def addLong(data: Long): Unit = {
    checkFieldNo()
    setLong(curBase, curRecordOffset + schema.fieldOffset(fieldNo), data)
    fieldNo += 1
  }

  /**
   * Adds a Double to the record.  This must be called in the right order or the data might be corrupted.
   * Also this must be called between startNewRecord and endRecord.
   * Calling this method after all fields of a record has been filled will lead to an error.
   */
  final def addDouble(data: Double): Unit = {
    checkFieldNo()
    setDouble(curBase, curRecordOffset + schema.fieldOffset(fieldNo), data)
    fieldNo += 1
  }

  /**
   * Adds a string or raw bytes to the record.  They must fit in within 64KB.
   * The variable length area of the BinaryRecord will be extended.
   */
  final def addString(bytes: Array[Byte]): Unit =
    addBlob(bytes, UnsafeUtils.arayOffset, bytes.size)

  final def addString(s: String): Unit = addString(s.getBytes(StandardCharsets.UTF_8))

  final def addBlob(base: Any, offset: Long, numBytes: Int): Unit = {
    require(numBytes < 65536, s"bytes too large ($numBytes bytes) for addBlob")
    checkFieldAndMemory(numBytes + 2)
    UnsafeUtils.setShort(curBase, curRecEndOffset, numBytes.toShort) // length of blob
    UnsafeUtils.unsafe.copyMemory(base, offset, curBase, curRecEndOffset + 2, numBytes)
    updateFieldPointerAndLens(numBytes + 2)
    if (fieldNo >= firstPartField) recHash = combineHash(recHash, BinaryRegion.hash32(base, offset, numBytes))
    fieldNo += 1
  }

  final def addBlob(strPtr: ZCUTF8): Unit = addBlob(strPtr.base, strPtr.offset, strPtr.numBytes)

  // Adds a blob from another buffer which already has the length bytes as the first two bytes
  // For example: buffers created by BinaryHistograms.  OR, a UTF8String medium.
  final def addBlob(buf: DirectBuffer): Unit = {
    val numBytes = buf.getShort(0).toInt
    require(numBytes < buf.capacity)
    addBlob(buf.byteArray, buf.addressOffset + 2, numBytes)
  }

  /**
   * Adds fields of a Partition Key Binary Record into the record builder as column values in
   * the same order. Typically used for the downsampling use case where we copy partition key from
   * the TimeSeriesPartition into the ingest record for the downsample data.
   *
   * This also updates the hash for this record. OK since partKeys are added at the very end
   * of the record.
   */
  final def addPartKeyRecordFields(base: Any, offset: Long, partKeySchema: RecordSchema): Unit = {
    var id = 0
    partKeySchema.columns.foreach {
      case ColumnInfo(_, MapColumn, _, _) => addBlobFromBr(base, offset, id, partKeySchema); id += 1
      case ColumnInfo(_, StringColumn, _, _) => addBlobFromBr(base, offset, id, partKeySchema); id += 1
      case ColumnInfo(_, LongColumn, _, _) => addLongFromBr(base, offset, id, partKeySchema); id += 1
      case ColumnInfo(_, DoubleColumn, _, _) => addDoubleFromBr(base, offset, id, partKeySchema); id += 1
      case _ => ???
    }
    // finally copy the partition hash over
    recHash = partKeySchema.partitionHash(base, offset)
  }

  private def addBlobFromBr(base: Any, offset: Long, col: Int, schema: RecordSchema): Unit = {
    val blobDataOffset = schema.blobOffset(base, offset, col)
    val blobNumBytes = schema.blobNumBytes(base, offset, col)
    addBlob(base, blobDataOffset, blobNumBytes)
  }

  /**
   * IMPORTANT: Internal method, does not update hash values for the data.
   * If this method is used, then caller needs to also update the partitionHash manually.
   */
  private def addLargeBlobFromBr(base: Any, offset: Long, col: Int, schema: RecordSchema): Unit = {
    val strDataOffset = schema.utf8StringOffset(base, offset, col)
    addMap(base, strDataOffset + 4, BinaryRegionLarge.numBytes(base, strDataOffset))
  }

  private def addLongFromBr(base: Any, offset: Long, col: Int, schema: RecordSchema): Unit = {
    addLong(schema.getLong(base, offset, col))
  }

  private def addDoubleFromBr(base: Any, offset: Long, col: Int, schema: RecordSchema): Unit = {
    addDouble(schema.getDouble(base, offset, col))
  }

  /**
   * A SLOW but FLEXIBLE method to add data to the current field.  Boxes for sure but can take any data.
   * Relies on passing in an object (Any) and using match, lots of allocations here.
   * PLEASE don't use it in high performance code / hot paths.  Meant for ease of testing.
   */
  def addSlowly(item: Any): Unit = {
    (schema.columnTypes(fieldNo), item) match {
      case (IntColumn, i: Int) => addInt(i)
      case (LongColumn, l: Long) => addLong(l)
      case (TimestampColumn, l: Long) => addLong(l)
      case (DoubleColumn, d: Double) => addDouble(d)
      case (StringColumn, s: String) => addString(s)
      case (StringColumn, a: Array[Byte]) => addString(a)
      case (StringColumn, z: ZCUTF8) => addBlob(z)
      case (MapColumn, m: Map[ZCUTF8, ZCUTF8] @unchecked) => addMap(m)
      case (HistogramColumn, h: Histogram) => addBlob(h.serialize())
      case (other: Column.ColumnType, v) =>
        throw new UnsupportedOperationException(s"Column type of $other and value of class ${v.getClass}")
    }
  }

  /**
   * Adds an entire record from a BinaryRecord, with no boxing or allocations. The record must have the same
   * schema as the current one.
   */
  final def addFromBr(base: Any, offset: Long): Long = {
    checkPointers()
    val numBytes = UnsafeUtils.getInt(base, offset)
    requireBytes(numBytes + 4)
    val recordOffset = curRecordOffset
    UnsafeUtils.unsafe.copyMemory(base, offset, curBase, curRecEndOffset, numBytes + 4)
    curRecEndOffset = align(curRecEndOffset + numBytes + 4)
    curRecordOffset = curRecEndOffset
    fieldNo = -1
    postEndRecord()
    recordOffset
  }

  /**
   * Adds an entire record from a RowReader, with no boxing, using builderAdders
   *
   * @return the offset or NativePointer if the memFactory is an offheap one, to the new BinaryRecord
   */
  final def addFromReader(row: RowReader, schema: RecordSchema, schemID: Int): Long = {
    startNewRecord(schema, schemID)
    cforRange {
      0 until schema.numFields
    } { pos =>
      schema.builderAdders(pos)(row, this)
    }
    endRecord()
  }

  final def addFromReader(row: RowReader, schema: Schema): Long =
    addFromReader(row, schema.ingestionSchema, schema.schemaHash)

  // Really only for testing. Very slow.  Only for partition keys
  def partKeyFromObjects(schema: Schema, parts: Any*): Long =
    addFromReader(SeqRowReader(parts.toSeq), schema.partKeySchema, schema.schemaHash)

  /**
   * IMPORTANT: Internal method, does not update hash values for the map key/values individually.
   * If this method is used, then caller needs to also update the partitionHash manually.
   */
  private def addMap(base: Any, offset: Long, numBytes: Int): Unit = {
    require(numBytes < 65536, s"bytes too large ($numBytes bytes) for addMap")
    checkFieldAndMemory(numBytes + 2)
    UnsafeUtils.setShort(curBase, curRecEndOffset, numBytes.toShort) // length of blob
    UnsafeUtils.unsafe.copyMemory(base, offset, curBase, curRecEndOffset + 2, numBytes)
    updateFieldPointerAndLens(numBytes + 2)
    fieldNo += 1
  }

  /**
   * Sorts and adds keys and values from a map.  The easiest way to add a map to a BinaryRecord.
   */
  def addMap(map: Map[ZCUTF8, ZCUTF8]): Unit = {
    startMap()
    map.toSeq.sortBy(_._1).foreach { case (k, v) =>
      addMapKeyValue(k.bytes, v.bytes)
    }
    endMap()
  }

  /**
   * Encodes a Java TreeMap directly into the map field without Scala collection conversion.
   * TreeMap is already sorted by key, so no re-sorting needed.
   *
   * This avoids the overhead of:
   *   - convertToScalaImmutableMap (Stream, Tuple2, HashMap construction)
   *   - Scala Map.toSeq.sortBy (TreeMap is already sorted)
   *   - ZeroCopyUTF8String wrapping
   *
   * Produces the same wire format as addMap(Map[ZCUTF8, ZCUTF8]) — identical bytes,
   * same predefined key handling, same partition hash.
   *
   * Usage from Java:
   * TreeMap<String, String> tags = new TreeMap<>();
   * tags.put("_ns_", "myapp");
   * tags.put("_ws_", "demo");
   * builder.startNewRecord(schema);
   * // ... add other fields ...
   * builder.encodeMapFrom(tags);
   * builder.endRecord();
   */
  final def encodeMapFrom(tags: java.util.TreeMap[String, String]): Unit = {
    startMap()
    val iter = tags.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val keyBytes = entry.getKey.getBytes(StandardCharsets.UTF_8)
      val valBytes = entry.getValue.getBytes(StandardCharsets.UTF_8)
      addMapKeyValue(keyBytes, 0, keyBytes.length, valBytes, 0, valBytes.length)
    }
    endMap()
  }

  final def updatePartitionHash(newHash: Int): Unit = {
    recHash = combineHash(recHash, newHash)
  }

  /**
   * Low-level function to start adding a map field.  Must be followed by addMapKeyValue() in sorted order of
   * keys (UTF8 byte sort).  Might want to use one of the higher level functions.
   */
  final def startMap(): Unit = {
    require(mapOffset == -1L)
    checkFieldAndMemory(2) // 2 bytes for map length header
    mapOffset = curRecEndOffset
    setShort(curBase, mapOffset, 0)
    updateFieldPointerAndLens(2)
    // Don't update fieldNo, we'll be working on map for a while
  }

  /**
   * Use key which is a UTF8StringShort, and value which is a UTF8StringMedium, to add a
   * key-value pair to the map field started by startMap().
   * This method is used to serialize RangeVectorKeys in arrow flight RPC path
   *
   * IMPORTANT: The encoding does not use pre-defined keys since this can
   * travel across partitions which can potentially have different pre-defined
   * keys during deployment roll outs.
   *
   * Hash is also not computed
   */
  final def addMapKeyValue(keyBase: Any, keyOffset: Long,
                           valueBase: Any, valueOffset: Long): Unit = {
    require(mapOffset > curRecordOffset, "illegal state, did you call startMap() first?")
    val keyLen = UTF8StringShort.numBytes(keyBase, keyOffset)
    val valLen = UTF8StringMedium.numBytes(valueBase, valueOffset)
    require(keyLen < 192, s"key is too large: $keyLen bytes")
    require(valLen < 64 * 1024, s"value is too large: $valLen bytes")
    requireBytes(keyLen + valLen + 3)
    UnsafeUtils.unsafe.copyMemory(keyBase, keyOffset, curBase, curRecEndOffset, keyLen + 1)
    curRecEndOffset += keyLen + 1
    UnsafeUtils.unsafe.copyMemory(valueBase, valueOffset, curBase, curRecEndOffset, valLen + 2)
    curRecEndOffset += valLen + 2
    val newMapLen = curRecEndOffset - mapOffset - 2
    require(newMapLen < 65536, s"Map entries cannot total more than 64KB, but is now $newMapLen")
    setShort(curBase, mapOffset, newMapLen.toShort)
    setInt(curBase, curRecordOffset, (curRecEndOffset - curRecordOffset - 4).toInt)
  }

  /**
   * Use key which is a ZCUTF8, and value which is a UTF8StringMedium, to add a
   * key-value pair to the map field started by startMap().
   * This method is used to serialize RangeVectorKeys in arrow flight RPC path
   *
   * IMPORTANT: The encoding does not use pre-defined keys since this can
   * travel across partitions which can potentially have different pre-defined
   * keys during deployment roll outs.
   *
   * Hash is also not computed
   */
  final def addMapKeyValue(key: ZCUTF8,
                           valueBase: Any, valueOffset: Long): Unit = {
    require(mapOffset > curRecordOffset, "illegal state, did you call startMap() first?")
    val keyLen = key.numBytes
    val valLen = UTF8StringMedium.numBytes(valueBase, valueOffset)
    require(keyLen < 192, s"key is too large: $keyLen bytes")
    require(valLen < 64 * 1024, s"value is too large: $valLen bytes")
    requireBytes(keyLen + valLen + 3)
    UnsafeUtils.setByte(curBase, curRecEndOffset, keyLen.toByte) // write key length as 1 byte for UTF8StringShort
    curRecEndOffset += 1
    UnsafeUtils.unsafe.copyMemory(key.base, key.offset, curBase, curRecEndOffset, keyLen)
    curRecEndOffset += keyLen
    UnsafeUtils.unsafe.copyMemory(valueBase, valueOffset, curBase, curRecEndOffset, valLen + 2)
    curRecEndOffset += valLen + 2
    val newMapLen = curRecEndOffset - mapOffset - 2
    require(newMapLen < 65536, s"Map entries cannot total more than 64KB, but is now $newMapLen")
    setShort(curBase, mapOffset, newMapLen.toShort)
    setInt(curBase, curRecordOffset, (curRecEndOffset - curRecordOffset - 4).toInt)
  }

  /**
   * Adds a single key-value pair to the map field started by startMap().
   * Takes care of matching and translating predefined keys into short codes.
   * Keys must be < 60KB and values must be < 64KB
   * Hash is not computed or added for you - it must be separately added by you!
   *
   * IMPORTANT: MapEncoder.encode() replicates this encoding logic. If the wire format
   * changes here (predefined key codes, UTF8StringShort/Medium encoding, byte order),
   * MapEncoder must be updated to match.
   */
  final def addMapKeyValue(keyBytes: Array[Byte], keyOffset: Int, keyLen: Int,
                           valueBytes: Array[Byte], valueOffset: Int, valueLen: Int,
                           keyHash: Int = 7): Unit = {
    require(mapOffset > curRecordOffset, "illegal state, did you call startMap() first?")
    // check key size, must be < 60KB
    require(keyLen < 192, s"key is too large: ${keyLen} bytes")
    require(valueLen < 64 * 1024, s"value is too large: $valueLen bytes")

    // Check if key is a predefined key
    val predefKeyNum = // but if there are no predefined keys, skip the cost of hashing the key
      if (schema.predefinedKeys.isEmpty) {
        -1
      }
      else {
        val keyKey = RecordSchema.makeKeyKey(keyBytes, keyOffset, keyLen, keyHash)
        schema.predefKeyNumMap.getOrElse(keyKey, -1)
      }
    val keyValueSize = if (predefKeyNum >= 0) {
      valueLen + 3
    } else {
      keyLen + valueLen + 3
    }
    requireBytes(keyValueSize)
    if (predefKeyNum >= 0) {
      setByte(curBase, curRecEndOffset, (0x0C0 | predefKeyNum).toByte)
      curRecEndOffset += 1
    } else {
      UTF8StringShort.copyByteArrayTo(keyBytes, keyOffset, keyLen, curBase, curRecEndOffset)
      curRecEndOffset += keyLen + 1
    }
    UTF8StringMedium.copyByteArrayTo(valueBytes, valueOffset, valueLen, curBase, curRecEndOffset)
    curRecEndOffset += valueLen + 2

    // update map length, BR length
    val newMapLen = curRecEndOffset - mapOffset - 2
    require(newMapLen < 65536, s"Map entries cannot total more than 64KB, but is now $newMapLen")
    setShort(curBase, mapOffset, newMapLen.toShort)
    setInt(curBase, curRecordOffset, (curRecEndOffset - curRecordOffset - 4).toInt)
  }

  final def addMapKeyValue(key: Array[Byte], value: Array[Byte]): Unit =
    addMapKeyValue(key, 0, key.size, value, 0, value.size)

  /**
   * An alternative to above for adding a known key with precomputed key hash
   * along with a value, to the map, while updating the hash too.
   * Saves computing the key hash twice.
   * TODO: deprecate this.  We are switching to computing a hash for all keys at the same time.
   */
  final def addMapKeyValueHash(keyBytes: Array[Byte], keyHash: Int,
                               valueBytes: Array[Byte], valueOffset: Int, valueLen: Int): Unit = {
    addMapKeyValue(keyBytes, 0, keyBytes.size, valueBytes, valueOffset, valueLen, keyHash)
    val valueHash = BinaryRegion.hasher32.hash(valueBytes, valueOffset, valueLen, BinaryRegion.Seed)
    updatePartitionHash(combineHash(keyHash, valueHash))
  }

  /**
   * Ends creation of a map field.  Recompute the hash for all fields at once.
   *
   * @param bulkHash if true (default), computes the hash for all key/values.
   *                 Some users use the older alternate, sortAndComputeHashes() - then set this to false.
   */
  final def endMap(bulkHash: Boolean = true): Unit = {
    if (bulkHash) {
      val mapHash = BinaryRegion.hash32(curBase, mapOffset, (curRecEndOffset - mapOffset).toInt)
      updatePartitionHash(mapHash)
    }
    mapOffset = -1L
    fieldNo += 1
  }

  /**
   * Adds an encoded map field directly from a byte array already in RecordBuilder wire format.
   * Use MapEncoder.encode() to produce correctly encoded bytes.
   */
  final def addEncodedMap(mapBytes: Array[Byte]): Unit = {
    val numBytes = mapBytes.length
    require(numBytes < 65536, s"Map bytes too large: $numBytes")
    startMap()
    requireBytes(numBytes)
    UnsafeUtils.unsafe.copyMemory(mapBytes, UnsafeUtils.arayOffset,
      curBase, curRecEndOffset, numBytes)
    curRecEndOffset += numBytes
    // update map length and record length
    setShort(curBase, mapOffset, numBytes.toShort)
    setInt(curBase, curRecordOffset, (curRecEndOffset - curRecordOffset - 4).toInt)
    endMap()
  }
  final def align(offset: Long): Long = (offset + 3) & ~3

  /**
   * Ends the building of the current BinaryRecord.  Makes sures RecordContainer state is updated.
   * Aligns the next record on a 4-byte/short word boundary.
   * Returns the Long offset of the just finished BinaryRecord.  If the container is offheap, then this is the
   * full NativePointer.  If it is onHeap, you will need to access the current container and get the base
   * to form the (base, offset) pair needed to access the BinaryRecord.
   */
  final def endRecord(writeHash: Boolean = true): Long = {
    val recordOffset = curRecordOffset

    if (writeHash && firstPartField < Int.MaxValue) setInt(curBase, curRecordOffset + hashOffset, recHash)

    // Bring RecordOffset up to endOffset w/ align.  Now the state is complete at end of a record again.
    curRecEndOffset = align(curRecEndOffset)
    curRecordOffset = curRecEndOffset
    fieldNo = -1

    postEndRecord()
    recordOffset
  }

  def postEndRecord(): Unit
}

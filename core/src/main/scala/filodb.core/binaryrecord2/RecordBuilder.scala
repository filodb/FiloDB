package filodb.core.binaryrecord2

import com.typesafe.scalalogging.StrictLogging
import org.agrona.concurrent.UnsafeBuffer
import scalaxy.loops._

import filodb.core.metadata.{Column, Dataset}
import filodb.core.metadata.Column.ColumnType.{DoubleColumn, LongColumn, MapColumn, StringColumn}
import filodb.core.query.ColumnInfo
import filodb.memory._
import filodb.memory.format.{RowReader, SeqRowReader, UnsafeUtils, ZeroCopyUTF8String => ZCUTF8}
import filodb.memory.format.vectors.Histogram


// scalastyle:off number.of.methods
/**
 * A RecordBuilder allocates fixed size containers and builds BinaryRecords within them.
 * The size of the container should be much larger than the average size of a record for efficiency.
 * Many BinaryRecords are built within one container.
 * This is very much a mutable, stateful class and should be run within a single thread or stream of execution.
 * It is NOT multi-thread safe.
 * The idea is to use one RecordBuilder per context/stream/thread. The context should make sense; as the list of
 * containers can then be 1) sent over the wire, with no further transformations needed, 2) obtained and maybe freed
 *
 * @param memFactory the MemFactory used to allocate containers for building BinaryRecords in
 * @param schema the RecordSchema for the BinaryRecord to build
 * @param containerSize the size of each container
 * @param reuseOneContainer if true, resets the container when we run out of container space.  Designed for scenario
 *                   where one copies the BinaryRecord somewhere else every time, and allocation is minimized by
 *                   reusing the same container over and over.
 */
final class RecordBuilder(memFactory: MemFactory,
                          val schema: RecordSchema,
                          containerSize: Int = RecordBuilder.DefaultContainerSize,
                          reuseOneContainer: Boolean = false,
                          // Put fields here so scalac won't generate stupid and slow initialization check logic
                          // Seriously this makes var updates like twice as fast
                          private var curBase: Any = UnsafeUtils.ZeroPointer,
                          private var fieldNo: Int = -1,
                          private var curRecordOffset: Long = -1L,
                          private var curRecEndOffset: Long = -1L,
                          private var maxOffset: Long = -1L,
                          private var mapOffset: Long = -1L,
                          private var recHash: Int = -1) extends StrictLogging {
  import RecordBuilder._
  import UnsafeUtils._
  require(containerSize >= RecordBuilder.MinContainerSize, s"RecordBuilder.containerSize < minimum")

  private val containers = new collection.mutable.ArrayBuffer[RecordContainer]
  private val firstPartField = schema.partitionFieldStart.getOrElse(Int.MaxValue)
  private val hashOffset = schema.fieldOffset(schema.numFields)

  if (reuseOneContainer) newContainer()

  // Reset last container and all pointers
  def reset(): Unit = if (containers.nonEmpty) {
    resetContainerPointers()
    fieldNo = -1
    mapOffset = -1L
    recHash = -1
  }

  // Only reset the container offsets, but not the fieldNo, mapOffset, recHash
  private def resetContainerPointers(): Unit = {
    curRecordOffset = containers.last.offset + ContainerHeaderLen
    curRecEndOffset = curRecordOffset
    containers.last.updateLengthWithOffset(curRecordOffset)
    curBase = containers.last.base
  }

  /**
   * Start building a new BinaryRecord.
   * This must be called after a previous endRecord() or when the builder just started.
   */
  final def startNewRecord(): Unit = {
    require(curRecEndOffset == curRecordOffset, s"Illegal state: $curRecEndOffset != $curRecordOffset")
    requireBytes(schema.variableAreaStart)

    // write length header and update RecEndOffset
    setInt(curBase, curRecordOffset, schema.variableAreaStart - 4)
    curRecEndOffset = curRecordOffset + schema.variableAreaStart

    fieldNo = 0
    recHash = HASH_INIT
  }

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

  final def addString(s: String): Unit = addString(s.getBytes)

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
  final def addBlob(buf: UnsafeBuffer): Unit = {
    val numBytes = buf.getShort(0).toInt
    require(numBytes < buf.capacity)
    addBlob(buf.byteArray, buf.addressOffset + 2, numBytes)
  }

  /**
    * IMPORTANT: Internal method, does not update hash values for the map key/values individually.
    * If this method is used, then caller needs to also update the partitionHash manually.
    */
  private def addMap(base: Any, offset: Long, numBytes: Int): Unit = {
    require(numBytes < 65536, s"bytes too large ($numBytes bytes) for addBlob")
    checkFieldAndMemory(numBytes + 4)
    UnsafeUtils.setInt(curBase, curRecEndOffset, numBytes) // length of blob
    UnsafeUtils.unsafe.copyMemory(base, offset, curBase, curRecEndOffset + 4, numBytes)
    updateFieldPointerAndLens(numBytes + 4)
    fieldNo += 1
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
      case ColumnInfo(_, MapColumn) => addLargeBlobFromBr(base, offset, id, partKeySchema); id += 1
      case ColumnInfo(_, StringColumn) => addBlobFromBr(base, offset, id, partKeySchema); id += 1
      case ColumnInfo(_, LongColumn) => addLongFromBr(base, offset, id, partKeySchema); id += 1
      case ColumnInfo(_, DoubleColumn) => addDoubleFromBr(base, offset, id, partKeySchema); id += 1
      case _ => ???
    }
    // finally copy the partition hash over
    recHash = partKeySchema.partitionHash(base, offset)
  }

  import Column.ColumnType._

  /**
   * A SLOW but FLEXIBLE method to add data to the current field.  Boxes for sure but can take any data.
   * Relies on passing in an object (Any) and using match, lots of allocations here.
   * PLEASE don't use it in high performance code / hot paths.  Meant for ease of testing.
   */
  def addSlowly(item: Any): Unit = {
    (schema.columnTypes(fieldNo), item) match {
      case (IntColumn, i: Int)       => addInt(i)
      case (LongColumn, l: Long)     => addLong(l)
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
   * Adds an entire record from a RowReader, with no boxing, using builderAdders
   * @return the offset or NativePointer if the memFactory is an offheap one, to the new BinaryRecord
   */
  final def addFromReader(row: RowReader): Long = {
    startNewRecord()
    for { pos <- 0 until schema.numFields optimized } {
      schema.builderAdders(pos)(row, this)
    }
    endRecord()
  }

  // Really only for testing. Very slow.
  def addFromObjects(parts: Any*): Long = addFromReader(SeqRowReader(parts.toSeq))

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

  final def updatePartitionHash(newHash: Int): Unit = {
    recHash = combineHash(recHash, newHash)
  }

  /**
   * Low-level function to start adding a map field.  Must be followed by addMapKeyValue() in sorted order of
   * keys (UTF8 byte sort).  Might want to use one of the higher level functions.
   */
  final def startMap(): Unit = {
    require(mapOffset == -1L)
    checkFieldAndMemory(4)   // 4 bytes for map length header
    mapOffset = curRecEndOffset
    setInt(curBase, mapOffset, 0)
    updateFieldPointerAndLens(4)
    // Don't update fieldNo, we'll be working on map for a while
  }

  /**
   * Adds a single key-value pair to the map field started by startMap().
   * Takes care of matching and translating predefined keys into short codes.
   * Keys must be < 60KB and values must be < 64KB
   * Hash is not computed or added for you - it must be separately added by you!
   */
  final def addMapKeyValue(keyBytes: Array[Byte], keyOffset: Int, keyLen: Int,
                           valueBytes: Array[Byte], valueOffset: Int, valueLen: Int,
                           keyHash: Int = 7): Unit = {
    require(mapOffset > curRecordOffset, "illegal state, did you call startMap() first?")
    // check key size, must be < 60KB
    require(keyLen < 60*1024, s"key is too large: ${keyLen} bytes")
    require(valueLen < 64*1024, s"value is too large: $valueLen bytes")

    // Check if key is a predefined key
    val predefKeyNum =  // but if there are no predefined keys, skip the cost of hashing the key
      if (schema.predefinedKeys.isEmpty) { -1 }
      else {
        val keyKey = RecordSchema.makeKeyKey(keyBytes, keyOffset, keyLen, keyHash)
        schema.predefKeyNumMap.getOrElse(keyKey, -1)
      }
    val keyValueSize = if (predefKeyNum >= 0) { valueLen + 4 } else { keyLen + valueLen + 4 }
    requireBytes(keyValueSize)
    if (predefKeyNum >= 0) {
      setShort(curBase, curRecEndOffset, (0xF000 | predefKeyNum).toShort)
      curRecEndOffset += 2
    } else {
      UTF8StringMedium.copyByteArrayTo(keyBytes, keyOffset, keyLen, curBase, curRecEndOffset)
      curRecEndOffset += keyLen + 2
    }
    UTF8StringMedium.copyByteArrayTo(valueBytes, valueOffset, valueLen, curBase, curRecEndOffset)
    curRecEndOffset += valueLen + 2

    // update map length, BR length
    setInt(curBase, mapOffset, (curRecEndOffset - mapOffset - 4).toInt)
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
   */
  final def endMap(): Unit = {
    val mapHash = BinaryRegion.hash32(curBase, mapOffset, (curRecEndOffset - mapOffset).toInt)
    updatePartitionHash(mapHash)
    mapOffset = -1L
    fieldNo += 1
  }

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

    // Update container length.  This is atomic so it is updated only when the record is complete.
    val lastContainer = containers.last
    lastContainer.updateLengthWithOffset(curRecEndOffset)
    lastContainer.numRecords += 1

    recordOffset
  }

  final def align(offset: Long): Long = (offset + 3) & ~3

  /**
   * Used only internally by RecordComparator etc. to shortcut create a new BR by copying bytes from an existing BR.
   * You BETTER know what you are doing.
   */
  private[binaryrecord2] def copyFixedAreasFrom(base: Any, offset: Long, numBytes: Int): Unit = {
    require(curRecEndOffset == curRecordOffset, s"Illegal state: $curRecEndOffset != $curRecordOffset")
    requireBytes(numBytes + 4)

    // write length header, copy bytes, and update RecEndOffset
    setInt(curBase, curRecordOffset, numBytes)
    UnsafeUtils.unsafe.copyMemory(base, offset, curBase, curRecordOffset + 4, numBytes)
    curRecEndOffset = curRecordOffset + numBytes + 4
  }

  // Extend current variable area with stuff from somewhere else
  private[binaryrecord2] def copyVarAreasFrom(base: Any, offset: Long, numBytes: Int): Unit = {
    requireBytes(numBytes)
    UnsafeUtils.unsafe.copyMemory(base, offset, curBase, curRecEndOffset, numBytes)
    // Increase length of current BR.  Then bump curRecEndOffset so we are consistent
    setInt(curBase, curRecordOffset, getInt(curBase, curRecordOffset) + numBytes)
    curRecEndOffset += numBytes
  }

  private[binaryrecord2] def adjustFieldOffset(fieldNo: Int, adjustment: Int): Unit = {
    val offset = curRecordOffset + schema.fieldOffset(fieldNo)
    UnsafeUtils.setInt(curBase, offset, UnsafeUtils.getInt(curBase, offset) + adjustment)
  }

  // resets or empties current container.  Only used for testing.  Also ensures there's at least one container
  private[filodb] def resetCurrent(): Unit = {
    if (containers.isEmpty) requireBytes(100)
    curBase = currentContainer.get.base
    curRecordOffset = currentContainer.get.offset + 4
    currentContainer.get.updateLengthWithOffset(curRecordOffset)
    curRecEndOffset = curRecordOffset
  }

  /**
   * Returns Some(container) reference to the current RecordContainer or None if there is no container
   */
  def currentContainer: Option[RecordContainer] = containers.lastOption

  /**
   * Returns the list of all current containers
   */
  def allContainers: Seq[RecordContainer] = containers


  /**
    * Returns all the full containers other than currentContainer as byte arrays.
    * Assuming all of the containers except the last one is full,
    * calls array() on all the non-last container excluding the currentContainer.
    * The memFactory needs to be an on heap one otherwise UnsupportedOperationException will be thrown.
    * The sequence of byte arrays can be for example sent to Kafka as a sequence of messages - one message
    * per byte array.
    * @param reset if true, clears out all the containers other than lastContainer.
    *              Allows a producer of containers to obtain the
    *              byte arrays for sending somewhere else, while clearing containers for the next batch.
    */
  def nonCurrentContainerBytes(reset: Boolean = false): Seq[Array[Byte]] = {
    val bytes = allContainers.dropRight(1).map(_.array)
    if (reset) removeAndFreeContainers(containers.size - 1)
    bytes
  }

  /**
   * Returns the containers as byte arrays.  Assuming all of the containers except the last one is full,
   * calls array() on the non-last container and trimmedArray() on the last one.
   * The memFactory needs to be an on heap one otherwise UnsupportedOperationException will be thrown.
   * The sequence of byte arrays can be for example sent to Kafka as a sequence of messages - one message
   * per byte array.
   * @param reset if true, clears out all the containers EXCEPT the last one.  Pointers to the last container
   *              are simply reset, which avoids an extra container buffer allocation.
   */
  def optimalContainerBytes(reset: Boolean = false): Seq[Array[Byte]] = {
    val bytes = allContainers.dropRight(1).map(_.array) ++
      allContainers.takeRight(1).filterNot(_.isEmpty).map(_.trimmedArray)
    if (reset) {
      removeAndFreeContainers(containers.size - 1)
      this.reset()
    }
    bytes
  }

  /**
   * Remove the first numContainers containers and release the memory they took up.
   * If no more containers are left, then everything will be reset.
   * @param numContainers the # of containers to remove
   */
  def removeAndFreeContainers(numContainers: Int): Unit = if (numContainers > 0) {
    require(numContainers <= containers.length)
    if (numContainers == containers.length) reset()
    containers.take(numContainers).foreach { c => memFactory.freeMemory(c.offset) }
    containers.remove(0, numContainers)
  }

  /**
   * Returns the number of free bytes in the current container, or 0 if container is not initialized
   */
  def containerRemaining: Long = maxOffset - curRecEndOffset

  private def requireBytes(numBytes: Int): Unit =
    // if container is none, allocate a new one, make sure it has enough space, reset length, update offsets
    if (containers.isEmpty) {
      newContainer()
    // if we don't have enough space left, get a new container and move existing BR being written into new space
    } else if (curRecEndOffset + numBytes > maxOffset) {
      val oldBase = curBase
      val recordNumBytes = curRecEndOffset - curRecordOffset
      val oldOffset = curRecordOffset
      if (reuseOneContainer) resetContainerPointers() else newContainer()
      logger.debug(s"Moving $recordNumBytes bytes from end of old container to new container")
      require((containerSize - ContainerHeaderLen) > (recordNumBytes + numBytes), "Record too big for container")
      unsafe.copyMemory(oldBase, oldOffset, curBase, curRecordOffset, recordNumBytes)
      if (mapOffset != -1L) mapOffset = curRecordOffset + (mapOffset - oldOffset)
      curRecEndOffset = curRecordOffset + recordNumBytes
    }

  private[filodb] def newContainer(): Unit = {
    val (newBase, newOff, _) = memFactory.allocate(containerSize)
    val container = new RecordContainer(newBase, newOff, containerSize)
    containers += container
    logger.debug(s"Creating new RecordContainer with $containerSize bytes using $memFactory")
    curBase = newBase
    curRecordOffset = newOff + ContainerHeaderLen
    curRecEndOffset = curRecordOffset
    container.updateLengthWithOffset(curRecordOffset)
    container.writeVersionWord()
    maxOffset = newOff + containerSize
  }

  private def checkFieldNo(): Unit = require(fieldNo >= 0 && fieldNo < schema.numFields)

  private def checkFieldAndMemory(bytesRequired: Int): Unit = {
    checkFieldNo()
    requireBytes(bytesRequired)
  }

  private def updateFieldPointerAndLens(varFieldLen: Int): Unit = {
    // update fixed field area, which is a 4-byte offset to the var field
    setInt(curBase, curRecordOffset + schema.fieldOffset(fieldNo), (curRecEndOffset - curRecordOffset).toInt)
    curRecEndOffset += varFieldLen

    // update BinaryRecord length header as well
    setInt(curBase, curRecordOffset, (curRecEndOffset - curRecordOffset).toInt - 4)
  }
}

object RecordBuilder {
  val DefaultContainerSize = 256 * 1024
  val MinContainerSize = 2048
  val HASH_INIT = 7

  // Please do not change this.  It should only be changed with a change in BinaryRecord and/or RecordContainer
  // format, and only then REALLY carefully.
  val Version = 0
  val ContainerHeaderLen = 8

  val stringPairComparator = new java.util.Comparator[(String, String)] {
    def compare(pair1: (String, String), pair2: (String, String)): Int = pair1._1 compare pair2._1
  }

  /**
    * Make is a convenience factory method to access from java.
    */
  def make(memFactory: MemFactory,
           schema: RecordSchema,
           containerSize: Int = RecordBuilder.DefaultContainerSize): RecordBuilder = {
    new RecordBuilder(memFactory, schema, containerSize)
  }

  /**
    * == Auxiliary functions to compute hashes. ==
    */

  import filodb.core._

  val keyHashCache = concurrentCache[String, Int](1000)

  /**
    * Sorts an incoming list of key-value pairs and then computes a hash value
    * for each pair.  The output can be fed into the combineHash methods to produce an overall hash.
    * NOTE: we use XXHash, it gives a MUCH higher quality hash than the default String hashCode.
    * @param pairs an unsorted list of key-value pairs.  Will be mutated and sorted.
    */
  final def sortAndComputeHashes(pairs: java.util.ArrayList[(String, String)]): Array[Int] = {
    pairs.sort(stringPairComparator)
    val hashes = new Array[Int](pairs.size)
    for { i <- 0 until pairs.size optimized } {
      val (k, v) = pairs.get(i)
      // This is not very efficient, we have to convert String to bytes first to get the hash
      // TODO: work on different API which is far more efficient and saves memory allocation
      val valBytes = v.getBytes
      val keyHash = keyHashCache.getOrElseUpdate(k, { key =>
        val keyBytes = key.getBytes
        BinaryRegion.hasher32.hash(keyBytes, 0, keyBytes.size, BinaryRegion.Seed)
      })
      hashes(i) = combineHash(keyHash, BinaryRegion.hasher32.hash(valBytes, 0, valBytes.size, BinaryRegion.Seed))
    }
    hashes
  }

  // NOTE: I've tried many different hash combiners, but nothing tried (including Murmur3) seem any better than
  // XXHash + the simple formula below.
  @inline
  final def combineHash(hash1: Int, hash2: Int): Int = 31 * hash1 + hash2

  /**
    * Combines the hashes from sortAndComputeHashes, excluding certain keys, into an overall hash value.
    * @param sortedPairs sorted pairs of byte key values, from sortAndComputeHashes
    * @param hashes the output from sortAndComputeHashes
    * @param excludeKeys set of String keys to exclude
    */
  final def combineHashExcluding(sortedPairs: java.util.ArrayList[(String, String)],
                                 hashes: Array[Int],
                                 excludeKeys: Set[String]): Int = {
    var hash = 7
    for { i <- 0 until sortedPairs.size optimized } {
      if (!(excludeKeys contains sortedPairs.get(i)._1))
        hash = combineHash(hash, hashes(i))
    }
    hash
  }

  /**
   * Computes a shard key hash from the metric name and the values of the non-metric shard key columns
   * @param shardKeyValues the non-metric shard key values (such as the job/exporter/app), sorted in order of
   *        the key name.  For example, it should be Seq(exporter, job).
   * @param metric the metric value to use in the calculation.
   */
  final def shardKeyHash(shardKeyValues: Seq[Array[Byte]], metric: Array[Byte]): Int = {
    var hash = 7
    shardKeyValues.foreach { value => hash = combineHash(hash, BinaryRegion.hash32(value)) }
    combineHash(hash, BinaryRegion.hash32(metric))
  }

  final def shardKeyHash(shardKeyValues: Seq[String], metric: String): Int =
    shardKeyHash(shardKeyValues.map(_.getBytes), metric.getBytes)

  /**
    * Removes the ignoreShardKeyColumnSuffixes from LabelPair as configured in DataSet.
    *
    * Few metric types like Histogram, Summary exposes multiple time
    * series for the same metric during a scrape by appending suffixes _bucket,_sum,_count.
    *
    * In order to ingest all these multiple time series of a single metric to the
    * same shard, we have to trim the suffixes while calculating shardKeyHash.
    *
    * @param dataSet    - Current DataSet
    * @param shardKeyColName  - ShardKey label name as String
    * @param shardKeyColValue - ShardKey label value as String
    * @return - Label value after removing the suffix
    */
  final def trimShardColumn(dataSet: Dataset, shardKeyColName: String, shardKeyColValue: String): String = {
    dataSet.options.ignoreShardKeyColumnSuffixes.get(shardKeyColName) match {
      case Some(trimMetricSuffixColumn) => trimMetricSuffixColumn.find(shardKeyColValue.endsWith) match {
                                            case Some(s)  => shardKeyColValue.dropRight(s.length)
                                            case _        => shardKeyColValue
                                           }
      case _                            => shardKeyColValue
    }
  }

}
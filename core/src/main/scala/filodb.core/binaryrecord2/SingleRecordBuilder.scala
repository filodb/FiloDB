package filodb.core.binaryrecord2

import filodb.memory.format.UnsafeUtils.unsafe

/**
 * A RowBuilder that writes a single BinaryRecord directly into a pre-allocated memory region,
 * with no container management overhead. Supports both on-heap (base = Array[Byte]) and off-heap
 * (base = UnsafeUtils.ZeroPointer, offset = native address) memory.
 *
 * Call reset(newBase, newOffset, newCapacity) to repoint the builder at a new memory region
 * for each successive record, enabling zero-allocation reuse.
 *
 * Intended for callers that own the destination buffer — e.g. writing BinaryRecords directly
 * into an Arrow VarBinaryVector's off-heap data buffer to avoid the on-heap RecordBuilder →
 * Arrow copy present in the current ArrowSerializedRangeVectorOps flow.
 *
 * endRecord() returns the starting offset of the completed record. The caller can derive
 * record length via BinaryRegionLarge.numBytes(base, recordOffset) (content bytes, excluding
 * the 4-byte length header), with aligned total size = (numBytes + 7) & ~3.
 */
class SingleRecordBuilder(initialBase: Any, initialOffset: Long, initialCapacity: Int)
                         (allocateNewBuf: => (Any, Long, Int))
    extends RecordBuilderBase {

  reset(initialBase, initialOffset, initialCapacity)

  /**
   * Repoints the builder to a new memory region. Must be called before startNewRecord()
   * for each new record.
   *
   * @param newBase     base pointer for the destination memory
   *                    (Array[Byte] for on-heap; UnsafeUtils.ZeroPointer for off-heap)
   * @param newOffset   byte offset within newBase where the record will start
   * @param newCapacity available bytes starting at newOffset
   */
  def reset(newBase: Any, newOffset: Long, newCapacity: Int): Unit = {
    curBase         = newBase
    curRecordOffset = newOffset
    curRecEndOffset = newOffset
    maxOffset       = newOffset + newCapacity
    fieldNo         = -1
    mapOffset       = -1L
    recHash         = -1
  }

  protected def requireBytes(numBytes: Int): Unit = {
    if (curRecEndOffset + numBytes > maxOffset) {
      val oldBase = curBase
      val recordNumBytes = curRecEndOffset - curRecordOffset
      val oldOffset = curRecordOffset
      val (newBase, newOffset, newCapacity) = allocateNewBuf
      curBase         = newBase
      curRecordOffset = newOffset
      curRecEndOffset = newOffset
      maxOffset       = newOffset + newCapacity

      logger.trace(s"Moving $recordNumBytes bytes from end of old container to new container")
      require(newCapacity > (recordNumBytes + numBytes),
        s"The intermediate or final result is too big. Please try to add more query filters or time range.")
      unsafe.copyMemory(oldBase, oldOffset, curBase, curRecordOffset, recordNumBytes)
      if (mapOffset != -1L) {
        require(mapOffset >= oldOffset,
          s"mapOffset=$mapOffset < record start=$oldOffset; invariant violated in RowBuilder")
        mapOffset = curRecordOffset + (mapOffset - oldOffset)
      }
      curRecEndOffset = curRecordOffset + recordNumBytes
    }
  }

  // Caller owns the memory; no container bookkeeping.
  def postEndRecord(): Unit = ()

  /**
   * Offset where the next record should start — the 4-byte-aligned end of the record just written.
   * Valid after endRecord(). Equal to recordEndOffset.
   */
  def nextRecordOffset: Long = curRecordOffset
}

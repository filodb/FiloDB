package filodb.downsampler

import filodb.core.store.{AllChunkScan, ChunkSetInfoReader, ReadablePartition}

object Utils {
  /**
   * Specifies a range of a chunk's rows.
   */
  case class ChunkRange(chunkSetInfoReader: ChunkSetInfoReader,
                        istartRow: Int,
                        iendRow: Int)

  /**
   * Determine, for each chunk, the range of rows with timestamps inside the time-range
   *   given by userTimeStart and userTimeEndExclusive.
   */
  def getChunkRangeIter(readablePart: ReadablePartition,
                        userTimeStart: Long,
                        userTimeEndExclusive: Long): Iterator[ChunkRange] = {
    val timestampCol = 0  // FIXME: need a dynamic (but efficient) way to determine this
    val rawChunksets = readablePart.infos(AllChunkScan)

    val it = new Iterator[ChunkSetInfoReader]() {
      override def hasNext: Boolean = rawChunksets.hasNext
      override def next(): ChunkSetInfoReader = rawChunksets.nextInfoReader
    }

    it.filter(chunkset => chunkset.startTime < userTimeEndExclusive && userTimeStart <= chunkset.endTime)
      .map { chunkset =>
        val tsPtr = chunkset.vectorAddress(timestampCol)
        val tsAcc = chunkset.vectorAccessor(timestampCol)
        val tsReader = readablePart.chunkReader(timestampCol, tsAcc, tsPtr).asLongReader

        val startRow = tsReader.binarySearch(tsAcc, tsPtr, userTimeStart) & 0x7fffffff
        // userTimeEndExclusive-1 since ceilingIndex does an inclusive check
        val endRow = Math.min(tsReader.ceilingIndex(tsAcc, tsPtr, userTimeEndExclusive - 1), chunkset.numRows - 1)
        ChunkRange(chunkset, startRow, endRow)
      }
      .filter{ chunkRange =>
        val isValidChunk = chunkRange.istartRow <= chunkRange.iendRow
        if (!isValidChunk) {
          // TODO(a_theimer): numRawChunksSkipped.increment()
          // TODO(a_theimer): use DsContext?
          DownsamplerContext.dsLogger.warn(s"Skipping chunk of partition since startRow lessThan endRow " +
            s"hexPartKey=${readablePart.hexPartKey} " +
            s"startRow=${chunkRange.istartRow} " +
            s"endRow=${chunkRange.iendRow} " +
            s"chunkset: ${chunkRange.chunkSetInfoReader.debugString}")
        }
        isValidChunk
      }
  }
}

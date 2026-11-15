package filodb.query.exec

import scala.collection.mutable.ArrayBuffer

import filodb.core.query.{QueryContext, RawDataRangeVector}
import filodb.core.store.WindowedChunkIterator
import filodb.query.exec.rangefn.RawDataWindowingSpec

/**
 * Exercises the array deque inside WindowedChunkIterator (append, front eviction, in-place compaction and growth)
 * against a reference implementation of the original Buffer based algorithm.
 */
class WindowedChunkIteratorSpec extends RawDataWindowingSpec {

  private val numChunks = 40
  private val samplesPerChunk = 5
  // Chunk k holds samples at chunkStart(k) + i * pubFreq: it spans 40s, followed by a 60s gap before the next chunk
  private val chunkSpacing = 100000L
  private def chunkStart(k: Int): Long = defaultStartTS + k * chunkSpacing
  private val lastSampleTs = chunkStart(numChunks - 1) + (samplesPerChunk - 1) * pubFreq

  private def chunkTuples(k: Int): Seq[(Long, Double)] =
    (0 until samplesPerChunk).map(i => (chunkStart(k) + i * pubFreq, (k * samplesPerChunk + i).toDouble))

  private def manyChunksRV(): RawDataRangeVector = {
    val rv = timeValueRVPk(chunkTuples(0))
    (1 until numChunks).foreach(k => addChunkToRV(rv, chunkTuples(k)))
    rv
  }

  private case class Info(id: Long, startTime: Long, endTime: Long, numRows: Int)

  private def allInfos(rv: RawDataRangeVector, start: Long, end: Long): Seq[Info] = {
    val it = rv.chunkInfos(start, end)
    try {
      val infos = new ArrayBuffer[Info]
      while (it.hasNext) {
        val r = it.nextInfoReader
        infos += Info(r.id, r.startTime, r.endTime, r.numRows)
      }
      infos.toSeq
    } finally it.close()
  }

  private case class RunResult(windows: Seq[Seq[Long]], expected: Seq[Seq[Long]],
                               capacity: Int, maxOccupancy: Int, numAppended: Int)

  /**
   * Runs WindowedChunkIterator over all windows, and in lockstep the original algorithm: a buffer of the window's
   * infos, evicting from the front infos which end before the window start, and appending infos that end at or after
   * the window start while the window end is beyond the end of the last buffered info.
   */
  private def run(rv: RawDataRangeVector, start: Long, step: Long, end: Long, window: Long,
                  isInclusiveRange: Boolean = true): RunResult = {
    val source = allInfos(rv, start - window, end).iterator
    val reference = new ArrayBuffer[Info]
    var numAppended = 0
    var maxOccupancy = 0
    val windows = new ArrayBuffer[Seq[Long]]
    val expected = new ArrayBuffer[Seq[Long]]

    val it = new WindowedChunkIterator(rv, start, step, end, window, QueryContext(),
      isInclusiveRange = isInclusiveRange)
    it.lock()
    try {
      while (it.hasMoreWindows) {
        it.nextWindow()
        val actual = new ArrayBuffer[Long]
        while (it.hasNext) actual += it.next().id
        windows += actual.toSeq

        while (reference.nonEmpty && reference.head.endTime < it.curWindowStart) reference.remove(0)
        var lastEndTime = if (reference.isEmpty) -1L else reference.last.endTime
        while (it.curWindowEnd > lastEndTime && source.hasNext) {
          val next = source.next()
          if (it.curWindowStart <= next.endTime && next.numRows > 0) {
            reference += next
            numAppended += 1
            lastEndTime = Math.max(next.endTime, lastEndTime)
          }
        }
        maxOccupancy = Math.max(maxOccupancy, reference.size)
        expected += reference.map(_.id).toSeq
      }
      RunResult(windows.toSeq, expected.toSeq, it.windowInfosCapacity, maxOccupancy, numAppended)
    } finally {
      it.unlock()
      it.close()
    }
  }

  private def checkMatchesReference(r: RunResult): Unit = {
    r.windows.size shouldEqual r.expected.size
    r.windows.zip(r.expected).zipWithIndex.foreach { case ((actual, exp), w) =>
      withClue(s"window $w: ") { actual shouldEqual exp }
    }
  }

  describe("WindowedChunkIterator window infos") {
    it("should build the partition with one chunk per sample group") {
      val rv = manyChunksRV()
      val infos = allInfos(rv, 0L, Long.MaxValue)
      infos.size shouldEqual numChunks
      infos.map(_.startTime) shouldEqual (0 until numChunks).map(chunkStart)
    }

    it("should compact in place, without growing, when a short window slides over many chunks") {
      val rv = manyChunksRV()
      val window = 150000L   // covers at most 3 chunks
      val r = run(rv, defaultStartTS + window, 10000L, lastSampleTs, window)
      checkMatchesReference(r)
      r.maxOccupancy should be <= 3
      // every chunk passed through the initial 8 slot array, so it must have compacted in place several times
      r.numAppended shouldEqual numChunks
      r.capacity shouldEqual 8
    }

    it("should grow when the window holds more chunks than the array") {
      val rv = manyChunksRV()
      val window = 2500000L  // covers ~25 chunks
      val r = run(rv, defaultStartTS + window, 50000L, lastSampleTs, window)
      checkMatchesReference(r)
      r.maxOccupancy should be > 16
      r.capacity should be > 16
      r.capacity should be < 4 * r.maxOccupancy
    }

    it("should both grow and compact when the occupancy hovers around half the capacity") {
      val rv = manyChunksRV()
      val window = 700000L   // covers 7-8 chunks
      val r = run(rv, defaultStartTS + window, 20000L, lastSampleTs, window)
      checkMatchesReference(r)
      r.numAppended shouldEqual numChunks
      r.maxOccupancy should be > 4
      // grew past the initial 8 slots, yet all 40 chunks passed through at most 16 slots, so it also compacted
      r.capacity should be > 8
      r.capacity should be <= 16
    }

    it("should handle a window covering all chunks from the first window") {
      val rv = manyChunksRV()
      val window = lastSampleTs
      val r = run(rv, lastSampleTs, 10000L, lastSampleTs, window)
      checkMatchesReference(r)
      r.windows.head.size shouldEqual numChunks
      r.capacity shouldEqual 64
    }

    it("should handle steps longer than the window, which skip chunks and empty the window") {
      val rv = manyChunksRV()
      val window = 20000L
      Seq(true, false).foreach { inclusive =>
        withClue(s"isInclusiveRange=$inclusive: ") {
          val r = run(rv, defaultStartTS + 70000L, 230000L, lastSampleTs, window, inclusive)
          checkMatchesReference(r)
          r.capacity shouldEqual 8
        }
      }
    }

    it("should handle a window lining up with chunk boundaries for inclusive and exclusive ranges") {
      val rv = manyChunksRV()
      val window = chunkSpacing * 3
      Seq(true, false).foreach { inclusive =>
        withClue(s"isInclusiveRange=$inclusive: ") {
          checkMatchesReference(run(rv, chunkStart(3), chunkSpacing, lastSampleTs, window, inclusive))
        }
      }
    }
  }
}

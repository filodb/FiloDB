package filodb.core.columnstore

import com.typesafe.config.ConfigFactory
import filodb.core._
import filodb.core.metadata.Column
import org.velvia.filo.TupleRowReader
import java.nio.ByteBuffer

import org.scalatest.FunSpec
import org.scalatest.Matchers

class ChunkMergingStrategySpec extends FunSpec with Matchers {
  import SegmentSpec._
  val keyRange = KeyRange("dataset", "partition", 0L, 10000L)
  val sortKey = "age"

  import scala.concurrent.ExecutionContext.Implicits.global
  val colStore = new InMemoryColumnStore
  val dataset = "foo"
  val mergingStrategy = new AppendingChunkMergingStrategy(colStore)

  private def mergeRows(firstSegRows: Seq[Product], secondSegRows: Seq[Product]) = {
      val segment = getRowWriter(keyRange)
      if (firstSegRows.nonEmpty) segment.addRowsAsChunk(mapper(firstSegRows), getSortKey _)

      val segment2 = getRowWriter(keyRange)
      if (secondSegRows.nonEmpty) segment2.addRowsAsChunk(mapper(secondSegRows), getSortKey _)

      val mergedSeg = mergingStrategy.mergeSegments(segment, segment2)
      mergedSeg should not be ('empty)
      mergedSeg.keyRange should equal (segment.keyRange)
      mergedSeg.getColumns should equal (Set("first", "last", "age"))

      // Verify that the merged Segment has the same chunks as segment2, except the chunkId is offset
      val offsetChunks = segment2.getChunks.map { case (colId, chunkId, bytes) =>
        (colId, chunkId + segment.index.nextChunkId, bytes)
      }
      mergedSeg.getChunks.toSet should equal (offsetChunks.toSet)
      mergedSeg
  }

  describe("mergeSegments") {
    it("should forbid merging segments from different keyRanges") {
      val segment = getRowWriter(keyRange)
      segment.addRowsAsChunk(mapper(names take 3), getSortKey _)

      val segment2 = getRowWriter(keyRange.copy(start = 20000L, end = 30000L))
      segment2.addRowsAsChunk(mapper(names drop 3), getSortKey _)

      intercept[RuntimeException] { mergingStrategy.mergeSegments(segment, segment2) }
    }

    it("should merge a new segment with an empty segment") {
      val mergedSeg = mergeRows(Nil, names take 3)

      // One thing to keep in mind is that we cannot just read all the data from the merged segment
      // because the AppendingChunkMergingStrategy does not keep or assume the first segment has
      // any data, other than the sort key.
      mergedSeg.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0))
      mergedSeg.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1))
    }

    it("should merge new rows to a nonempty GenericSegment successfully") {
      // Nonoverlapping rows, pure append
      val mergedSeg = mergeRows(names take 3, names drop 3)

      mergedSeg.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 1, 1, 1))
      mergedSeg.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 2, 1, 0))
    }

    it("should merge new rows to a nonempty RowReaderSegment successfully") {
      val segment = getRowWriter(keyRange)
      segment.addRowsAsChunk(mapper(names take 3), getSortKey _)
      // The below two lines simulate a write segment / read cycle
      val prunedSeg = mergingStrategy.pruneForCache(projection, segment)
      val readerSeg = RowReaderSegment(prunedSeg.asInstanceOf[GenericSegment[Long]], schema drop 2)

      val segment2 = getRowWriter(keyRange)
      segment2.addRowsAsChunk(mapper(names drop 3), getSortKey _)
      val mergedSeg = mergingStrategy.mergeSegments(readerSeg, segment2)

      mergedSeg.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 1, 1, 1))
      mergedSeg.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 2, 1, 0))
    }

    it("should merge segment with out of order rows successfully") {
      // Segment 1  - chunk 0 in merged Seg
      // (Some("Rodney"), Some("Hudson"), Some(25L)),
      // (Some("Jerry"),  None,           Some(40L)),
      // (Some("Peyton"), Some("Manning"), Some(39L)),
      // Segment 2  - chunk 1 in merged Seg
      // (Some("Khalil"), Some("Mack"), Some(24L)),
      // (Some("Ndamukong"), Some("Suh"), Some(28L)),
      // (Some("Terrance"), Some("Knighton"), Some(29L)))
      val mergedSeg = mergeRows(names drop 2 take 3, (names take 2) ++ (names drop 5))

      mergedSeg.index.chunkIdIterator.toSeq should equal (Seq(1, 0, 1, 1, 0, 0))
      mergedSeg.index.rowNumIterator.toSeq should equal (Seq(0, 0, 1, 2, 2, 1))
    }

    it("should replace rows to a segment successfully") {
      // Case 1: overwrite some of orig rows again
      val mergedSeg = mergeRows(names, names take 2)

      mergedSeg.index.chunkIdIterator.toSeq should equal (Seq(1, 0, 1, 0, 0, 0))
      mergedSeg.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 5, 4, 3))

      // Case 2: overwrite and append new data.  Let's give Jerry Rice a last name.
      val newNames = Seq((Some("Jerry"), Some("Rice"),  Some(40L)),
                         (Some("Tim"),   Some("Brown"), Some(45L)))
      val mergedSeg2 = mergeRows(names, newNames)

      mergedSeg2.index.chunkIdIterator.toSeq should equal (Seq(0, 0, 0, 0, 0, 1, 1))
      mergedSeg2.index.rowNumIterator.toSeq should equal (Seq(0, 2, 1, 5, 4, 0, 1))
    }
  }

  describe("pruneForCache") {
    it("should prune segments that have more than the sortColumn") {
      val segment = getRowWriter(keyRange)
      segment.addRowsAsChunk(mapper(names take 3), getSortKey _)
      val prunedSeg = mergingStrategy.pruneForCache(projection, segment)

      prunedSeg.getColumns should equal (Set("age"))
      prunedSeg.getChunks.toSet should equal (segment.getChunks.filter(_._1 == "age").toSet)
    }
  }
}
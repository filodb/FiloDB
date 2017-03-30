package filodb.core.memstore

import org.velvia.filo.BinaryVector

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core._

class TimeSeriesPartitionSpec extends FunSpec with Matchers with ScalaFutures {
  import monix.execution.Scheduler.Implicits.global
  import MachineMetricsData._

  it("should be able to read immediately after ingesting rows") {
    val part = new TimeSeriesPartition(projection, defaultPartKey, chunksToKeep = 3, maxChunkSize = 10)
    val data = mapper(singleSeriesData()).take(5)
    part.ingest(data(0), 1000L)
    part.numChunks should equal (1)
    val minData = data.map(_.getDouble(1)).take(1)
    val chunk1 = part.streamReaders(part.newestChunkIds(1), Array(1)).headL.runAsync.futureValue
    chunk1.vectors(0).toSeq should equal (minData)
  }

  it("should ingest rows, flush, and be able to ingest new rows") {
    val part = new TimeSeriesPartition(projection, defaultPartKey, chunksToKeep = 3, maxChunkSize = 10)
    val data = mapper(singleSeriesData()).take(11)
    val minData = data.map(_.getDouble(1))
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, 1000L + i) }

    // there should be a frozen chunk of 10 records plus 1 record in currently appending chunks
    part.numChunks should equal (2)
    val chunks = part.streamReaders(part.newestChunkIds(2), Array(1))
                     .map(_.vectors(0).toSeq).toListL.runAsync
    chunks.futureValue should equal (Seq(minData take 10, minData drop 10))
  }

  it("should remove old chunks when # chunks > chunksToKeep") {
    val part = new TimeSeriesPartition(projection, defaultPartKey, chunksToKeep = 2, maxChunkSize = 10)
    val data = mapper(singleSeriesData()).take(21)   // one more than needed to kick old chunks out
    val minData = data.map(_.getDouble(1))

    // First ingest 20 rows. This should fill up and finalize 2 chunks.  Both chunks should be kept.
    data.zipWithIndex.take(20).foreach { case (r, i) => part.ingest(r, 1000L + i) }
    part.numChunks should equal (2)
    val ids1 = part.newestChunkIds(2).toBuffer
    ids1 should have length (2)
    val chunks = part.streamReaders(part.newestChunkIds(2), Array(1)).map(_.vectors(0).toSeq).toListL.runAsync
    chunks.futureValue should equal (Seq(minData take 10, minData drop 10 take 10))

    // Now ingest one more.  This should start creating a new set of chunks which means old one gotta go
    part.ingest(data.last, 10000L)
    part.numChunks should equal (2)
    val ids2 = part.newestChunkIds(2).toBuffer
    ids2 should not equal (ids1)
    val chunks2 = part.streamReaders(part.newestChunkIds(2), Array(1)).map(_.vectors(0).toSeq).toListL.runAsync
    chunks2.futureValue should equal (Seq(minData drop 10 take 10, minData drop 20))
  }

  it("should skip ingesting rows when offset < flushedWatermark") {
    val part = new TimeSeriesPartition(projection, defaultPartKey, chunksToKeep = 3, maxChunkSize = 10)
    part.flushedWatermark = 500L
    val data = mapper(singleSeriesData()).take(10)
    val minData = data.map(_.getDouble(1))
    // First 6 will be skipped, offsets 0, 100, 200, 300, 400, 500
    data.zipWithIndex.foreach { case (r, i) => part.ingest(r, i * 100) }

    part.numChunks should equal (1)
    val chunks = part.streamReaders(part.newestChunkIds(1), Array(1))
                     .map(_.vectors(0).toSeq).toListL.runAsync
    chunks.futureValue should equal (Seq(minData drop 6))
  }
}
package filodb.core.store

import filodb.core.Setup._
import filodb.core.Types.{ChunkId, ColumnId}
import filodb.core.metadata._
import filodb.core.query.UnorderedSegmentScan
import filodb.core.reprojector.Reprojector
import filodb.core.store.ColumnStoreSpec.MapColumnStore
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.TupleRowReader

import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}

object ColumnStoreSpec {

  import scala.concurrent.ExecutionContext.Implicits.global

  trait MapChunkStore extends ChunkStore {
    val chunksMap = new java.util.HashMap[Any, java.util.TreeMap[Any, java.util.TreeMap[ChunkId, ChunkWithMeta]]]

    override def appendChunk(chunk: ChunkWithMeta): Future[Boolean] = {
      var segments = chunksMap.get(chunk.segmentInfo.partition)
      if (segments == null) {
        segments = new java.util.TreeMap[Any, java.util.TreeMap[ChunkId, ChunkWithMeta]]()
        chunksMap.put(chunk.segmentInfo.partition, segments)
      }
      var chunks = segments.get(chunk.segmentInfo.segment)
      if (chunks == null) {
        chunks = new java.util.TreeMap[ChunkId, ChunkWithMeta]()
        segments.put(chunk.segmentInfo.segment, chunks)
      }
      chunks.put(chunk.chunkId, chunk)
      Future(true)
    }

    override def getAllChunksForSegments(segmentInfoSeq: Seq[SegmentInfo],
                                         columns: Seq[ColumnId]): Future[Map[SegmentInfo, Seq[ChunkWithMeta]]] = {
      var segmentMap = Map[SegmentInfo, Seq[ChunkWithMeta]]()
      segmentInfoSeq.foreach { segmentInfo =>
        val segments = chunksMap.get(segmentInfo.partition)
        val segment = segments.get(segmentInfo.segment)
        val chunks = segment.values().asScala.toSeq
        segmentMap = segmentMap + (segmentInfo -> chunks)
      }
      Future(segmentMap)
    }

    override def getChunks(segmentInfo: SegmentInfo,
                           chunkIds: Seq[ChunkId]): Seq[ChunkWithId] = {
      val segments = chunksMap.get(segmentInfo.partition)
      val segment = segments.get(segmentInfo.segment)
      val chunks = segment.values().asScala
      chunks.filter(i => chunkIds.contains(i.chunkId)).toSeq
    }

  }

  trait MapSummaryStore extends SummaryStore {
    val summaryMap = new java.util.HashMap[Any, java.util.TreeMap[Any, (SegmentVersion, SegmentSummary)]]()

    /**
     * Atomically compare and swap the new SegmentSummary for this SegmentID
     */
    override def compareAndSwapSummary(segmentVersion: SegmentVersion,
                                       segmentInfo: SegmentInfo,
                                       segmentSummary: SegmentSummary): Future[Boolean] = {
      var segments = summaryMap.get(segmentInfo.partition)
      if (segments == null) {
        segments = new java.util.TreeMap[Any, (SegmentVersion, SegmentSummary)]()
        summaryMap.put(segmentInfo.partition, segments)
      }
      segments.put(segmentInfo.segment, (segmentVersion, segmentSummary))
      Future(true)
    }

    override def readSegmentSummary(segmentInfo: SegmentInfo):
    Future[Option[(SegmentVersion, SegmentSummary)]] = {
      val segments = summaryMap.get(segmentInfo.partition)
      if (segments != null) {
        val s = segments.get(segmentInfo.segment)
        if (s != null) {
          val (version, summary: SegmentSummary) = s
          Future(Some((version, summary.asInstanceOf[SegmentSummary])))
        } else Future(None)
      } else Future(None)
    }

    override def readSegmentSummaries(projection: Projection,
                                      partitionKey: Any,
                                      keyRange: KeyRange[_]): Future[Seq[SegmentSummary]] = {
      val segments = summaryMap.get(partitionKey)
      if (segments == null)
        Future(List.empty[SegmentSummary])
      else
        Future(segments.subMap(keyRange.start, keyRange.end).values()
          .asScala.map(_._2).toSeq)
    }
  }

  class MapColumnStore extends ColumnStore with MapChunkStore with MapSummaryStore

}

class ColumnStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {

  import scala.concurrent.duration._

  describe("Store and read rows") {
    it("should store and read one flush properly") {
      val mapColumnStore = new MapColumnStore()

      val rows = names.map(TupleRowReader)
      val partitions = Reprojector.project(projection, rows).toSeq
      partitions.length should be(2)
      val results = partitions.map { case (p, flushes) =>
        flushes.map { flush =>
          Await.result(mapColumnStore.flushToSegment(projection, flush), 100 seconds)
        }
      }
      results.foreach(seq => seq.foreach { r: Boolean =>
        r should be(true)
      })
      val segments = Await.result(mapColumnStore.readSegments(projection, "US",
        projection.schema.map(i => i.name), KeyRange("A", "Z")), 10 seconds)

      segments.length should be(2)
      val scan = new UnorderedSegmentScan(segments.head)
      scan.hasMoreRows should be(true)
      val threeReaders = scan.getMoreRows(3)
      scan.hasMoreRows should be(false)
      val reader = threeReaders.head
      reader.getString(0) should be("US")
      reader.getString(1) should be("NY")
      reader.getString(2) should be("Ndamukong")
      reader.getLong(4) should be(28)

      val scan2 = new UnorderedSegmentScan(segments.last)
      scan2.hasMoreRows should be(true)
      val threeMore = scan2.getMoreRows(3)
      val reader1 = threeMore.last
      reader1.getString(0) should be("US")
      reader1.getString(1) should be("SF")
      reader1.getString(2) should be("Khalil")
      reader1.getLong(4) should be(24)

    }
  }

}

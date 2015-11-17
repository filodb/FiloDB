package filodb.core.store

import filodb.core.Setup._
import filodb.core.Types.{ChunkId, ColumnId, PartitionKey}
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
    val chunksMap = new java.util.HashMap[PartitionKey, java.util.TreeMap[Any, java.util.TreeMap[ChunkId, Chunk[_,_]]]]

    override def appendChunk[R,S](chunk: Chunk[R,S]): Future[Boolean] = {
      var segments = chunksMap.get(chunk.segmentInfo.partition)
      if (segments == null) {
        segments = new java.util.TreeMap[Any, java.util.TreeMap[ChunkId, Chunk[_,_]]]()
        chunksMap.put(chunk.segmentInfo.partition, segments)
      }
      var chunks = segments.get(chunk.segmentInfo.segment)
      if (chunks == null) {
        chunks = new java.util.TreeMap[ChunkId, Chunk[_,_]]()
        segments.put(chunk.segmentInfo.segment, chunks)
      }
      chunks.put(chunk.chunkId, chunk)
      Future(true)
    }

    override def readChunksForSegments[R,S](segmentInfoSeq: Seq[SegmentInfo[R,S]],
                                            columns: Seq[ColumnId]): Future[Map[SegmentInfo[R,S], Seq[Chunk[R,S]]]] = {
      var segmentMap = Map[SegmentInfo[R,S], Seq[Chunk[R,S]]]()
      segmentInfoSeq.foreach { segmentInfo =>
        val segments = chunksMap.get(segmentInfo.partition)
        val segment = segments.get(segmentInfo.segment)
        val chunks: Seq[Chunk[R,S]] = segment.values().asScala.map(_.asInstanceOf[Chunk[R,S]]).toSeq
        segmentMap = segmentMap + (segmentInfo -> chunks)
      }
      Future(segmentMap)
    }
  }

  trait MapSummaryStore extends SummaryStore {
    val summaryMap = new java.util.HashMap[PartitionKey, java.util.TreeMap[Any, (SegmentVersion, SegmentSummary[_,_])]]()

    /**
     * Atomically compare and swap the new SegmentSummary for this SegmentID
     */
    override def compareAndSwapSummary[R,S](segmentVersion: SegmentVersion, segmentInfo: SegmentInfo[R,S], segmentSummary: SegmentSummary[R,S]): Future[Boolean] = {
      var segments = summaryMap.get(segmentInfo.partition)
      if (segments == null) {
        segments = new java.util.TreeMap[Any, (SegmentVersion, SegmentSummary[_,_])]()
        summaryMap.put(segmentInfo.partition, segments)
      }
      segments.put(segmentInfo.segment, (segmentVersion, segmentSummary))
      Future(true)
    }

    override def readSegmentSummary[R,S](segmentInfo: SegmentInfo[R,S]): Future[Option[(SegmentVersion, SegmentSummary[R,S])]] = {
      val segments = summaryMap.get(segmentInfo.partition)
      if (segments != null) {
        val s = segments.get(segmentInfo.segment)
        if (s != null) {
          val (version, summary: SegmentSummary[R,S]) = s
          Future(Some((version, summary.asInstanceOf[SegmentSummary[R,S]])))
        } else Future(None)
      } else Future(None)
    }

    override def readSegmentSummaries[R,S](projection: ProjectionInfo[R,S],
                                         partitionKey: PartitionKey,
                                         keyRange: KeyRange[S]): Future[Seq[SegmentSummary[R,S]]] = {
      val segments = summaryMap.get(partitionKey)
      Future(segments.subMap(keyRange.start, keyRange.end).values()
        .asScala.map(_._2.asInstanceOf[SegmentSummary[R,S]]).toSeq)
    }
  }

  class MapColumnStore extends ColumnStore with MapChunkStore with MapSummaryStore

}

class ColumnStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {

  import scala.concurrent.duration._

  describe("Store and read rows") {
    it("should store and read one flush properly") {
      val mapColumnStore = new MapColumnStore()

      val rows = names2.map(TupleRowReader)
      val flushes = Reprojector.project(Dataset.DefaultPartitionKey, projection, rows)
      flushes.map { flush =>
        Await.result(mapColumnStore.flushToSegment(projection, flush), 10 seconds)
      }
      val segments = Await.result(mapColumnStore.readSegments(projection,
        Dataset.DefaultPartitionKey,
        projection.columns.map(i => i.name), KeyRange[Long](10L, 50L)), 10 seconds)

      segments.length should be(2)
      val scan = new UnorderedSegmentScan(segments.head)
      scan.hasMoreRows should be(true)
      val threeReaders = scan.getMoreRows(3)
      scan.hasMoreRows should be(false)
      val reader = threeReaders.head
      reader.getString(0) should be("Peyton")
      reader.getLong(2) should be(24)

      val scan2 = new UnorderedSegmentScan(segments.last)
      scan2.hasMoreRows should be(true)
      val threeMore = scan2.getMoreRows(3)
      val reader1 = threeMore.last
      reader1.getString(0) should be("Jerry")
      reader1.getLong(2) should be(40)

    }
  }

}

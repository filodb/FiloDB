package filodb.core.store

import java.util.UUID

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

    override def appendChunk(projection: Projection,
                             partition: Any,
                             segment: Any,
                             chunk: ChunkWithMeta): Future[Boolean] = {
      var segments = chunksMap.get(partition)
      if (segments == null) {
        segments = new java.util.TreeMap[Any, java.util.TreeMap[ChunkId, ChunkWithMeta]]()
        chunksMap.put(partition, segments)
      }
      var chunks = segments.get(segment)
      if (chunks == null) {
        chunks = new java.util.TreeMap[ChunkId, ChunkWithMeta]()
        segments.put(segment, chunks)
      }
      chunks.put(chunk.chunkId, chunk)
      Future(true)
    }

    override def getAllChunksForSegments(projection: Projection,
                                         partition: Any,
                                         segmentRange: KeyRange[_],
                                         columns: Seq[ColumnId]): Future[Seq[(Any, Seq[ChunkWithMeta])]] = {
      val segments = chunksMap.get(partition).subMap(segmentRange.start, segmentRange.end)
      val segmentMap = segments.entrySet().asScala.map { case entry =>
        val segmentId = entry.getKey
        val chunkMap = entry.getValue
        val chunks = chunkMap.entrySet().asScala.map(_.getValue).toSeq
        segmentId -> chunks
      }.toSeq
      Future(segmentMap)
    }

    override def getSegmentChunks(projection: Projection,
                                  partition: Any,
                                  segmentId: Any,
                                  columns: Seq[ColumnId],
                                  chunkIds: Seq[ChunkId]): Future[Seq[ChunkWithId]] = {
      val segments = chunksMap.get(partition)
      val segment = segments.get(segmentId)
      val chunks = segment.values().asScala
      Future(chunks.filter(i => chunkIds.contains(i.chunkId)).toSeq)
    }

  }

  trait MapSummaryStore extends SummaryStore {
    val summaryMap = new java.util.HashMap[Any, java.util.TreeMap[Any, (SegmentVersion, SegmentSummary)]]()

    override def compareAndSwapSummary(projection: Projection,
                                       partition: Any,
                                       segment: Any,
                                       oldVersion: Option[SegmentVersion],
                                       segmentVersion: SegmentVersion,
                                       segmentSummary: SegmentSummary): Future[Boolean] = {
      var segments = summaryMap.get(partition)
      if (segments == null) {
        segments = new java.util.TreeMap[Any, (SegmentVersion, SegmentSummary)]()
        summaryMap.put(partition, segments)
      }
      val vAndS = segments.get(segment)
      oldVersion match {
        case Some(v) => {
          if (vAndS._1.equals(v)) {
            segments.put(segment, (segmentVersion, segmentSummary))
            Future(true)
          } else Future(false)
        }
        case None => {
          if (vAndS == null) {
            segments.put(segment, (segmentVersion, segmentSummary))
            Future(true)
          } else Future(false)
        }
      }

    }

    override def readSegmentSummary(projection: Projection,
                                    partition: Any,
                                    segment: Any):
    Future[Option[(SegmentVersion, SegmentSummary)]] = {
      val segments = summaryMap.get(partition)
      val v = if (segments != null) {
        val s = segments.get(segment)
        if (s != null) {
          val (version, summary: SegmentSummary) = s
          Future(Some((version, summary.asInstanceOf[SegmentSummary])))
        } else Future(None)
      } else Future(None)
      v
    }

    override def newVersion: SegmentVersion = UUID.randomUUID()
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
          Await.result(mapColumnStore.flushToSegment(flush.projection, flush.partition, flush.segment, flush), 100 seconds)
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

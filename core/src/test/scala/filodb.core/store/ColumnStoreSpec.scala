package filodb.core.store

import java.util.UUID

import filodb.core.Setup._
import filodb.core.Types.{ChunkId, ColumnId}
import filodb.core.metadata._
import filodb.core.query.SegmentScan
import filodb.core.reprojector.Reprojector
import filodb.core.reprojector.Reprojector.SegmentFlush
import filodb.core.store.ColumnStoreSpec.MapColumnStore
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.velvia.filo.TupleRowReader

import scala.collection.immutable.TreeMap
import scala.concurrent.{Await, Future}

object ColumnStoreSpec {

  import scala.concurrent.ExecutionContext.Implicits.global

  trait MapChunkStore extends ChunkStore {
    var chunksMap: TreeMap[Any, TreeMap[Any, TreeMap[ChunkId, ChunkWithMeta]]] = null

    override def appendChunk(projection: Projection,
                             partition: Any,
                             segment: Any,
                             chunk: ChunkWithMeta): Future[Boolean] = {
      if (chunksMap == null) {
        chunksMap = new TreeMap[Any, TreeMap[Any, TreeMap[ChunkId, ChunkWithMeta]]]()(projection.partitionType.ordering.asInstanceOf[Ordering[Any]])
      }
      val segmentsRes = chunksMap.get(partition)
      var segments = segmentsRes match {
        case Some(segments) => segments
        case None => {
          val segments = new TreeMap[Any, TreeMap[ChunkId, ChunkWithMeta]]()(projection.segmentType.ordering.asInstanceOf[Ordering[Any]])
          chunksMap += (partition -> segments)
          segments
        }
      }
      val chunksRes = segments.get(segment)
      var chunks = chunksRes match {
        case Some(chunks) => chunks
        case None => {
          val chunks = new TreeMap[ChunkId, ChunkWithMeta]()
          segments += (segment -> chunks)
          chunks
        }
      }
      chunks += (chunk.chunkId -> chunk)
      segments += (segment -> chunks)
      chunksMap += (partition -> segments)
      Future(true)
    }

    override def getAllChunksForSegments(projection: Projection,
                                         partition: Any,
                                         segmentRange: KeyRange[_],
                                         columns: Seq[ColumnId]): Future[Seq[(Any, Seq[ChunkWithMeta])]] = {
      val segmentRes = chunksMap.get(partition)
      val chunks = segmentRes match {
        case Some(segments) => segments.range(segmentRange.start, segmentRange.end).map { case (segmentId, chunkMap) =>
          segmentId -> chunkMap.map(_._2).toSeq
        }

        case None => Seq.empty[(Any, Seq[ChunkWithMeta])]
      }
      Future(chunks.toSeq)
    }

    override def getSegmentChunks(projection: Projection,
                                  partition: Any,
                                  segmentId: Any,
                                  columns: Seq[ColumnId],
                                  chunkIds: Seq[ChunkId]): Future[Seq[ChunkWithId]] = {
      val segmentRes = chunksMap.get(partition)
      val chunks = segmentRes match {
        case Some(segments) => {
          segments.get(segmentId).fold(Seq.empty[ChunkWithId]) { case segment =>
            segment.map(_._2).toSeq
          }
        }
        case None => Seq.empty[ChunkWithId]
      }
      Future(chunks)
    }

  }

  trait MapSummaryStore extends SummaryStore {
    var summaryMap: TreeMap[Any, TreeMap[Any, (SegmentVersion, SegmentSummary)]] = null

    override def compareAndSwapSummary(projection: Projection,
                                       partition: Any,
                                       segment: Any,
                                       oldVersion: Option[SegmentVersion],
                                       segmentVersion: SegmentVersion,
                                       segmentSummary: SegmentSummary): Future[Boolean] = {
      if (summaryMap == null) {
        summaryMap = new TreeMap[Any, TreeMap[Any, (SegmentVersion, SegmentSummary)]]()(projection.partitionType.ordering.asInstanceOf[Ordering[Any]])
      }
      val segmentRes = summaryMap.get(partition)
      var segments = segmentRes match {
        case Some(segments) => segments
        case None => {
          val segments = new TreeMap[Any, (SegmentVersion, SegmentSummary)]()(projection.segmentType.ordering.asInstanceOf[Ordering[Any]])
          summaryMap += (partition -> segments)
          segments
        }
      }
      val summaryRes = segments.get(segment)
      val isSet = summaryRes match {
        case Some(vAndS) => {
          if (oldVersion.isEmpty) false
          else {
            if (oldVersion.get.equals(vAndS._1)) {
              segments += segment -> Tuple2(segmentVersion, segmentSummary)
              true
            } else false
          }
        }
        case None => {
          if (oldVersion.isEmpty) {
            segments += segment -> Tuple2(segmentVersion, segmentSummary)
            true
          } else false
        }
      }
      summaryMap += (partition -> segments)
      Future(isSet)
    }

    override def readSegmentSummary(projection: Projection,
                                    partition: Any,
                                    segment: Any):
    Future[Option[(SegmentVersion, SegmentSummary)]] = {
      if (summaryMap != null) {
        val segmentRes = summaryMap.get(partition)
        val v = segmentRes flatMap { case segments => segments.get(segment) }
        Future(v)
      } else {
        summaryMap = new TreeMap[Any, TreeMap[Any, (SegmentVersion, SegmentSummary)]]()(projection.partitionType.ordering.asInstanceOf[Ordering[Any]])
        Future(None)
      }
    }

    override def newVersion: SegmentVersion = UUID.randomUUID()
  }

  class MapColumnStore extends ColumnStore with MapChunkStore with MapSummaryStore

}

class ColumnStoreSpec extends FunSpec with Matchers with BeforeAndAfter with ScalaFutures {

  import scala.concurrent.duration._

  def flushPartitions(mapColumnStore: MapColumnStore, partitions: Seq[(Any, Seq[SegmentFlush])]): Seq[Seq[Boolean]] = {
    partitions.map { case (p, flushes) =>
      flushes.map { flush =>
        Await.result(mapColumnStore.flushToSegment(flush.projection, flush.partition, flush.segment, flush), 100 seconds)
      }
    }
  }

  def checkResults(results: Seq[Seq[Boolean]]): Unit = {
    results.foreach(seq => seq.foreach { r: Boolean =>
      r should be(true)
    })
  }

  describe("Concurrent flushes") {
    it("should NOT allow concurrent flushes to write against the same summary version") {
      import scala.concurrent.ExecutionContext.Implicits.global
      val mapColumnStore = new MapColumnStore()
      val rows = names.map(TupleRowReader)
      val partitions = Reprojector.project(projection, rows).toSeq

      partitions.length should be(2)
      val results = flushPartitions(mapColumnStore, partitions)
      checkResults(results)


      val rows1 = names2.map(TupleRowReader)
      val partitions1 = Reprojector.project(projection, rows1).toSeq
      partitions1.length should be(2)
      val flush1 = partitions1.head._2.head

      val rows2 = names3.map(TupleRowReader)
      val partitions2 = Reprojector.project(projection, rows2).toSeq
      partitions2.length should be(2)
      val flush2 = partitions2.head._2.head

      flush1.partition should be(flush2.partition)
      flush1.segment should be(flush2.segment)

      val (v1, v2, s1, s2) = Await.result(for {
      //now read the segment summary
        (version1, summary1) <- mapColumnStore.getVersionAndSummaryWithDefaults(projection, flush1.partition, flush1.segment)
        (version2, summary2) <- mapColumnStore.getVersionAndSummaryWithDefaults(projection, flush2.partition, flush2.segment)
      } yield (version1, version2, summary1, summary2), 100 seconds)
      //check we got the same version
      v1.get should be(v2.get)

      val (result1, result2) = Await.result(for {
        newChunk1 <- mapColumnStore.newChunkFromSummary(projection, flush1.partition, flush1.segment, flush1, s1)
        newChunk2 <- mapColumnStore.newChunkFromSummary(projection, flush2.partition, flush2.segment, flush2, s2)
        newSummary1 = s1.withKeys(newChunk1.chunkId, newChunk1.keys)
        newSummary2 = s2.withKeys(newChunk2.chunkId, newChunk2.keys)

        r1 <- mapColumnStore.compareAndSwapSummaryAndChunk(projection,
          flush1.partition, flush1.segment, v1, mapColumnStore.newVersion, newChunk1, newSummary1)
        r2 <- mapColumnStore.compareAndSwapSummaryAndChunk(projection,
          flush2.partition, flush2.segment, v2, mapColumnStore.newVersion, newChunk2, newSummary2)

      } yield (r1, r2), 100 seconds)
      result1 should be(true)
      result2 should be(false)

    }
  }



  describe("Store and read rows") {
    it("should store and read one flush properly") {
      val mapColumnStore = new MapColumnStore()

      val rows = names.map(TupleRowReader)
      val partitions = Reprojector.project(projection, rows).toSeq

      partitions.length should be(2)
      partitions.head._1 should be("US")
      partitions.last._1 should be("UK")
      val results = flushPartitions(mapColumnStore, partitions)
      checkResults(results)

      val segments = Await.result(mapColumnStore.readSegments(projection, "US",
        projection.schema.map(i => i.name), KeyRange("A", "Z")), 10 seconds)

      segments.length should be(2)
      val scan = new SegmentScan(segments.head)
      scan.hasMoreRows should be(true)
      val threeReaders = scan.getMoreRows(3)
      scan.hasMoreRows should be(false)
      val reader = threeReaders.head
      reader.getString(0) should be("US")
      reader.getString(1) should be("NY")
      reader.getString(2) should be("Ndamukong")
      reader.getLong(4) should be(28)

      val scan2 = new SegmentScan(segments.last)
      scan2.hasMoreRows should be(true)
      val threeMore = scan2.getMoreRows(3)
      val reader1 = threeMore.last
      reader1.getString(0) should be("US")
      reader1.getString(1) should be("SF")
      reader1.getString(2) should be("Khalil")
      reader1.getLong(4) should be(24)

    }

    it("should store and read data from multiples flushes properly with overrides") {
      val mapColumnStore = new MapColumnStore()

      val rows = names.map(TupleRowReader)
      val rows2 = names2.map(TupleRowReader)
      val partitions = Reprojector.project(projection, rows).toSeq
      val partitions2 = Reprojector.project(projection, rows2).toSeq
      partitions.length should be(2)
      partitions2.length should be(2)
      val results = flushPartitions(mapColumnStore, partitions)
      checkResults(results)
      val results2 = flushPartitions(mapColumnStore, partitions2)
      checkResults(results2)

      val segments = Await.result(mapColumnStore.readSegments(projection, "US",
        projection.schema.map(i => i.name), KeyRange("A", "Z")), 10 seconds)

      segments.length should be(2)

      val scan2 = new SegmentScan(segments.last)
      scan2.hasMoreRows should be(true)
      val threeMore = scan2.getMoreRows(3)
      val reader1 = threeMore.last
      reader1.getString(0) should be("US")
      reader1.getString(1) should be("SF")
      reader1.getString(2) should be("Khalil")
      reader1.getString(3) should be("Khadri")
      reader1.getLong(4) should be(24)

    }
  }

}

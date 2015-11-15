package filodb.core.columnstore

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentSkipListMap
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scodec.bits._
import spray.caching._

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}

/**
 * A ColumnStore implementation which is entirely in memory for speed.
 * Good for testing or performance.
 * TODO: use thread-safe structures
 */
class InMemoryColumnStore(implicit val ec: ExecutionContext)
extends CachedMergingColumnStore with StrictLogging {
  import Types._
  import collection.JavaConversions._

  val segmentCache = LruCache[Segment[_]](100)

  val mergingStrategy = new AppendingChunkMergingStrategy(this)

  type ChunkKey = (ColumnId, SegmentId, ChunkID)
  type ChunkTree = ConcurrentSkipListMap[ChunkKey, ByteBuffer]
  type RowMapTree = ConcurrentSkipListMap[SegmentId, (ByteBuffer, ByteBuffer, Int)]
  type SegInfoTree = ConcurrentSkipListMap[ByteVector, (ByteVector, Int)]

  val EmptyChunkTree = new ChunkTree(Ordering[ChunkKey])
  val EmptyRowMapTree = new RowMapTree(Ordering[SegmentId])
  val EmptySegInfoTree = new SegInfoTree(Ordering[ByteVector])

  val chunkDb = new HashMap[(TableName, PartitionKey, Int), ChunkTree]
  val rowMaps = new HashMap[(TableName, PartitionKey, Int), RowMapTree]
  val segInfos = new HashMap[(TableName, PartitionKey, Int), (Long, SegInfoTree)]

  def initializeProjection(projection: Projection): Future[Response] = Future.successful(Success)

  def clearProjectionDataInner(projection: Projection): Future[Response] = Future {
    chunkDb.keys.collect { case key @ (ds, _, _) if ds == projection.dataset => chunkDb remove key }
    rowMaps.keys.collect { case key @ (ds, _, _) if ds == projection.dataset => rowMaps remove key }
    segInfos.keys.collect { case key @ (ds, _, _) if ds == projection.dataset => segInfos remove key }
    Success
  }

  def writeChunks(dataset: TableName,
                  partition: PartitionKey,
                  version: Int,
                  segmentId: SegmentId,
                  chunks: Iterator[(ColumnId, ChunkID, ByteBuffer)]): Future[Response] = Future {
    val chunkTree = chunkDb.synchronized {
      chunkDb.getOrElseUpdate((dataset, partition, version), new ChunkTree(Ordering[ChunkKey]))
    }
    chunks.foreach { case (colId, chunkId, bytes) =>
      chunkTree.put((colId, segmentId, chunkId), bytes)
    }
    Success
  }

  def writeChunkRowMap(dataset: TableName,
                       partition: PartitionKey,
                       version: Int,
                       segmentId: SegmentId,
                       chunkRowMap: ChunkRowMap): Future[Response] = Future {
    val rowMapTree = rowMaps.synchronized {
      rowMaps.getOrElseUpdate((dataset, partition, version), new RowMapTree(Ordering[SegmentId]))
    }
    val (chunkIds, rowNums) = chunkRowMap.serialize()
    rowMapTree.put(segmentId, (chunkIds, rowNums, chunkRowMap.nextChunkId))
    Success
  }

  def readChunks[K](columns: Set[ColumnId],
                    keyRange: KeyRange[K],
                    version: Int): Future[Seq[ChunkedData]] = Future {
    val chunkTree = chunkDb.getOrElse((keyRange.dataset, keyRange.partition, version),
                                      EmptyChunkTree)
    logger.debug(s"Reading chunks from columns $columns, keyRange $keyRange, version $version")
    for { column <- columns.toSeq } yield {
      val startKey = (column, keyRange.binaryStart, 0)
      val endKey   = if (keyRange.endExclusive) { (column, keyRange.binaryEnd, 0) }
                     else                       { (column, keyRange.binaryEnd, Int.MaxValue) }
      val it = chunkTree.subMap(startKey, true, endKey, !keyRange.endExclusive).entrySet.iterator
      val chunkList = it.toSeq.map { entry =>
        val (colId, segmentId, chunkId) = entry.getKey
        (segmentId, chunkId, entry.getValue)
      }
      ChunkedData(column, chunkList)
    }
  }

  def readChunkRowMaps[K](keyRange: KeyRange[K], version: Int):
      Future[Seq[(SegmentId, BinaryChunkRowMap)]] = Future {
    val rowMapTree = rowMaps.getOrElse((keyRange.dataset, keyRange.partition, version), EmptyRowMapTree)
    val it = rowMapTree.subMap(keyRange.binaryStart, true,
                               keyRange.binaryEnd, !keyRange.endExclusive).entrySet.iterator
    it.toSeq.map { entry =>
      val (chunkIds, rowNums, nextChunkId) = entry.getValue
      (entry.getKey, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId))
    }
  }

  def scanChunkRowMaps(dataset: TableName,
                       version: Int,
                       partitionFilter: (PartitionKey => Boolean),
                       params: Map[String, String]): Future[Iterator[ChunkMapInfo]] = Future {
    val parts = rowMaps.keys.filter { case (ds, partition, ver) => ds == dataset && ver == version }
    val maps = parts.toIterator.flatMap { case key @ (_, partition, _) =>
                 rowMaps(key).keySet.iterator.map { segId =>
                   val (chunkIds, rowNums, nextChunkId) = rowMaps(key).get(segId)
                   (partition, segId, new BinaryChunkRowMap(chunkIds, rowNums, nextChunkId))
                 }
               }
    maps.toIterator
  }

  def readPartitionSegments[K](projection: RichProjection[K],
                               version: Int,
                               partition: PartitionKey): Future[(Long, Seq[SegmentInfo[K]])] = Future {
    val (uuid, rawSegInfos) = segInfos.getOrElse((projection.dataset.name, partition, version),
                                                 (0L, EmptySegInfoTree))
    (uuid, rawSegInfos.entrySet.iterator.toSeq.map { entry =>
             SegmentInfo(projection.helper.fromSegmentBinary(entry.getKey),
                         projection.helper.fromSegmentBinary(entry.getValue._1),
                         entry.getValue._2)
           })
  }

  // NOTE: It is up to the application to keep the ChunkRowMaps and the partition segments in sync
  // at least in this implementation.
  def updatePartitionSegments[K](projection: RichProjection[K],
                                 version: Int,
                                 partition: PartitionKey,
                                 prevUuid: Long,
                                 newSegmentInfos: Seq[SegmentInfo[K]]): Future[Response] = Future {
    segInfos.synchronized {
      val (origUuid, segInfoTree) = segInfos.getOrElse((projection.dataset.name, partition, version),
                                                        (0L, new SegInfoTree(Ordering[ByteVector])))
      if (prevUuid != origUuid) {
        logger.info(s"Stored UUID $origUuid does not match passed UUID $prevUuid, beware multiple writers!")
        NotApplied
      } else {
        val helper = projection.helper
        newSegmentInfos.foreach { case SegmentInfo(start, end, numRows) =>
          segInfoTree.put(helper.toSegmentBinary(start), (helper.toSegmentBinary(end), numRows))
        }

        // Compute new Uuid.  TODO - find a better algorithm, is this really unique?
        val newUuid = java.util.UUID.randomUUID.getMostSignificantBits

        segInfos((projection.dataset.name, partition, version)) = (newUuid, segInfoTree)
        SegmentsUpdated(newUuid)
      }
    }
  }

  // InMemoryColumnStore is just on one node, so return no splits for now.
  // TODO: achieve parallelism by splitting on a range of partitions.
  def getScanSplits(dataset: TableName,
                    params: Map[String, String]): Seq[Map[String, String]] = Seq(Map.empty)

  def bbToHex(bb: ByteBuffer): String = DatatypeConverter.printHexBinary(bb.array)
}
package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentSkipListMap, ConcurrentNavigableMap}
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.metadata.{Column, Projection, RichProjection}

/**
 * A ColumnStore implementation which is entirely in memory for speed.
 * Good for testing or performance.
 *
 * NOTE: This implementation effectively only works on a single node.
 * We would need, for example, a Spark-specific implementation which can
 * know how to distribute data, or at least keep track of different nodes,
 * TODO: use thread-safe structures
 */
class InMemoryColumnStore(val readEc: ExecutionContext)(implicit val ec: ExecutionContext)
extends ColumnStore with InMemoryColumnStoreScanner with StrictLogging {
  import Types._
  import collection.JavaConversions._

  logger.info("Starting InMemoryColumnStore...")

  val chunkBatchSize = 256

  val chunkDb = new HashMap[(DatasetRef, BinaryPartition, Int), ChunkTree]
  val indices = new HashMap[(DatasetRef, BinaryPartition, Int), IndexTree]

  def initializeProjection(projection: Projection): Future[Response] = Future.successful(Success)

  def clearProjectionData(projection: Projection): Future[Response] = Future {
    chunkDb.keys.collect { case key @ (ds, _, _) if ds == projection.dataset => chunkDb remove key }
    indices.keys.collect { case key @ (ds, _, _) if ds == projection.dataset => indices remove key }
    Success
  }

  def dropDataset(dataset: DatasetRef): Future[Response] = {
    chunkDb.synchronized {
      chunkDb.retain { case ((ds, _, _), _) => ds != dataset }
    }
    indices.synchronized {
      indices.retain { case ((ds, _, _), _) => ds != dataset }
    }
    Future.successful(Success)
  }

  def appendSegment(projection: RichProjection,
                    segment: ChunkSetSegment,
                    version: Int): Future[Response] = Future {
    val segmentId = segment.segmentId
    val dbKey = (projection.datasetRef, segment.binaryPartition, version)

    // Add chunks
    val chunkTree = chunkDb.synchronized {
      chunkDb.getOrElseUpdate(dbKey, new ChunkTree(Ordering[ChunkKey]))
    }
    for { chunkSet       <- segment.chunkSets
          (colId, bytes) <- chunkSet.chunks } {
      chunkTree.put((colId, segmentId, chunkSet.info.id), bytes)
    }

    // Add index objects - not serialized for speed
    val indexTree = indices.synchronized {
      indices.getOrElseUpdate(dbKey, new IndexTree(Ordering[SegmentId]))
    }
    val segmentIndex = Option(indexTree.get(segmentId)).getOrElse(Nil)
    indexTree.put(segmentId, segmentIndex ++ segment.infosAndSkips)

    Success
  }

  // Add an efficient scanSegments implementation here, which can avoid much of the async
  // cruft unnecessary for in-memory stuff
  override def scanSegments(projection: RichProjection,
                            columns: Seq[Column],
                            version: Int,
                            method: ScanMethod): Future[Iterator[Segment]] = {
    for { indices <- scanIndices(projection, version, method) }
    yield {
      indices.map { case SegmentIndex(binPart, segId, part, segmentKey, infosAndSkips) =>
        val segInfo = SegmentInfo(part, segmentKey)
        val chunkInfos = ChunkSetInfo.collectSkips(infosAndSkips)
        val segment = new RowReaderSegment(projection, segInfo, chunkInfos, columns)
        stats.incrReadSegments(1)
        for { column <- columns } {
          val colName = column.name
          val chunkTree = chunkDb.getOrElse((projection.datasetRef, binPart, version), EmptyChunkTree)
          chunkTree.subMap((colName, segId, 0), true, (colName, segId, Int.MaxValue), true)
                   .entrySet.iterator.foreach { entry =>
            val (_, _, chunkId) = entry.getKey
            // NOTE: extremely important to duplicate the ByteBuffer, because of mutable state / multi threading
            segment.addChunk(chunkId, colName, entry.getValue.duplicate)
          }
        }
        segment
      }
    }
  }

  def shutdown(): Unit = {}

  def reset(): Unit = {
    chunkDb.clear()
    indices.clear()
  }

  // InMemoryColumnStore is just on one node, so return no splits for now.
  // TODO: achieve parallelism by splitting on a range of partitions.
  def getScanSplits(dataset: DatasetRef, splitsPerNode: Int): Seq[ScanSplit] =
    Seq(InMemoryWholeSplit)

  def bbToHex(bb: ByteBuffer): String = DatatypeConverter.printHexBinary(bb.array)
}

// TODO(velvia): Implement real splits?
case object InMemoryWholeSplit extends ScanSplit {
  def hostnames: Set[String] = Set.empty
}

trait InMemoryColumnStoreScanner extends ColumnStoreScanner {
  import Types._
  import collection.JavaConversions._

  type ChunkKey = (ColumnId, SegmentId, ChunkID)
  type ChunkTree = ConcurrentSkipListMap[ChunkKey, ByteBuffer]
  type IndexTree = ConcurrentSkipListMap[SegmentId, ChunkSetInfo.ChunkInfosAndSkips]
  type IndexTreeLike = ConcurrentNavigableMap[SegmentId, ChunkSetInfo.ChunkInfosAndSkips]

  val EmptyChunkTree = new ChunkTree(Ordering[ChunkKey])
  val EmptyIndexTree = new IndexTree(Ordering[SegmentId])

  def chunkDb: HashMap[(DatasetRef, BinaryPartition, Int), ChunkTree]
  def indices: HashMap[(DatasetRef, BinaryPartition, Int), IndexTree]

  def readChunks(dataset: DatasetRef,
                 columns: Set[ColumnId],
                 keyRange: BinaryKeyRange,
                 version: Int)(implicit ec: ExecutionContext): Future[Seq[ChunkedData]] = {
    val chunkTree = chunkDb.getOrElse((dataset, keyRange.partition, version),
                                      EmptyChunkTree)
    logger.debug(s"Reading chunks from columns $columns, keyRange $keyRange, version $version")
    val chunks = for { column <- columns.toSeq } yield {
      val startKey = (column, keyRange.start, 0)
      val endKey   = if (keyRange.endExclusive) { (column, keyRange.end, 0) }
                     else                       { (column, keyRange.end, Int.MaxValue) }
      val it = chunkTree.subMap(startKey, true, endKey, !keyRange.endExclusive).entrySet.iterator
      val chunkList = it.toSeq.map { entry =>
        val (colId, segmentId, chunkId) = entry.getKey
        // NOTE: extremely important to duplicate the ByteBuffer, because of mutable state / multi threading
        (segmentId, chunkId, entry.getValue.duplicate)
      }
      ChunkedData(column, chunkList)
    }
    Future.successful(chunks)
  }

  def singlePartScan(projection: RichProjection, version: Int, partition: Any):
    Iterator[(BinaryPartition, IndexTreeLike)] = {
    val binPart = projection.partitionType.toBytes(partition.asInstanceOf[projection.PK])
    Seq((binPart, indices.getOrElse((projection.datasetRef, binPart, version), EmptyIndexTree))).
      toIterator
  }

  def multiPartScan(projection: RichProjection, version: Int, partitions: Seq[Any]):
    Iterator[(BinaryPartition, IndexTreeLike)] = {
    partitions.toIterator.collect { case (partition)  => {
      val binPart = projection.partitionType.toBytes(partition.asInstanceOf[projection.PK])
       (binPart, indices.getOrElse((projection.datasetRef, binPart, version), EmptyIndexTree))
    }}
  }

  def multiPartRangeScan(projection: RichProjection, version: Int, keyRanges: Seq[KeyRange[_, _]]):
    Iterator[(BinaryPartition, IndexTreeLike)] = {
    keyRanges.toIterator.collect { case (range) => {
      val binKR = projection.toBinaryKeyRange(range)
      val indexTree = indices.getOrElse((projection.datasetRef, binKR.partition, version),
                                        EmptyIndexTree)
      val subMap = indexTree.subMap(binKR.start, true, binKR.end, !binKR.endExclusive)
      (binKR.partition, subMap)
    }}
  }

  def singlePartRangeScan(projection: RichProjection, version: Int, k: KeyRange[_, _]):
    Iterator[(BinaryPartition, IndexTreeLike)] = {
    val binKR = projection.toBinaryKeyRange(k)
    val indexTree = indices.getOrElse((projection.datasetRef, binKR.partition, version),
                                       EmptyIndexTree)
    val subMap = indexTree.subMap(binKR.start, true, binKR.end, !binKR.endExclusive)
    Iterator.single((binKR.partition, subMap))
  }

  def filteredPartScan(projection: RichProjection,
                       version: Int,
                       split: ScanSplit,
                       filterFunc: Any => Boolean): Iterator[(BinaryPartition, IndexTreeLike)] = {
    val binParts = indices.keysIterator.collect { case (ds, binPart, ver) if
      ds == projection.datasetRef && ver == version => binPart }
    binParts.filter { binPart => filterFunc(projection.partitionType.fromBytes(binPart))
            }.map { binPart => (binPart, indices((projection.datasetRef, binPart, version))) }
  }

  def scanIndices(projection: RichProjection,
                  version: Int,
                  method: ScanMethod)
                 (implicit ec: ExecutionContext):
    Future[Iterator[SegmentIndex[projection.PK, projection.SK]]] = {
    val partAndMaps = method match {
      case SinglePartitionScan(partition) => singlePartScan(projection, version, partition)
      case SinglePartitionRangeScan(k)    => singlePartRangeScan(projection, version, k)

      case MultiPartitionScan(partitions) => multiPartScan(projection, version, partitions)
      case MultiPartitionRangeScan(keyRanges) => multiPartRangeScan(projection, version, keyRanges)

      case FilteredPartitionScan(split, filterFunc) =>
        filteredPartScan(projection, version, split, filterFunc)

      case FilteredPartitionRangeScan(split, segRange, filterFunc) =>
        val binSegRange = projection.toBinarySegRange(segRange)
        filteredPartScan(projection, version, split, filterFunc).map { case (binPart, indexTree) =>
          (binPart, indexTree.subMap(binSegRange.start, true, binSegRange.end, true))
        }
    }
    val segIndices = partAndMaps.flatMap { case (binPart, indexTree) =>
      indexTree.entrySet.iterator.map { entry =>
        toSegIndex(projection, binPart, entry.getKey, entry.getValue)
      }
    }
    Future.successful(segIndices)
  }
}
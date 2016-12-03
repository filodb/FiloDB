package filodb.core.store

import com.typesafe.scalalogging.slf4j.StrictLogging
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentSkipListMap, ConcurrentNavigableMap}
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.HashMap
import scala.concurrent.{ExecutionContext, Future}
import scodec.bits.ByteVector

import filodb.core._
import filodb.core.binaryrecord.BinaryRecord
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

    if (segment.chunkSets.isEmpty) { NotApplied }
    else {
      // Add chunks
      val chunkTree = chunkDb.synchronized {
        chunkDb.getOrElseUpdate(dbKey, new ChunkTree(Ordering[ChunkKey]))
      }
      for { chunkSet       <- segment.chunkSets
            (colId, bytes) <- chunkSet.chunks } {
        val keyBytes = keyToVector(chunkSet.info.firstKey)
        chunkTree.put((colId, segmentId, keyBytes, chunkSet.info.id), bytes)
      }

      // Add index objects - not serialized for speed
      val indexTree = indices.synchronized {
        indices.getOrElseUpdate(dbKey, new IndexTree(Ordering[SegmentId]))
      }
      val segmentIndex = Option(indexTree.get(segmentId)).getOrElse(Nil)
      indexTree.put(segmentId, segmentIndex ++ segment.infosSkipsFilters)

      Success
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

  type ChunkKey = (ColumnId, SegmentId, ByteVector, ChunkID)
  type ChunkTree = ConcurrentSkipListMap[ChunkKey, ByteBuffer]
  type IndexTree = ConcurrentSkipListMap[SegmentId, ChunkSetInfo.IndexAndFilterSeq]
  type IndexTreeLike = ConcurrentNavigableMap[SegmentId, ChunkSetInfo.IndexAndFilterSeq]

  val EmptyChunkTree = new ChunkTree(Ordering[ChunkKey])
  val EmptyIndexTree = new IndexTree(Ordering[SegmentId])

  def chunkDb: HashMap[(DatasetRef, BinaryPartition, Int), ChunkTree]
  def indices: HashMap[(DatasetRef, BinaryPartition, Int), IndexTree]

  protected def keyToVector(binRec: BinaryRecord): ByteVector = ByteVector(binRec.toSortableBytes())

  def readChunks(dataset: DatasetRef,
                 version: Int,
                 columns: Seq[ColumnId],
                 partition: Types.BinaryPartition,
                 segment: Types.SegmentId,
                 key1: (BinaryRecord, Types.ChunkID),
                 key2: (BinaryRecord, Types.ChunkID))
                (implicit ec: ExecutionContext): Future[Seq[ChunkedData]] = {
    val chunkTree = chunkDb.getOrElse((dataset, partition, version),
                                      EmptyChunkTree)
    logger.debug(s"Reading chunks from columns $columns, $partition/$segment, ($key1, $key2)")
    val chunks = for { column <- columns.toSeq } yield {
      val startKey = (column, segment, keyToVector(key1._1), key1._2)
      val endKey   = (column, segment, keyToVector(key2._1), key2._2)
      val it = chunkTree.subMap(startKey, true, endKey, true).entrySet.iterator
      val chunkList = it.toSeq.map { entry =>
        val (colId, _, _, chunkId) = entry.getKey
        // NOTE: extremely important to duplicate the ByteBuffer, because of mutable state / multi threading
        (chunkId, entry.getValue.duplicate)
      }
      ChunkedData(column, chunkList)
    }
    Future.successful(chunks)
  }

  def readFilters(dataset: DatasetRef,
                  version: Int,
                  partition: Types.BinaryPartition,
                  segment: Types.SegmentId,
                  chunkRange: (Types.ChunkID, Types.ChunkID))
                 (implicit ec: ExecutionContext): Future[Iterator[SegmentState.IDAndFilter]] = {
    val indexTree = indices.getOrElse((dataset, partition, version), EmptyIndexTree)
    val indexData = indexTree.get(segment)
    val it = Option(indexData).map(_.iterator.map { case (info, _, filter) => (info.id, filter) })
                              .getOrElse(Iterator.empty)
    Future.successful(it)
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
      case SinglePartitionRowKeyScan(part, _, _) => singlePartScan(projection, version, part)

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
        val infosSkips = entry.getValue.map { case (info, skips, _) => (info, skips) }.toArray
        toSegIndex(projection, binPart, entry.getKey, filterSkips(projection, infosSkips, method))
      }
    }
    Future.successful(segIndices)
  }
}
package filodb.core.columnstore

import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{ExecutionContext, Future}

import filodb.core._
import filodb.core.metadata.RichProjection
import filodb.core.Types.{PartitionKey, TableName}

case class SegmentMeta[K](partition: PartitionKey,
                          // start and end are the segment boundaries, fixed for pre-existing segments
                          // end is exclusive
                          var start: Option[K],
                          var end: Option[K],
                          var numRows: Int,
                          var updated: Boolean = false)

/**
 * A helper class to determine new segment boundaries and split existing segments as new keys are ingested.
 * The rules:
 * - Segments should be kept between the minRows and maxRows numbers
 * - Existing segment boundaries cannot be changed but may be split to keep the max from growing.
 * - New segments may be kept open
 *
 * Segment types:
 *   (Open, Open) - typical when new partition is written with not enough rows.  Encoded as minimal value
 *                  segmentID with no ending value.
 *   (Open, Closed] - Initial (Open, Open) is split due to filling up into (Open, Closed] and [Closed, Open)
 *   [Closed, Open] - when this splits it becomes -> [Closed, Closed] and [Closed, Open)
 *
 * NOTE: Open) actually gets written with an ending value, which gets updated since it is the last segment.
 *       This is to make it easier to know where to split the final segment.
 *
 * This can be used in one of two ways:
 * - To quickly determine segment boundaries from a memtable about to be flushed
 * - To dynamically adjust segment boundaries as new rows are coming in, for combining reads from an
 *   actively filling memtable, a flushing memtable, and segments on disk
 *
 * The lifecycle is that a new SegmentChopper is set up for each new memtable, but it can inherit the
 * segmentMetaMap from previous SegmentChoppers.  At the beginning, every SegmentMeta is marked with
 * updated = false, and when new keys come in, the updated SegmentMetas have updated = true.  Then, when it
 * is time to update the partition segment info, or reproject, only the updated metas are returned.
 *
 * @param segmentMetaMap a mutable Hashmap partitionKey -> sorted Seq of SegmentMeta by start key
 * @param minRowsPerSegment the minimum # rows when a segment (typically the last open one) is split
 * @param maxRowsPerSegment the max # rows allowed for a segment, it MUST be split at this point
 */
class SegmentChopper[K](projection: RichProjection[K],
                        val segmentMetaMap: SegmentChopper.SegmentMetaMap[K],
                        minRowsPerSegment: Int = SegmentChopper.DefaultMinRowsPerSegment,
                        maxRowsPerSegment: Int = SegmentChopper.DefaultMaxRowsPerSegment) {
  implicit val ordering = projection.helper.ordering
  import scala.math.Ordered._   // allows > < via implicit conversion of Ordering to Ordered
  import scala.util.control.Breaks._

  for { partition <- segmentMetaMap.keys
        meta      <- segmentMetaMap(partition) } {
    meta.updated = false
  }

  /**
   * Updates segments for one partition when given a sorted array of new sort keys.
   * Note: All new keys are counted as appends, because they are in fact stored that way, and we want the
   * numRows to reflect storage cost and not the logical # of rows.
   * The result is that the segments will then take the min and max policies into account.
   *
   * This function currently has a runtime of (# keys), but could in theory be improved to
   * (# segments) * log(# keys).  Keep it simple, then improve!
   *
   * @param keys a list of partition and sort keys, ordered at least by sort key within each partition.
   */
  def insertKeysForPartition(partition: PartitionKey, keys: Seq[K]): Unit = {
    if (keys.size == 0) {}
    //    Note that the start of the first segment is always open (None), and
    //    the end of the last segment is also always open, so segments should cover every key.
    //    Also, both the segmentMetaMap and the keys are in sorted order, so no need for binary search.
    else {
      var segIndex = 0
      var keyIndex = 0
      val segments = segmentMetaMap.getOrElseUpdate(partition, ArrayBuffer())
      // Empty partition, create an initial empty Segments
      if (segments.isEmpty) segments += SegmentMeta[K](partition, None, None, 0, updated = true)
      val lastSegIndex = segments.length - 1

      // Loop through keys, until we come to end of a segment.... unless we're already at last segment
      // NOTE: This part could definitely be sped up by binary-searching through the keys
      breakable {
        while (segIndex < lastSegIndex) {
          val segmentEnd = segments(segIndex).end.get
          while (keys(keyIndex) < segmentEnd) {
            segments(segIndex).numRows += 1
            segments(segIndex).updated = true
            // TODO: check if we need to split this segment, ie if numRows >= maxRowsPerSegment
            keyIndex += 1
            if (keyIndex >= keys.size) break
          }
          segIndex += 1
        }
      }

      // Now at last open segment.... add to it and split if needed
      if (keyIndex < keys.size) {
        // This has to be the last statement, due to recursion
        splitFinalSegment(segments, keys, keyIndex)
      }
    }
  }

  /**
   * Splits the final SegmentMeta if there are enough keys remaining.
   * NOTE: what is the split point?  For now, split at the highest key recorded for that segment, and
   * do not split further if incoming keys do not make it into the new segment.
   * Also adds to the number of rows the appropriate amount even if there is no splitting.
   * @param segments modify and extends this if there are enough keys remaining
   * @param keys an array sorted by K
   * @param start the index into keys at which the final segment starts.
   * @param splitSize the # of rows at which the final segment will be split into more segments
   */
  private def splitFinalSegment(segments: ArrayBuffer[SegmentMeta[K]],
                                keys: Seq[K],
                                start: Int,
                                splitSize: Int = minRowsPerSegment): Unit = {
    require(segments.nonEmpty)
    // Ingest just enough to fill up last segment.  Adjust end key.
    // NOTE: should not be possible for remainingInSegment to be 0.  Split should have happened.
    val remainingInSegment = splitSize - segments.last.numRows + 1
    val numToAdd = Math.min(remainingInSegment, keys.size - start)
    val lastKey = keys(start + numToAdd - 1)

    if (segments.last.end.map(lastKey > _).getOrElse(true)) segments.last.end = Some(lastKey)
    segments.last.numRows += numToAdd
    segments.last.updated = true

    // Split segment if we filled the last segment.  Splitting reduces number of rows by one since the end
    // key is inclusive....  then recurse on remaining keys, since some of them might belong to last segment
    if (numToAdd == remainingInSegment) {
      // Account for the fact that the last key is inclusive, so this will split it into the next segment
      segments.last.numRows -= 1
      val nextSeg = SegmentMeta(segments.last.partition,
                                segments.last.end, None,
                                1, true)
      segments += nextSeg
      insertKeysForPartition(nextSeg.partition, keys.drop(start + numToAdd))
    }
  }

  /**
   * Inserts keys from an ordered iterator of partitionKey/K, such as from MemTable.allKeys() method.
   */
  def insertOrderedKeys(keys: Iterator[(PartitionKey, K)]): Unit = {
    import Iterators._

    keys.sortedGroupBy(_._1).foreach { case (partition, sortKeys) =>
      insertKeysForPartition(partition, sortKeys.map(_._2).toSeq)
    }
  }

  /**
   * Returns only the updated segments (the origSegments are immutable except when split).
   * @return a map of partition -> SegmentInfos, for using with columnStore.updatePartitionSegments
   */
  def updatedSegments(): collection.Map[PartitionKey, Seq[SegmentInfo[K]]] = {
    segmentMetaMap.mapValues { segmentMetas =>
      segmentMetas.collect {
        case SegmentMeta(_, start, end, numRows, true) => SegmentInfo[K](start, end, numRows)
      }
    }
  }

  /**
   * Returns all of the updated segments as KeyRanges, intended for use with Reprojector.
   * @return a list of KeyRanges, sorted in ascending partition key / sortkey order.
   *         Note that the last keyRange of a partition has endExclusive = false, others true.
   */
  def keyRanges(): Seq[KeyRange[K]] = {
    implicit val helper = projection.helper
    segmentMetaMap.keys.toSeq.sorted.flatMap { partition =>
      val keyRanges = segmentMetaMap(partition).collect {
        case SegmentMeta(_, start, end, _, true) =>
          KeyRange[K](projection.dataset.name, partition, start, end, true)
      }
      keyRanges.dropRight(1) :+ keyRanges.last.copy(endExclusive = false)
    }
  }
}

object SegmentChopper extends StrictLogging {
  val DefaultMaxRowsPerSegment = 50000
  val DefaultMinRowsPerSegment = 25000

  type SegmentMetaMap[K] = HashMap[PartitionKey, ArrayBuffer[SegmentMeta[K]]]
  type SegmentUuidMap = HashMap[PartitionKey, Long]

  /**
   * @param origSegments a list of the original segments, should be sorted in order of partitionKey and start.
   */
  def apply[K](projection: RichProjection[K],
               origSegments: Seq[(PartitionKey, SegmentInfo[K])],
               minRowsPerSegment: Int = DefaultMinRowsPerSegment,
               maxRowsPerSegment: Int = DefaultMaxRowsPerSegment): SegmentChopper[K] = {
    val segmentMeta = new SegmentMetaMap[K]
    origSegments.foreach { case (partition, SegmentInfo(start, end, numRows)) =>
      val meta = SegmentMeta(partition, start, end, numRows)
      val partitionSegments = segmentMeta.getOrElseUpdate(partition, new ArrayBuffer[SegmentMeta[K]])
      partitionSegments += meta
    }
    new SegmentChopper(projection, segmentMeta, minRowsPerSegment, maxRowsPerSegment)
  }

  def toSegmentMeta[K](partition: PartitionKey, segmentInfos: Seq[SegmentInfo[K]]): Seq[SegmentMeta[K]] =
    segmentInfos.map { case SegmentInfo(start, end, numRows) =>
      SegmentMeta(partition, start, end, numRows)
    }

  /**
   * Produces a segmentMetaMap for initializing or updating a SegmentChopper.  One use case is for updating
   * a SegmentChopper after a tryUpdateSegmentInfos call fails for certain partitions.
   */
  def loadSegmentInfos[K](projection: RichProjection[K],
                          partitions: Seq[PartitionKey],
                          version: Int,
                          columnStore: CachedMergingColumnStore)
                         (implicit ec: ExecutionContext): Future[(SegmentMetaMap[K], SegmentUuidMap)] = {
    val uuidMap = new SegmentUuidMap
    val metaMap = new SegmentMetaMap[K]
    for { uuidsInfos <- Future.sequence(partitions.map { p =>
                          columnStore.readPartitionSegments(projection, version, p)
                        }) } yield {
      uuidsInfos.zip(partitions).foreach { case ((uuid, segInfos), partition) =>
        uuidMap(partition) = uuid
        metaMap(partition) = ArrayBuffer(toSegmentMeta(partition, segInfos) :_*)
      }
      (metaMap, uuidMap)
    }
  }

  /**
   * Attempts to update segmentInfos for a whole set of partitions based on a SegmentChopper and previous
   * UUIDs for each partition.  CompareAndSwap is done on write.
   * @return a Map(PartitionKey -> UUID) containing the newer UUIDs for successfully updated partitions.
   *         Missing partition keys for which the compare and swap failed (not I/O failure, which would
   *         result in a Future failure, but rather that the UUIDs did not agree).  Segment info must be
   *         reloaded for these partition keys.
   */
  def tryUpdateSegmentInfos[K](projection: RichProjection[K],
                               partitions: Seq[PartitionKey],
                               version: Int,
                               chopper: SegmentChopper[K],
                               uuidMap: SegmentUuidMap,
                               columnStore: CachedMergingColumnStore)
                              (implicit ec: ExecutionContext): Future[Map[PartitionKey, Long]] = {
    val infoMap = chopper.updatedSegments()
    val filteredPartitions = partitions.filter { p => infoMap(p).nonEmpty }
    for { responses <- Future.sequence(filteredPartitions.map { p =>
                         columnStore.updatePartitionSegments(projection, version, p, uuidMap(p), infoMap(p))
                       }) } yield {
      logger.debug(s"Responses for updating segment infos for partitions $partitions: $responses")
      responses.zip(filteredPartitions).collect {
        case (SegmentsUpdated(newUuid), part) => part -> newUuid }.toMap
    }
  }
}
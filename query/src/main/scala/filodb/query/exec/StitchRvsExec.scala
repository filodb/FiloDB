package filodb.query.exec

import scala.collection.mutable

import monix.eval.Task
import monix.reactive.Observable

import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.{vectors => bv, RowReader}
import filodb.query._
import filodb.query.Query.qLogger

/**
 * Decides what StitchRvsExec emits when more than one child has a non-NaN value for the same range vector key
 * at the same timestamp.
 */
sealed trait StitchOverlapMerge

object StitchOverlapMerge {
  /**
   * Default. Children are expected to hold the same data at overlapping timestamps; the value is emitted only if
   * all non-NaN values are (approximately) equal, NaN otherwise.
   */
  case object NaNOnConflict extends StitchOverlapMerge

  /**
   * Children hold disjoint partial results for overlapping timestamps (for example, partial aggregates computed over
   * disjoint time-partitions of the same lookback window) that need to be combined with the given operation.
   * NaN (or empty histogram) values are treated as "no data" from that child and are skipped.
   * Histogram results support only Sum.
   */
  case object Sum extends StitchOverlapMerge
  case object Max extends StitchOverlapMerge
  case object Min extends StitchOverlapMerge
}

object StitchRvsExec {

  private class RVCursorImpl(
                              vectors: Iterable[RangeVectorCursor],
                              outputRange: Option[RvRange],
                              enableApproximatelyEqualCheck: Boolean,
                              toleranceNumDecimal: Int,
                              overlapMerge: StitchOverlapMerge,
                              isHistogram: Boolean)
    extends RangeVectorCursor {
    private val weight = math.pow(10, toleranceNumDecimal)
    private val bVectors = vectors.map(_.buffered)
    val mins = new mutable.ArrayBuffer[BufferedIterator[RowReader]](2)
    val nanResult = new NaNRowReader(0)
    private val mergedResult = new TransientRow()
    private val mergedHistResult = new TransientHistRow()
    val tsIter: BufferedIterator[Long] = outputRange match {
      case Some(RvRange(startMs, 0, endMs)) =>
        if (startMs == endMs)
          List(startMs).toIterator.buffered
        else {
          // Should never happen
          throw new IllegalStateException("Expected to a non zero step size when start and end timestamps are not same")
        }
      case Some(RvRange(startMs, stepMs, endMs)) =>
        Iterator.iterate(startMs){_ + stepMs}.takeWhile( _ <= endMs).buffered
      case None                                  => Iterator.iterate(Long.MaxValue)(_ =>Long.MaxValue).buffered
    }
    override def hasNext: Boolean = outputRange match {
                // In case outputRange is provided, the merging will honor the provided range, that is, the
                // result of metrging will have a range provided by outputRange. In case outputRange is not provided
                // i.e. None, then hasNext should be determined by the vectors it merges. The none handling is just done
                // for matching both possibilities and in reality for a given periodic series with a range, the
                // outputRange should never be none
                case Some(_)      => tsIter.hasNext
                case None         => bVectors.exists(_.hasNext)
    }

    // scalastyle:off method.length
    override def next(): RowReader = {
      // This is an n-way merge without using a heap.
      // Heap is not used since n is expected to be very small (almost always just 1 or 2)
      mins.clear()
      var minTime = Long.MaxValue
      bVectors.foreach { r =>
        if (r.hasNext) {
          val t = r.head.getLong(0)
          if (mins.isEmpty) {
            minTime = t
            mins += r
          } else if (t < minTime) {
            mins.clear()
            mins += r
            minTime = t
          } else if (t == minTime) {
            mins += r
          }
        }
      }
      if (mins.isEmpty && tsIter.isEmpty) throw new IllegalStateException("next was called when no element")
      if (minTime > tsIter.head) {
        nanResult.timestamp = tsIter.next()
        nanResult
      } else {
        if (minTime == tsIter.head)
          tsIter.next()

        if (mins.size == 1) mins.head.next()
        else {
          nanResult.timestamp = minTime
          // until we have a different indicator for "unable-to-calculate", use NaN when multiple values seen
          val minRows = mins.map(it => if (it.hasNext) it.next() else nanResult) // move iterators forward
          if (overlapMerge != StitchOverlapMerge.NaNOnConflict) {
            if (isHistogram) mergeHistograms(minTime, minRows) else mergeDoubles(minTime, minRows)
          } else {
            val minsWithoutNan = minRows.filter(!_.getDouble(1).isNaN)
            // The second condition checks if these values are equal within the tolerable limits and if yes, do not
            // emit NaN.
            // TODO: Make the second check and tolerance configurable?
            if (minsWithoutNan.size == 1) {
              minsWithoutNan.head
            } else if (minsWithoutNan.size > 1 && enableApproximatelyEqualCheck &&
              minsWithoutNan.map(x => (x.getDouble(1) * weight).toLong / weight).toSet.size == 1) {
              minsWithoutNan.head
            } else {
              nanResult
            }
          }
        }
      }

    }

    /**
     * Combines the non-NaN values seen at the same timestamp using overlapMerge. NaNs are treated as "no data"
     * from that child and are skipped. If all values are NaN, NaN is emitted.
     */
    private def mergeDoubles(timestamp: Long, rows: Iterable[RowReader]): RowReader = {
      var acc = Double.NaN
      rows.foreach { r =>
        val v = r.getDouble(1)
        if (!v.isNaN) {
          acc = if (acc.isNaN) v else overlapMerge match {
            case StitchOverlapMerge.Sum           => acc + v
            case StitchOverlapMerge.Max           => math.max(acc, v)
            case StitchOverlapMerge.Min           => math.min(acc, v)
            case StitchOverlapMerge.NaNOnConflict => throw new IllegalStateException("merge not expected")
          }
        }
      }
      mergedResult.setValues(timestamp, acc)
      mergedResult
    }

    /**
     * Histogram version of mergeDoubles. Only Sum is supported; empty histograms are treated as "no data".
     */
    private def mergeHistograms(timestamp: Long, rows: Iterable[RowReader]): RowReader = {
      val present = rows.filter(r => !r.getHistogram(1).isEmpty)
      if (present.isEmpty) nanResult
      else if (present.size == 1) present.head
      else overlapMerge match {
        case StitchOverlapMerge.Sum =>
          // MutableHistogram.apply copies the bucket values, so the child rows are not mutated
          val acc = bv.MutableHistogram(present.head.getHistogram(1))
          present.tail.foreach(r => acc.addNoCorrection(r.getHistogram(1).asInstanceOf[bv.HistogramWithBuckets]))
          mergedHistResult.setValues(timestamp, acc)
          mergedHistResult
        case other =>
          throw new IllegalArgumentException(s"StitchOverlapMerge $other is not supported for histograms")
      }
    }

    override def close(): Unit = vectors.foreach(_.close())
  }

  def stitch(v1: RangeVector, v2: RangeVector, outputRvRange: Option[RvRange]): RangeVector = {
    val rows = StitchRvsExec.merge(Seq(v1.rows(), v2.rows()), outputRvRange)
    IteratorBackedRangeVector(v1.key, rows, outputRvRange)
  }

  def merge(vectors: Iterable[RangeVectorCursor], outputRange: Option[RvRange],
            enableApproximatelyEqualCheck: Boolean = false,
            toleranceNumDecimal: Int = 10,
            overlapMerge: StitchOverlapMerge = StitchOverlapMerge.NaNOnConflict,
            isHistogram: Boolean = false): RangeVectorCursor = {
    new RVCursorImpl(vectors, outputRange, enableApproximatelyEqualCheck, toleranceNumDecimal,
      overlapMerge, isHistogram)
  }
}


/**
  * Use when data for same time series spans multiple shards, or clusters.
  *
  * @param overlapMerge what to do when more than one child has a non-NaN value at the same timestamp.
  *                     See [[StitchOverlapMerge]].
  */
final case class StitchRvsExec(queryContext: QueryContext,
                               dispatcher: PlanDispatcher,
                               outputRvRange: Option[RvRange],
                               children: Seq[ExecPlan],
                               enableApproximatelyEqualCheck: Boolean = false,
                               toleranceNumDecimal: Int = 10,
                               overlapMerge: StitchOverlapMerge = StitchOverlapMerge.NaNOnConflict)
  extends NonLeafExecPlan {
  require(children.nonEmpty)

  outputRvRange match {
    case Some(RvRange(startMs, stepMs, endMs)) =>
                            require(startMs <= endMs && stepMs > 0, "RvRange start <= end and step > 0")
    case None                                  =>
  }
  protected def args: String =
    if (overlapMerge == StitchOverlapMerge.NaNOnConflict) "" else s"overlapMerge=$overlapMerge"

  protected def composeStreaming(childResponses: Observable[(Observable[RangeVector], Int)],
                                 schemas: Observable[(ResultSchema, Int)],
                                 querySession: QuerySession): Observable[RangeVector] = ???

  protected[exec] def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    qLogger.debug(s"StitchRvsExec: Stitching results:")
    val stitched = childResponses.map(_._1).toListL.map { results =>
      val srvs = results.flatMap(_.result)
      // Needed only to pick the right value accessor when overlapping values are merged
      val isHistogram = results.find(_.resultSchema.columns.size > 1)
        .exists(_.resultSchema.columns(1).colType == ColumnType.HistogramColumn)
      val groups = srvs.groupBy(_.key.labelValues)
      groups.mapValues { toMerge =>
        val rows = StitchRvsExec.merge(toMerge.map(_.rows()), outputRvRange,
          enableApproximatelyEqualCheck, toleranceNumDecimal, overlapMerge, isHistogram)
        val key = toMerge.head.key
        IteratorBackedRangeVector(key, rows, outputRvRange)
      }.values
    }.map(Observable.fromIterable)
    Observable.fromTask(stitched).flatten
  }

}

/**
  * Range Vector Transformer version of StitchRvsExec
  */
final case class StitchRvsMapper(outputRvRange: Option[RvRange]) extends RangeVectorTransformer {

  def apply(source: Observable[RangeVector],
            querySession: QuerySession,
            limit: Int,
            sourceSchema: ResultSchema, paramResponse: Seq[Observable[ScalarRangeVector]]): Observable[RangeVector] = {
    qLogger.debug(s"StitchRvsMapper: Stitching results:")
    val stitched = source.toListL.map { rvs =>
      val groups = rvs.groupBy(_.key)
      groups.mapValues { toMerge =>
        val rows = StitchRvsExec.merge(toMerge.map(_.rows()), outputRvRange)
        val key = toMerge.head.key
        IteratorBackedRangeVector(key, rows, outputRvRange)
      }.values
    }.map(Observable.fromIterable)
    Observable.fromTask(stitched).flatten
  }

  override protected[query] def args: String = ""

  override def funcParams: Seq[FuncArgs] = Nil
}

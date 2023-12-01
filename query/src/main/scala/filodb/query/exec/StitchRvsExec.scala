package filodb.query.exec

import scala.collection.mutable

import monix.eval.Task
import monix.reactive.Observable

import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query._
import filodb.query.Query.qLogger

object StitchRvsExec {

  private class RVCursorImpl(vectors: Iterable[RangeVectorCursor], outputRange: Option[RvRange])
    extends RangeVectorCursor {
    private val bVectors = vectors.map(_.buffered)
    val mins = new mutable.ArrayBuffer[BufferedIterator[RowReader]](2)
    val nanResult = new TransientRow(0, Double.NaN)
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
          }
          else if (t < minTime) {
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
          val minsWithoutNan = minRows.filter(!_.getDouble(1).isNaN)
          if (minsWithoutNan.size == 1) minsWithoutNan.head else nanResult
        }
      }

    }

    override def close(): Unit = vectors.foreach(_.close())
  }

  def stitch(v1: RangeVector, v2: RangeVector, outputRvRange: Option[RvRange]): RangeVector = {
    val rows = StitchRvsExec.merge(Seq(v1.rows(), v2.rows()), outputRvRange)
    IteratorBackedRangeVector(v1.key, rows, outputRvRange)
  }

  def merge(vectors: Iterable[RangeVectorCursor], outputRange: Option[RvRange]): RangeVectorCursor = {
    new RVCursorImpl(vectors, outputRange)
  }
}


/**
  * Use when data for same time series spans multiple shards, or clusters.
  */
final case class StitchRvsExec(queryContext: QueryContext,
                               dispatcher: PlanDispatcher,
                               outputRvRange: Option[RvRange],
                               children: Seq[ExecPlan]) extends NonLeafExecPlan {
  require(children.nonEmpty)

  outputRvRange match {
    case Some(RvRange(startMs, stepMs, endMs)) =>
                            require(startMs <= endMs && stepMs > 0, "RvRange start <= end and step > 0")
    case None                                  =>
  }
  protected def args: String = ""

  protected def composeStreaming(childResponses: Observable[(Observable[RangeVector], Int)],
                                 schemas: Observable[(ResultSchema, Int)],
                                 querySession: QuerySession): Observable[RangeVector] = ???

  protected[exec] def compose(childResponses: Observable[(QueryResult, Int)],
                        firstSchema: Task[ResultSchema],
                        querySession: QuerySession): Observable[RangeVector] = {
    qLogger.debug(s"StitchRvsExec: Stitching results:")
    val stitched = childResponses.map(_._1.result).toListL.map(_.flatten).map { srvs =>
      val groups = srvs.groupBy(_.key.labelValues)
      groups.mapValues { toMerge =>
        val rows = StitchRvsExec.merge(toMerge.map(_.rows()), outputRvRange)
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

package filodb.query.exec

import scala.collection.mutable

import monix.reactive.Observable

import filodb.core.query._
import filodb.memory.format.RowReader
import filodb.query._
import filodb.query.Query.qLogger

object StitchRvsExec {

  def merge(vectors: Seq[Iterator[RowReader]]): Iterator[RowReader] = {
    // This is an n-way merge without using a heap.
    // Heap is not used since n is expected to be very small (almost always just 1 or 2)
    new Iterator[RowReader] {
      val bVectors = vectors.map(_.buffered)
      val mins = new mutable.ArrayBuffer[BufferedIterator[RowReader]](2)
      val noResult = new TransientRow(0, 0)
      override def hasNext: Boolean = bVectors.exists(_.hasNext)

      override def next(): RowReader = {
        mins.clear()
        var minTime = Long.MaxValue
        bVectors.foreach { r =>
          if (r.hasNext) {
            val t = r.head.getLong(0)
            if (mins.size == 0) {
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
        if (mins.size == 1) mins.head.next()
        else if (mins.isEmpty) throw new IllegalStateException("next was called when no element")
        else {
          mins.foreach(it => if (it.hasNext) it.next()) // move iterator forward
          noResult.timestamp = minTime
          noResult.value = Double.NaN // until we have a different indicator for "unable-to-calculate" use NaN
          noResult
        }
      }
    }
  }
}

/**
  * Use when data for same time series spans multiple shards, or clusters.
  */
final case class StitchRvsExec(id: String,
                               dispatcher: PlanDispatcher,
                               children: Seq[ExecPlan]) extends NonLeafExecPlan {
  require(children.nonEmpty)

  protected def args: String = ""

  protected def schemaOfCompose(): ResultSchema = children.head.schema()

  protected def compose(childResponses: Observable[(QueryResponse, Int)],
                        queryConfig: QueryConfig): Observable[RangeVector] = {
    qLogger.debug(s"StitchRvsExec: Stitching results:")
    val stitched = childResponses.map {
      case (QueryResult(_, _, result), _) => result
      case (QueryError(_, ex), _)         => throw ex
    }.toListL.map(_.flatten).map { srvs =>
      val groups = srvs.groupBy(_.key.labelValues)
      groups.mapValues { toMerge =>
        val rows = StitchRvsExec.merge(toMerge.map(_.rows))
        val key = toMerge.head.key
        IteratorBackedRangeVector(key, rows)
      }.values
    }.map(Observable.fromIterable)
    Observable.fromTask(stitched).flatten
  }
}

/**
  * Range Vector Transformer version of StitchRvsExec
  */
final case class StitchRvsMapper() extends RangeVectorTransformer {

  def apply(source: Observable[RangeVector],
            queryConfig: QueryConfig,
            limit: Int,
            sourceSchema: ResultSchema): Observable[RangeVector] = {
    qLogger.debug(s"StitchRvsMapper: Stitching results:")
    val stitched = source.toListL.map { rvs =>
      val groups = rvs.groupBy(_.key)
      groups.mapValues { toMerge =>
        val rows = StitchRvsExec.merge(toMerge.map(_.rows))
        val key = toMerge.head.key
        IteratorBackedRangeVector(key, rows)
      }.values
    }.map(Observable.fromIterable)
    Observable.fromTask(stitched).flatten
  }

  override protected[query] def args: String = ""
}

package filodb.query.exec.rangefn

import monix.reactive.Observable

import filodb.core.query.{RangeVector, RangeVectorKey}
import filodb.memory.format.RowReader
import filodb.query.exec.BufferableIterator

case class SortFunction(sortAscending: Boolean = true) {

  def execute(source: Observable[RangeVector]): Observable[RangeVector] = {

    val ordering: Ordering[Double] = if (sortAscending)
      (Ordering[Double])
    else
      (Ordering[Double]).reverse

    val resultRv = source.toListL.map { rvs =>
      rvs.map { rv =>
        new RangeVector {
          override def key: RangeVectorKey = rv.key

          override def rows: Iterator[RowReader] = new BufferableIterator(rv.rows).buffered
        }
      }.sortBy { rv => rv.rows.asInstanceOf[BufferedIterator[RowReader]].head.getDouble(1)
      }(ordering)

    }.map(Observable.fromIterable)

    Observable.fromTask(resultRv).flatten
  }

}

package filodb.core.memstore

import scala.concurrent.duration.FiniteDuration

import monix.reactive.Observable

/**
 * Utilities for generating streams of FlushCommands.
 */
object FlushStream {
  val empty = Observable.empty[FlushCommand]

  /**
   * Produces a stream of FlushCommands at the interval defined by period.  Note that if there is backpressure
   * then there might be bunching of FlushCommands.
   * The group number round robins from 0 to (numGroups - 1) starting at startingGroupNo
   * @param numGroups the number of groups to round robin
   * @param period the period of flush commands.  Every period the next group will be flushed.
   * @param startingGroupNo the starting group number
   */
  def interval(numGroups: Int, period: FiniteDuration, startingGroupNo: Int = 0): Observable[FlushCommand] =
    Observable.interval(period).map { n => FlushCommand((n + startingGroupNo).toInt % numGroups) }

  /**
   * Produces a stream of FlushCommands approximately every nRecords input records.
   * Intended for testing only.  Might not be very efficient if applied to a real stream....
   *
   * The group number round robins from 0 to (numGroups - 1).
   * @param numGroups the number of groups to round robin
   * @param nRecords  the approximate "period" of the flushes in terms of number of source records
   */
  def everyN(numGroups: Int, nRecords: Int, source: Observable[SomeData]): Observable[FlushCommand] =
    // State: (runningRecordCount, groupNum, flush)
    source.scan((0, -1, false)) { case ((count, group, _), SomeData(records, _)) =>
            val newCount = count + records.countRecords()
            if (newCount >= nRecords) { (newCount - nRecords, (group + 1) % numGroups, true) }
            else                      { (newCount, group, false) }
          }.collect { case (_, group, true) => FlushCommand(group) }

  /**
   * For testing only.  Just a simple stream of FlushCommands, one for each gorup.
   */
  def allGroups(numGroups: Int): Observable[FlushCommand] =
    Observable.fromIterable((0 until numGroups).map { n => FlushCommand(n) })
}
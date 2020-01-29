package filodb.coordinator.queryengine2

import com.typesafe.scalalogging.StrictLogging

/**
  * HA Strategy for routing queries between replica clusters based on failure time ranges.
  */
trait FailureRoutingStrategy extends StrictLogging {
  def plan(failure: Seq[FailureTimeRange], time: TimeRange, lookbackTime: Long, step: Long): Seq[Route]
}

object QueryFailureRoutingStrategy extends FailureRoutingStrategy {

  /**
    * Remove smaller FailureTimeRange when more than one FailureTimeRanges have overlapping times
    */
  private def removeSmallerOverlappingFailures(failures: Seq[FailureTimeRange]): Seq[FailureTimeRange] = {

    failures.sortWith(_.timeRange.startInMillis < _.timeRange.startInMillis).
      foldLeft(Seq[FailureTimeRange]()) { (buildList, tail) =>
        buildList match {
          case Nil => Seq(tail)
          case head :+ value =>
            if (value.timeRange.endInMillis >= tail.timeRange.startInMillis) {
              // Remove larger overlapping interval
              if ((value.timeRange.endInMillis - value.timeRange.startInMillis) <
                (tail.timeRange.endInMillis - tail.timeRange.startInMillis)) {
                buildList.dropRight(1) :+ tail
              }
              else {
                buildList
              }

            } else {
              buildList :+ tail
            }
        }
      }
  }

  /**
    *
    * @param failures seq of FailureTimeRanges
    * @param time query start and end time
    * @param lookbackTime query lookbackTime
    * @param step query step
    * @return seq of routes
    */
  def plan(failures: Seq[FailureTimeRange], time: TimeRange, lookbackTime: Long, step: Long): Seq[Route] = {

    val nonOverlappingFailures = removeSmallerOverlappingFailures(failures)
    if ((nonOverlappingFailures.last.timeRange.endInMillis < time.startInMillis) ||
      (nonOverlappingFailures.head.timeRange.startInMillis > time.endInMillis)) {
       Seq(LocalRoute(None)) // No failure in this time range
    }
    logger.info("Logical plan time:" + time)

    // Recursively split query into local and remote routes starting from first FailureTimeRange
    splitQueryTime(nonOverlappingFailures, 0, time.startInMillis - lookbackTime, time.endInMillis, lookbackTime,
     step)
  }

  /**
    * Recursively generate Local and Remote Routes by splitting query time based on failures
    *
    * @param failure seq of FailureTimeRanges
    * @param index   index of FailureTimeRange which has to be processed
    * @param start   start time for route
    * @param end     end time for route
    * @return seq of Routes
    */
  private def splitQueryTime(failure: Seq[FailureTimeRange], index: Int, start: Long, end: Long,
                             lookbackTime: Long, step: Long): Seq[Route] = {

    if (index >= failure.length)
      return Nil

    val startWithLookBack = if (index == 0)
        start + lookbackTime
    else
        start

    if (startWithLookBack > end)
       return Nil
    // traverse query range time from left to right , break at failure start
    var i = index + 1

    if (!failure(index).isRemote) {
      // Handle local failure
      // Traverse till we get a remote failure to minimize number of queries
      while ((i < failure.length) && (!failure(i).isRemote))
        i = i + 1
      // need further splitting
      if (i < failure.length) {
        val lastSampleTime = getLastSampleTimeWithStep(startWithLookBack,
          failure(i).timeRange.startInMillis, step)
        // Query from current start time till next remote failure starts should be executed remotely
        // Routes should have Periodic series time so add lookbackTime
        RemoteRoute(Some(TimeRange(startWithLookBack, lastSampleTime))) +:
          splitQueryTime(failure, i, lastSampleTime + step, end,
            lookbackTime, step) // Process remaining query
      } else {
        // Last failure so no further splitting required
        Seq(RemoteRoute(Some(TimeRange(startWithLookBack , end))))
      }

    } else {
      // Iterate till we get a local failure
      while ((i < failure.length) && (failure(i).isRemote))
        i = i + 1
      if (i < failure.length) {
        val lastSampleTime = getLastSampleTimeWithStep(startWithLookBack,
          failure(i).timeRange.startInMillis, step )
        // Query from current start time till next local failure starts should be executed locally
        LocalRoute(Some(TimeRange(startWithLookBack, lastSampleTime))) +:
          splitQueryTime(failure, i, lastSampleTime + step, end, lookbackTime, step)
      }
      else {
        Seq(LocalRoute(Some(TimeRange(startWithLookBack, end))))
      }
    }
  }

  private def getLastSampleTimeWithStep( start: Long, end: Long, step: Long): Long = {
    var ctr = 0
    var currentTime = start
    var nextTime = start
    while (nextTime < end) {
      ctr = ctr + 1
      currentTime = nextTime
      nextTime = start + (step * ctr)
    }
    currentTime
  }

}

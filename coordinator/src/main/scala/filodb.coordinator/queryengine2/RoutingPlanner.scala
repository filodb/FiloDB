package filodb.coordinator.queryengine2

import com.typesafe.scalalogging.StrictLogging

import filodb.query._

/**
  * Planner for routing based on failure ranges for a given LogicalPlan.
  */
trait RoutingPlanner extends StrictLogging {
  def plan(lp: LogicalPlan, failure: Seq[FailureTimeRange], time: TimeRange): Seq[Route]
}

object QueryRoutingPlanner extends RoutingPlanner {

  /**
    * Remove larger FailureTimeRange when more than one FailureTimeRanges have overlapping times
    */
  def removeLargerOverlappingFailures(failures: Seq[FailureTimeRange]): Seq[FailureTimeRange] = {

    failures.sortWith(_.timeRange.startInMillis < _.timeRange.startInMillis).
      foldLeft(Seq[FailureTimeRange]()) { (buildList, tail) =>
        buildList match {
          case Nil => Seq(tail)
          case head :+ value =>
            if (value.timeRange.endInMillis >= tail.timeRange.startInMillis) {
              // Remove larger overlapping interval
              if ((value.timeRange.endInMillis - value.timeRange.startInMillis) >
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

  def plan(lp: LogicalPlan, failures: Seq[FailureTimeRange], time: TimeRange): Seq[Route] = {

    val nonOverlappingFailures = removeLargerOverlappingFailures(failures)
    if (nonOverlappingFailures.last.timeRange.startInMillis < time.startInMillis)
      return Seq(LocalRoute(None)) // No failure in this time range
    logger.info("Logical plan time:" + time)

    // Recursively split query into local and remote routes starting from first FailureTimeRange
    splitQueryTime(nonOverlappingFailures, 0, time.startInMillis, time.endInMillis)

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
  def splitQueryTime(failure: Seq[FailureTimeRange], index: Int, start: Long, end: Long): Seq[Route] = {

    if (index >= failure.length)
      return Nil
    // traverse query range time from left to right , break at failure start
    var i = index + 1

    // Dispatcher is present only when failure is local
    failure(index).dispatcher.map { x =>
      // Handle local failure
      // Traverse till we get a remote failure to minimize number of queries
      while ((i < failure.length) && (failure(i).dispatcher.isDefined))
        i = i + 1

      if (i < failure.length) // need further splitting
      // Query from current start time till next remote failure starts should be executed remotely
        RemoteRoute(Some(TimeRange(start, failure(i).timeRange.startInMillis - 1)), x) +:
          splitQueryTime(failure, i, failure(i).timeRange.startInMillis, end) // Process remaining query
      else
      // Last failure so no further splitting required
        Seq(RemoteRoute(Some(TimeRange(start, end)), x))

    }.getOrElse {
      // Iterate till we get a local failure
      while ((i < failure.length) && (!failure(i).dispatcher.isDefined))
        i = i + 1
      if (i < failure.length)
      // Query from current start time till next local failure starts should be executed locally
        LocalRoute(Some(TimeRange(start, failure(i).timeRange.startInMillis - 1))) +:
          splitQueryTime(failure, i, failure(i).timeRange.startInMillis, end)
      else
        Seq(LocalRoute(Some(TimeRange(start, end))))
    }
  }

  /**
    * Check whether logical plan has a PeriodicSeriesPlan
    */
  def isPeriodicSeriesPlan(logicalPlan: LogicalPlan): Boolean = {
    if (logicalPlan.isInstanceOf[RawSeriesPlan] || logicalPlan.isInstanceOf[MetadataQueryPlan])
      return false;
    true
  }

  /**
    * Check whether all child logical plans have same start and end time
    */
  def hasSingleTimeRange(logicalPlan: LogicalPlan): Boolean = {
    if (logicalPlan.isInstanceOf[BinaryJoin]) {
      val binaryJoin = logicalPlan.asInstanceOf[BinaryJoin]
      val lhsTime = getPeriodicSeriesTimeFromLogicalPlan(binaryJoin.lhs)
      val rhsTime = getPeriodicSeriesTimeFromLogicalPlan(binaryJoin.rhs)
      return (lhsTime.startInMillis == rhsTime.startInMillis) && (lhsTime.endInMillis == rhsTime.endInMillis)
    }
    return true
  }

  /**
    * Retrieve start and end time from LogicalPlan
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def getPeriodicSeriesTimeFromLogicalPlan(logicalPlan: LogicalPlan): TimeRange = {
    logicalPlan match {
      case lp: PeriodicSeries => TimeRange(lp.start, lp.end)
      case lp: PeriodicSeriesWithWindowing => TimeRange(lp.start, lp.end)
      case lp: ApplyInstantFunction => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case lp: Aggregate => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case lp: BinaryJoin => getPeriodicSeriesTimeFromLogicalPlan(lp.lhs) // can assume lhs & rhs have same time
      case lp: ScalarVectorBinaryOperation => getPeriodicSeriesTimeFromLogicalPlan(lp.vector)
      case lp: ApplyMiscellaneousFunction => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case _ => throw new BadQueryException("Invalid logical plan")
    }
  }

  /**
    * Used to change start and end time(TimeRange) of LogicalPlan
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def copyWithUpdatedTimeRange(logicalPlan: LogicalPlan, timeRange: TimeRange,
                               lookBackTime: Long): PeriodicSeriesPlan = {
    logicalPlan match {
      case lp: PeriodicSeries => lp.copy(start = timeRange.startInMillis + lookBackTime,
        end = timeRange.endInMillis, rawSeries = copyRawSeriesWithUpdatedTimeRange(lp.rawSeries, timeRange))
      case lp: PeriodicSeriesWithWindowing => lp.copy(start = timeRange.startInMillis + lookBackTime, end =
        timeRange.endInMillis, rawSeries = copyRawSeriesWithUpdatedTimeRange(lp.rawSeries, timeRange))
      case lp: ApplyInstantFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))
      case lp: Aggregate => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))
      case lp: BinaryJoin => lp.copy(lhs = copyWithUpdatedTimeRange(lp.lhs, timeRange, lookBackTime), rhs =
        copyWithUpdatedTimeRange(lp.rhs, timeRange, lookBackTime))
      case lp: ScalarVectorBinaryOperation => lp.copy(vector = copyWithUpdatedTimeRange(lp.vector, timeRange,
        lookBackTime))
      case lp: ApplyMiscellaneousFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange,
        lookBackTime))
      case _ => throw new UnsupportedOperationException("Logical plan not supportred for copy")
    }
  }

  /**
    * Used to change rangeSelector of RawSeriesPlan
    */
  def copyRawSeriesWithUpdatedTimeRange(rawSeriesPlan: RawSeriesPlan, timeRange: TimeRange): RawSeries = {

    rawSeriesPlan match {
      case rs: RawSeries => rs.rangeSelector match {
        case is: IntervalSelector => rs.copy(rangeSelector = is.copy(timeRange.startInMillis, timeRange.endInMillis))
        case _ => throw new UnsupportedOperationException("Copy supported only for IntervalSelector")
      }
      case _ => throw new UnsupportedOperationException("Copy supported only for RawSeries")
    }
  }

  /**
    * Retrieve start time of Raw Series
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def getRawSeriesStartTime(logicalPlan: LogicalPlan): Option[Long] = {
    logicalPlan match {
      case lp: RawSeries => lp.rangeSelector match {
        case rs: IntervalSelector => Some(rs.from)
        case _ => None
      }
      case lp: PeriodicSeries => getRawSeriesStartTime(lp.rawSeries)
      case lp: PeriodicSeriesWithWindowing => getRawSeriesStartTime(lp.rawSeries)
      case lp: ApplyInstantFunction => getRawSeriesStartTime(lp.vectors)
      case lp: Aggregate => getRawSeriesStartTime(lp.vectors)
      case lp: BinaryJoin => getRawSeriesStartTime(lp.lhs) // can assume lhs & rhs have same time
      case lp: ScalarVectorBinaryOperation => getRawSeriesStartTime(lp.vector)
      case lp: ApplyMiscellaneousFunction => getRawSeriesStartTime(lp.vectors)
      case _ => None
    }
  }
}

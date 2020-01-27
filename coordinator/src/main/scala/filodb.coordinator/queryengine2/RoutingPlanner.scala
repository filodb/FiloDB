package filodb.coordinator.queryengine2

import com.typesafe.scalalogging.StrictLogging

import filodb.query._

/**
  * Planner for routing based on failure ranges for a given LogicalPlan.
  */
trait RoutingPlanner extends StrictLogging {
  def plan(failure: Seq[FailureTimeRange], time: TimeRange, lookbackTime: Long, step: Long): Seq[Route]
}

object QueryRoutingPlanner extends RoutingPlanner {

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

  /**
    * Check whether logical plan has a PeriodicSeriesPlan
    */
  def isPeriodicSeriesPlan(logicalPlan: LogicalPlan): Boolean = {
    if (!logicalPlan.isRoutable) {
      false
    } else {
      logicalPlan match {
        case s: ScalarVectorBinaryOperation => isPeriodicSeriesPlan(s.vector)
        case v: VectorPlan                  => isPeriodicSeriesPlan(v.scalars)
        case i: ApplyInstantFunction        => isPeriodicSeriesPlan(i.vectors)
        case b: BinaryJoin                  => isPeriodicSeriesPlan(b.lhs)
        case _                              => true
      }
    }
  }

  /**
    * Check whether all child logical plans have same start and end time
    */
  def hasSingleTimeRange(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case binaryJoin: BinaryJoin =>
        val lhsTime = getPeriodicSeriesTimeFromLogicalPlan(binaryJoin.lhs)
        val rhsTime = getPeriodicSeriesTimeFromLogicalPlan(binaryJoin.rhs)
        (lhsTime.startInMillis == rhsTime.startInMillis) && (lhsTime.endInMillis == rhsTime.endInMillis)
      case _ => true
    }
  }

  /**
    * Retrieve start and end time from LogicalPlan
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def getPeriodicSeriesTimeFromLogicalPlan(logicalPlan: LogicalPlan): TimeRange = {
    logicalPlan match {
      case lp: PeriodicSeries              => TimeRange(lp.start, lp.end)
      case lp: PeriodicSeriesWithWindowing => TimeRange(lp.start, lp.end)
      case lp: ApplyInstantFunction        => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case lp: Aggregate                   => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case lp: BinaryJoin                  => // can assume lhs & rhs have same time
                                              getPeriodicSeriesTimeFromLogicalPlan(lp.lhs)
      case lp: ScalarVectorBinaryOperation => getPeriodicSeriesTimeFromLogicalPlan(lp.vector)
      case lp: ApplyMiscellaneousFunction  => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case lp: ApplySortFunction           => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case lp: ScalarVaryingDoublePlan     => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case lp: ScalarTimeBasedPlan         => TimeRange(lp.rangeParams.start, lp.rangeParams.end)
      case lp: VectorPlan                  => getPeriodicSeriesTimeFromLogicalPlan(lp.scalars)
      case lp: ApplyAbsentFunction         => getPeriodicSeriesTimeFromLogicalPlan(lp.vectors)
      case _                               => throw new BadQueryException(s"Invalid logical plan")
    }
  }

  /**
    * Used to change start and end time(TimeRange) of LogicalPlan
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def copyWithUpdatedTimeRange(logicalPlan: LogicalPlan, timeRange: TimeRange,
                               lookBackTime: Long): PeriodicSeriesPlan = {
    logicalPlan match {
      case lp: PeriodicSeries => lp.copy(start = timeRange.startInMillis, end = timeRange.endInMillis,
        rawSeries = copyNonPeriodicWithUpdatedTimeRange(lp.rawSeries, timeRange, lookBackTime).asInstanceOf[RawSeries])
      case lp: PeriodicSeriesWithWindowing => lp.copy(start = timeRange.startInMillis, end =
        timeRange.endInMillis, series = copyNonPeriodicWithUpdatedTimeRange(lp.series, timeRange, lookBackTime))
      case lp: ApplyInstantFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))
      case lp: Aggregate => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))
      case lp: BinaryJoin => lp.copy(lhs = copyWithUpdatedTimeRange(lp.lhs, timeRange, lookBackTime), rhs =
        copyWithUpdatedTimeRange(lp.rhs, timeRange, lookBackTime))
      case lp: ScalarVectorBinaryOperation => lp.copy(vector = copyWithUpdatedTimeRange(lp.vector, timeRange,
        lookBackTime))
      case lp: ApplyMiscellaneousFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange,
        lookBackTime))
      case lp: ApplySortFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))
      case _ => throw new UnsupportedOperationException("Logical plan not supported for copy")
    }
  }

  /**
    * Used to change rangeSelector of RawSeriesLikePlan
    */
  def copyNonPeriodicWithUpdatedTimeRange(plan: RawSeriesLikePlan,
                                          timeRange: TimeRange,
                                          lookBackTime: Long): RawSeriesLikePlan = {
    plan match {
      case rs: RawSeries => rs.rangeSelector match {
        case is: IntervalSelector => rs.copy(rangeSelector = is.copy(timeRange.startInMillis - lookBackTime,
          timeRange.endInMillis))
        case _ => throw new UnsupportedOperationException("Copy supported only for IntervalSelector")
      }
      case p: ApplyInstantFunctionRaw =>
        p.copy(vectors = copyNonPeriodicWithUpdatedTimeRange(p.vectors, timeRange, lookBackTime)
                           .asInstanceOf[RawSeries])
      case _ => throw new UnsupportedOperationException("Copy supported only for RawSeries")
    }
  }

  /**
    * Retrieve start time of Raw Series
    * NOTE: Plan should be PeriodicSeriesPlan
    */
  def getRawSeriesStartTime(logicalPlan: LogicalPlan): Option[Long] = {
     LogicalPlan.findLeafLogicalPlans(logicalPlan).head match {
      case lp: RawSeries => lp.rangeSelector match {
        case rs: IntervalSelector => Some(rs.from)
        case _ => None
      }
      case _                      => throw new BadQueryException(s"Invalid logical plan $logicalPlan")
    }
  }
}

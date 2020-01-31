package filodb.coordinator.queryplanner

import filodb.query._

object LogicalPlanUtils {
  
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
  def copyWithUpdatedTimeRange(logicalPlan: LogicalPlan,
                               timeRange: TimeRange,
                               lookBackTime: Long): PeriodicSeriesPlan = {
    logicalPlan match {
      case lp: PeriodicSeries => lp.copy(start = timeRange.startInMillis,
                                         end = timeRange.endInMillis,
                                         rawSeries = copyNonPeriodicWithUpdatedTimeRange(lp.rawSeries, timeRange,
                                                                         lookBackTime).asInstanceOf[RawSeries])
      case lp: PeriodicSeriesWithWindowing => lp.copy(start = timeRange.startInMillis,
                                                      end = timeRange.endInMillis,
                                                      series = copyNonPeriodicWithUpdatedTimeRange(lp.series, timeRange,
                                                                                                   lookBackTime))
      case lp: ApplyInstantFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))

      case lp: Aggregate  => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))

      case lp: BinaryJoin => lp.copy(lhs = copyWithUpdatedTimeRange(lp.lhs, timeRange, lookBackTime),
                                     rhs = copyWithUpdatedTimeRange(lp.rhs, timeRange, lookBackTime))
      case lp: ScalarVectorBinaryOperation =>
                      lp.copy(vector = copyWithUpdatedTimeRange(lp.vector, timeRange, lookBackTime))

      case lp: ApplyMiscellaneousFunction =>
                      lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))

      case lp: ApplySortFunction => lp.copy(vectors = copyWithUpdatedTimeRange(lp.vectors, timeRange, lookBackTime))

      case _ => throw new UnsupportedOperationException("Logical plan not supported for copy")
    }
  }

  /**
    * Used to change rangeSelector of RawSeriesLikePlan
    */
  private def copyNonPeriodicWithUpdatedTimeRange(plan: RawSeriesLikePlan,
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

package filodb.coordinator.queryengine

import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import filodb.query.{LogicalPlan, _}
import filodb.query.exec.PlanDispatcher

/**
  * A provider to get failure ranges. Query engine can use failure ranges while preparing physical
  * plan to reroute or skip failure ranges. Ranges are based on dataset and over all clusters.
  * Provider will filter failure ranges by current cluster and its replicas. Failures which do not
  * belong to current cluster or its replica, will be skipped.
  */
trait FailureProvider {
  def getFailures(datasetRef: DatasetRef, queryTimeRange: TimeRange): Seq[FailureTimeRange]
}

/**
  * Time range.
  *
  * @param startInMillis epoch time in millis.
  * @param endInMillis   epoch time in millis.
  */
case class TimeRange(startInMillis: Long, endInMillis: Long)

/**
  * Failure details.
  *
  * @param clusterName cluster name.
  * @param datasetRef  Dataset reference for database and dataset.
  * @param timeRange   time range.
  * @param dispatcher  dispatcher implementation for given cluster. It is present if failure is in local.
  */
case class FailureTimeRange(clusterName: String, datasetRef: DatasetRef, timeRange: TimeRange,
                            dispatcher: Option[PlanDispatcher])

/**
  * For rerouting queries for failure ranges, Route trait will offer more context in the form of corrective
  * ranges for queries or alternative dispatchers.
  * A local route indicates a non-failure range on local cluster. A remote route indicates a non-failure
  * range on remote cluster.
  */
trait Route

case class LocalRoute(tr: Option[TimeRange]) extends Route

case class RemoteRoute(tr: Option[TimeRange], dispatcher: PlanDispatcher) extends Route

/**
  * Planner for routing based on failure ranges for a given LogicalPlan.
  */
trait RoutingPlanner extends StrictLogging {
  def plan(lp: LogicalPlan, failure: Seq[FailureTimeRange]): Seq[Route]
}

object QueryRoutingPlanner extends RoutingPlanner {

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

  def plan(lp: LogicalPlan, failures: Seq[FailureTimeRange]): Seq[Route] = {

    if (!isPeriodicSeriesPlan(lp) || !hasSingleTimeRange(lp))
      new LocalRoute(None)

    val nonOverlappingFailures = removeLargerOverlappingFailures(failures)
    val time = getTimeFromLogicalPlan(lp)
    if(nonOverlappingFailures.last.timeRange.startInMillis < time.startInMillis )
      return Seq(LocalRoute(None))  // No failure in this time range
    logger.info("Logical plan time:" + time)

    splitQueryTime(nonOverlappingFailures, 0, time.startInMillis, time.endInMillis)

  }

  def splitQueryTime(failure: Seq[FailureTimeRange], index: Int, start: Long, end: Long): Seq[Route] = {

    if (index >= failure.length)
      return Nil
    // traverse query range time from left to right , break at failure start
    var i = index + 1

    failure(index).dispatcher.map { x =>
      // Handle local failure
      // Traverse till we get a remote failure
      while ((i < failure.length) && (failure(i).dispatcher.isDefined))
        i = i + 1

      if (i < failure.length)
        RemoteRoute(Some(TimeRange(start, failure(i).timeRange.startInMillis)),
          x) +: splitQueryTime(failure, i, failure(i).timeRange.startInMillis, end)
      else
      // Last failure so no further splitting required
        Seq(RemoteRoute(Some(TimeRange(start, end)), x))

    }.getOrElse {
      // Iterate till we get a local failure
      while ((i < failure.length) && (!failure(i).dispatcher.isDefined))
        i = i + 1
      if (i < failure.length) //need further splitting
        LocalRoute(Some(TimeRange(start, failure(i).timeRange.startInMillis))) +:
          splitQueryTime(failure, i, failure(i).timeRange.startInMillis, end)
      else
        Seq(LocalRoute(Some(TimeRange(start, end))))
    }
  }

  def isPeriodicSeriesPlan(logicalPlan: LogicalPlan): Boolean = {
    if (logicalPlan.isInstanceOf[RawSeriesPlan] || logicalPlan.isInstanceOf[MetadataQueryPlan])
      return false;
    true
  }

  def hasSingleTimeRange(logicalPlan: LogicalPlan): Boolean = {
    if (logicalPlan.isInstanceOf[BinaryJoin]) {
      val binaryJoin = logicalPlan.asInstanceOf[BinaryJoin]
      val lhsTime = getTimeFromLogicalPlan(binaryJoin.lhs)
      val rhsTime = getTimeFromLogicalPlan(binaryJoin.rhs)
      return (lhsTime.startInMillis == rhsTime.startInMillis) && (lhsTime.endInMillis == rhsTime.endInMillis)
    }
    return true
  }

  def getTimeFromLogicalPlan(logicalPlan: LogicalPlan): TimeRange = {
    logicalPlan match {
      case lp: PeriodicSeries => val periodicSeries = lp.asInstanceOf[PeriodicSeries]
        TimeRange(periodicSeries.start, periodicSeries.end)
      case lp: PeriodicSeriesWithWindowing => val periodicSeriesWithWindowing = lp.
        asInstanceOf[PeriodicSeriesWithWindowing]
        TimeRange(periodicSeriesWithWindowing.start,
          periodicSeriesWithWindowing.end)
      case lp: ApplyInstantFunction => getTimeFromLogicalPlan(lp.asInstanceOf[ApplyInstantFunction].vectors)
      case lp: Aggregate => getTimeFromLogicalPlan(lp.asInstanceOf[Aggregate].vectors)
      case lp: BinaryJoin => // can assume lhs & rhs have same timr
        getTimeFromLogicalPlan(lp.asInstanceOf[BinaryJoin].lhs)

      case lp: ScalarVectorBinaryOperation => getTimeFromLogicalPlan(lp.asInstanceOf[ScalarVectorBinaryOperation]
        .vector)
      case lp: ApplyMiscellaneousFunction => getTimeFromLogicalPlan(lp.asInstanceOf[ApplyMiscellaneousFunction].
        vectors)
      case _ => throw new BadQueryException("Invalid logical plan")
    }
  }

  def updateTimeLogicalPlan(logicalPlan: LogicalPlan, timeRange: TimeRange): PeriodicSeriesPlan = {

    logicalPlan match {
      case lp: PeriodicSeries => val periodicSeries = lp.asInstanceOf[PeriodicSeries]
        periodicSeries.copy(start = timeRange.startInMillis,
          end = timeRange.endInMillis)
      case lp: PeriodicSeriesWithWindowing => lp.asInstanceOf[PeriodicSeriesWithWindowing].
        copy(start = timeRange.startInMillis, end = timeRange.endInMillis)
      case lp: ApplyInstantFunction => val applyInstantFunction = lp.asInstanceOf[ApplyInstantFunction]
        applyInstantFunction.copy(vectors = updateTimeLogicalPlan
        (applyInstantFunction.vectors, timeRange))
      case lp: Aggregate => val aggregate = lp.asInstanceOf[Aggregate]
        aggregate.copy(vectors = updateTimeLogicalPlan
        (aggregate.vectors, timeRange))
      case lp: BinaryJoin => val binaryJoin = lp.asInstanceOf[BinaryJoin]
        binaryJoin.copy(lhs = updateTimeLogicalPlan(binaryJoin.lhs,
          timeRange), rhs =
          updateTimeLogicalPlan(binaryJoin.rhs, timeRange))
      case lp: ScalarVectorBinaryOperation => val scalarVectorBinaryOperation = lp.
        asInstanceOf[ScalarVectorBinaryOperation]
        scalarVectorBinaryOperation.copy(vector = updateTimeLogicalPlan(scalarVectorBinaryOperation.vector, timeRange))
      case lp: ApplyMiscellaneousFunction => val applyMiscellaneousFunction = lp.
        asInstanceOf[ApplyMiscellaneousFunction]
        applyMiscellaneousFunction.copy(vectors = updateTimeLogicalPlan(applyMiscellaneousFunction.vectors, timeRange))
      case _ => throw new BadQueryException("Invalid logical plan")
    }
  }
}
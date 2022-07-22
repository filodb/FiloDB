package filodb.coordinator.queryplanner

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query._
import filodb.query.LogicalPlan._
import filodb.query.exec._

case class PartitionAssignment(partitionName: String, endPoint: String, timeRange: TimeRange)

trait PartitionLocationProvider {

  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                            timeRange: TimeRange): List[PartitionAssignment]
}

/**
 * The MultiPartitionPlanner  is responsible for planning queries that span one or more deployment partitions.
 * Data for each shard-key (ws/ns) combination can be ingested into a different partition. The partitionLocationProvider
 * param provides the locality mapping of fully specified ws/ns to a single partition. Note that this planner DOES NOT
 * handle regex in ws/ns labels in the queries as that is handled by ShardKeyRegexPlanner. Planners  are
 * Hierarchical and ShardKeyRegexPlanner wraps MultiPartitionPlanner, thus all regex on namespace (_ns_) in the
 * queries are already replaced with equals when the materialize of this class is invoked.
 *
 * @param partitionLocationProvider The implementation is responsible to get the partition assignments based on the
 *                                  shard keys and an additional time dimension to handle assignments of partitions
 * @param localPartitionPlanner     The planner instance to use of the data is available locally
 * @param localPartitionName        Unique name for the local partition
 * @param dataset                   The dataset instance, see Dataset documentation for more details
 * @param queryConfig               Configuration for the query planner
 * @param remoteExecHttpClient      If the partition is not local, a remote call is made to the correct partition to
 *                                  query and retrieve the data.
 */
class MultiPartitionPlanner(partitionLocationProvider: PartitionLocationProvider,
                            localPartitionPlanner: QueryPlanner,
                            localPartitionName: String,
                            val dataset: Dataset,
                            val queryConfig: QueryConfig,
                            remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient)
  extends QueryPlanner with StrictLogging with DefaultPlanner {

  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options

  val remoteHttpTimeoutMs: Long = queryConfig.remoteHttpTimeoutMs.getOrElse(60000)

  val datasetMetricColumn: String = dataset.options.metricColumn

  // scalastyle:off method.length
  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
      // Pseudo code for the materialize
      //
      // def materialize(lp) {
      //   walk(lp)
      // }
      //
      // def walk(lp) {
      //   if lp.isLocalPlan() {
      //     localPlanner.materialize(lp)
      //   } else if partitions(lp).size == 1 && partitions(lp.partitions).head.name != "localPartition" {
      //     materializeRemoteExecPlan(lp)
      //   } else {
      //     case BinaryJoin:
      //       lhs, rhs = walk(lp.lhs), walk(lp.rhs)
      //       BinaryJoinExec(lhs, rhs, inProcess)
      //     case x: _:
      //       // X represents appropriate handler based on type of x
      //       plannerHelper.materializeX(x)
      //    }
      // }
    val tsdbQueryParams = qContext.origQueryParams

    if(
      !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams] && !qContext.plannerParams.processMultiPartition)
    ) { // Query was part of routing
      localPartitionPlanner.materialize(logicalPlan, qContext)
    } else logicalPlan match {
      case mqp: MetadataQueryPlan  => materializeMetadataQueryPlan(mqp, qContext).plans.head
      case lp: TsCardinalities     => materializeTsCardinalities(lp, qContext).plans.head
      case lp                       => {
        val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
        // Get all the valid time-ranges to query.
        //   For example, cross-partition lookback is unsupported. These time-ranges would
        //   indicate queryable ranges where all involved lookbacks (range-selector windows,
        //   stale-data lookbacks, etc) do not cross partitions.
        val timeRanges = {
          val invalidRanges = getInvalidRanges(lp, queryParams)
          val mergedInvalidRanges = mergeAndSortRanges(invalidRanges)
          val totalTimeRange = TimeRange(1000 * queryParams.startSecs, 1000 * queryParams.endSecs)
          invertRanges(mergedInvalidRanges, totalTimeRange)
        }
        // Materialize an ExecPlan for each of the above time-ranges.
        val plans = timeRanges.map{ range =>
          // TODO(a_theimer): hack to support TopLevelSubqueries in shardKeyRegexPlanner.
          //   Need to update the logic there so this isn't necessary.
          val updLogicalPlan = if (timeRanges.size == 1 && range.startMs == range.endMs) {
            lp
          } else {
            copyLogicalPlanWithUpdatedTimeRange(lp, range)
          }
          // Adjust start seconds in case integer division (during millis-to-seconds conversion)
          //   gives a timestamp outside the valid range.
          val startSec: Long = {
            val msPastSec = range.startMs % 1000
            (range.startMs / 1000) + { if (msPastSec > 0) 1 else 0 }
          }
          val endSec: Long = range.endMs / 1000
          val updQueryContext = qContext.copy(origQueryParams =
            queryParams.copy(startSecs = startSec, endSecs = endSec))
          walkLogicalPlanTree(updLogicalPlan, updQueryContext).plans.head
        }
        if (plans.isEmpty) {
          // No time-range was marked valid.
          EmptyResultExec(qContext, dataset.ref, inProcessPlanDispatcher)
        } else if (plans.size == 1) {
          plans.head
        } else {
          StitchRvsExec(qContext, inProcessPlanDispatcher, None, plans)
        }
      }
    }
  }
  // scalastyle:on method.length

  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                   qContext: QueryContext,
                                   forceInProcess: Boolean = false): PlanResult = {
    // Should avoid this asInstanceOf, far many places where we do this now.
    val params = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    // MultiPartitionPlanner has capability to stitch across time partitions, however, the logic is mostly broken
    // and not well tested. The logic below would not work well for any kind of subquery since their actual
    // start and ends are different from the start/end parameter of the query context. If we are to implement
    // stitching across time, we need to to pass proper parameters to getPartitions() call
    val paramToCheckPartitions = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = getPartitions(logicalPlan, paramToCheckPartitions)

    if (isSinglePartition(partitions)) {
        val (partitionName, startMs, endMs) =
          ( partitions.headOption.map(_.partitionName).getOrElse(localPartitionName),
            params.startSecs * 1000L,
            params.endSecs * 1000L)

        // If the plan is on a single partition, then depending on partition name we either delegate to local or
        // remote planner
        val execPlan = if (partitionName.equals(localPartitionName)) {
            localPartitionPlanner.materialize(logicalPlan, qContext)
        } else {
          // Single partition but remote, send the entire plan remotely
          val remotePartitionEndpoint = partitions.head.endPoint
          val httpEndpoint = remotePartitionEndpoint + params.remoteQueryPath.getOrElse("")
          val remoteContext = if (logicalPlan.isInstanceOf[PeriodicSeriesPlan]) {
            val psp : PeriodicSeriesPlan = logicalPlan.asInstanceOf[PeriodicSeriesPlan]
            val startSecs = psp.startMs / 1000
            val stepSecs = psp.stepMs / 1000
            val endSecs = psp.endMs / 1000
            generateRemoteExecParamsWithStep(qContext, startSecs, stepSecs, endSecs)
          } else {
            generateRemoteExecParams(qContext, startMs, endMs)
          }
          PromQlRemoteExec(
            httpEndpoint, remoteHttpTimeoutMs, remoteContext, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient
          )
        }
        PlanResult(Seq(execPlan))
    } else walkMultiPartitionPlan(logicalPlan, qContext)
  }


  /**
   * Invoked when the plan tree spans multiple plans
   *
   * @param logicalPlan The multi partition LogicalPlan tree
   * @param qContext the QueryContext object
   * @return
   */
  private def walkMultiPartitionPlan(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    logicalPlan match {
      case lp: BinaryJoin                 => materializeMultiPartitionBinaryJoin(lp, qContext)
      case _: MetadataQueryPlan           => throw new IllegalArgumentException("MetadataQueryPlan unexpected here")
      case lp: ApplyInstantFunction       => super.materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw    => super.materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                  => super.materializeAggregate(qContext, lp)
      case lp: ScalarVectorBinaryOperation=> super.materializeScalarVectorBinOp(qContext, lp)
      case lp: ApplyMiscellaneousFunction => super.materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction          => super.materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan    => super.materializeScalarPlan(qContext, lp)
      case _: ScalarTimeBasedPlan         => throw new IllegalArgumentException("ScalarTimeBasedPlan unexpected here")
      case lp: VectorPlan                 => super.materializeVectorPlan(qContext, lp)
      case _: ScalarFixedDoublePlan       => throw new IllegalArgumentException("ScalarFixedDoublePlan unexpected here")
      case lp: ApplyAbsentFunction        => super.materializeAbsentFunction(qContext, lp)
      case lp: ScalarBinaryOperation      => super.materializeScalarBinaryOperation(qContext, lp)
      case lp: ApplyLimitFunction         => super.materializeLimitFunction(qContext, lp)
      case lp: TsCardinalities            => materializeTsCardinalities(lp, qContext)
      case lp: SubqueryWithWindowing       => super.materializeSubqueryWithWindowing(qContext, lp)
      case lp: TopLevelSubquery            => super.materializeTopLevelSubquery(qContext, lp)
      case _: PeriodicSeries |
           _: PeriodicSeriesWithWindowing |
           _: RawChunkMeta |
           _: RawSeries                   => materializePeriodicAndRawSeries(logicalPlan, qContext)
    }
  }

  private def getInvalidRangesLeaf(logicalPlan: LogicalPlan,
                                   queryParams: PromQlQueryParams): Seq[TimeRange] = {
    // Invalidate ranges at/after partition splits.
    // Invalidation only occurs at the splits; a plan that spans only one partition has no invalid ranges.
    val assignmentsSorted = getPartitions(logicalPlan, queryParams).sortBy(pa => pa.timeRange.startMs)
    val invalidRanges = new mutable.ArrayBuffer[TimeRange]
    val lookbackMs = getLookBackMillis(logicalPlan).fold(0L)((a: Long, b: Long) => math.max(a, b))
    val offsetMs = getOffsetMillis(logicalPlan).headOption.getOrElse(0L)
    for (i <- 1 until assignmentsSorted.size) {
      val invalidSize = (assignmentsSorted(i).timeRange.startMs - 1 + lookbackMs) -
                        (assignmentsSorted(i-1).timeRange.endMs + 1)
      if (invalidSize > 0) {
        invalidRanges.append(TimeRange(assignmentsSorted(i-1).timeRange.endMs + offsetMs + 1,
                                       assignmentsSorted(i).timeRange.startMs + offsetMs + lookbackMs + 1))
      }
    }
    invalidRanges
  }

  private def getInvalidRangesPeriodicSeries(logicalPlan: PeriodicSeries,
                                             queryParams: PromQlQueryParams) : Seq[TimeRange] = {
    // Snap the ends of child invalid ranges to periodic steps.
    //   This means valid ranges will also begin at periodic steps.
    val invalidRanges = getInvalidRanges(logicalPlan.rawSeries, queryParams)
    val snappedRanges = invalidRanges.map(r => {
      val totalDiff = r.endMs - logicalPlan.startMs
      val diffToNextStep = totalDiff % logicalPlan.stepMs
      TimeRange(r.startMs, math.min(logicalPlan.endMs, r.endMs + diffToNextStep))
    })
    mergeAndSortRanges(snappedRanges)
  }

  private def getInvalidRangesSubqueryWithWindowing(logicalPlan: SubqueryWithWindowing,
                                                    queryParams: PromQlQueryParams): Seq[TimeRange] = {
    // Extend the inner plan's invalid ranges by the window size.
    val invaldRangesInner = getInvalidRanges(logicalPlan.innerPeriodicSeries, queryParams)
    val invaldRangesArgs = logicalPlan.functionArgs.flatMap(getInvalidRanges(_, queryParams))
    invaldRangesArgs ++ invaldRangesInner.map{range =>
      TimeRange(range.startMs, range.endMs + logicalPlan.subqueryWindowMs)
    }
  }

  private def getInvalidRangesTopLevelSubquery(logicalPlan: TopLevelSubquery,
                                               queryParams: PromQlQueryParams): Seq[TimeRange] = {
    // Invalidate the entire query if any child ranges are invalid.
    val innerInvalidRanges = getInvalidRanges(logicalPlan.innerPeriodicSeries, queryParams)
    if (innerInvalidRanges.isEmpty) {
      Seq.empty
    } else {
      Seq(TimeRange(1000 * queryParams.startSecs,
                    1000 * queryParams.endSecs))
    }
  }

  // scalastyle:off cyclomatic.complexity
  /**
   * Returns a Seq of TimeRanges where the LogicalPlan cannot be evaluated.
   *   For example, cross-partition lookback is unsupported. These time-ranges indicate when
   *   any involved lookbacks (range-selector windows, stale-data lookbacks, etc) cross partitions.
   * @return sequence of time-ranges with inclusive start/end points
   */
  private def getInvalidRanges(logicalPlan: LogicalPlan,
                               promQlQueryParams: PromQlQueryParams) : Seq[TimeRange] = logicalPlan match {
    case lp: BinaryJoin                  => Seq(lp.lhs, lp.rhs).flatMap(getInvalidRanges(_, promQlQueryParams))
    case _: MetadataQueryPlan            => throw new IllegalArgumentException("MetadataQueryPlan unexpected here")
    case lp: ApplyInstantFunction        => (lp.functionArgs ++ Seq(lp.vectors))
                                                .flatMap(getInvalidRanges(_, promQlQueryParams))
    case lp: ApplyInstantFunctionRaw     => (lp.functionArgs ++ Seq(lp.vectors))
                                                .flatMap(getInvalidRanges(_, promQlQueryParams))
    case lp: Aggregate                   => getInvalidRanges(lp.vectors, promQlQueryParams)
    case lp: ScalarVectorBinaryOperation => Seq(lp.vector, lp.scalarArg).flatMap(getInvalidRanges(_, promQlQueryParams))
    case lp: ApplyMiscellaneousFunction  => getInvalidRanges(lp.vectors, promQlQueryParams)
    case lp: ApplySortFunction           => getInvalidRanges(lp.vectors, promQlQueryParams)
    case lp: ScalarVaryingDoublePlan     => (lp.functionArgs ++ Seq(lp.vectors))
                                                .flatMap(getInvalidRanges(_, promQlQueryParams))
    case _: ScalarTimeBasedPlan          => Seq.empty
    case lp: VectorPlan                  => getInvalidRanges(lp.scalars, promQlQueryParams)
    case _: ScalarFixedDoublePlan        => Seq.empty
    case lp: ApplyAbsentFunction         => (lp.functionArgs ++ Seq(lp.vectors)).map(_.asInstanceOf[LogicalPlan])
                                                .flatMap(getInvalidRanges(_, promQlQueryParams))
    case lp: ScalarBinaryOperation       => Seq.empty
    case lp: ApplyLimitFunction          => getInvalidRanges(lp.vectors, promQlQueryParams)
    case lp: TsCardinalities             => Seq.empty
    case lp: SubqueryWithWindowing       => getInvalidRangesSubqueryWithWindowing(lp, promQlQueryParams)
    case lp: TopLevelSubquery            => getInvalidRangesTopLevelSubquery(lp, promQlQueryParams)
    case lp: PeriodicSeries              => getInvalidRangesPeriodicSeries(lp, promQlQueryParams)
    case lp: PeriodicSeriesWithWindowing => (lp.functionArgs ++ Seq(lp.series))
                                                .flatMap(getInvalidRanges(_, promQlQueryParams))
    case _: RawChunkMeta |
         _: RawSeries                    => getInvalidRangesLeaf(logicalPlan, promQlQueryParams)
  }
  // scalastyle:on cyclomatic.complexity

  /**
   * Merges each set of overlapping ranges into one range
   *   with the min/max start/end times, respectively.
   * "Overlapping" is inclusive of start and end points.
   * @return sorted sequence of these merged ranges
   */
  private def mergeAndSortRanges(ranges: Seq[TimeRange]): Seq[TimeRange] = {
    if (ranges.isEmpty) {
      return Nil
    }
    val sortedRanges = ranges.sortBy(r => r.startMs)
    val mergedRanges = new mutable.ArrayBuffer[TimeRange]
    mergedRanges.append(sortedRanges.head)
    for (range <- sortedRanges.tail) {
      if (range.startMs > mergedRanges.last.endMs) {
        // Cannot overlap with any of the previous ranges; create a new range.
        mergedRanges.append(range)
      } else {
        // Extend the previous range to include this range's span.
        mergedRanges(mergedRanges.size - 1) = TimeRange(
          mergedRanges.last.startMs,
          math.max(mergedRanges.last.endMs, range.endMs))
      }
    }
    mergedRanges
  }

  /**
   * Given a sorted sequence of disjoint time-ranges and a "total" range,
   *   inverts the ranges and crops the result to the total range.
   * Range start/ends are inclusive.
   *
   * Example:
   *       Ranges: ----   -------  ---     ---------
   *        Total:          -------------
   *       Result:               --   ---
   *
   * @param ranges: must be sorted and disjoint (range start/ends are inclusive)
   */
  def invertRanges(ranges: Seq[TimeRange],
                   totalRange: TimeRange): Seq[TimeRange] = {
    val invertedRanges = new ArrayBuffer[TimeRange]()
    invertedRanges.append(totalRange)
    var irange = 0

    // ignore all ranges before totalRange
    while (irange < ranges.size &&
           ranges(irange).endMs < totalRange.startMs) {
      irange += 1
    }

    if (irange < ranges.size) {
      // check if the first overlapping range also overlaps the totalRange.start
      if (ranges(irange).startMs <= totalRange.startMs) {
        // if it does, then we either need to adjust the totalRange in the result or remove it altogether.
        if (ranges(irange).endMs < totalRange.endMs) {
          invertedRanges(0) = TimeRange(ranges(irange).endMs + 1, totalRange.endMs)
          irange += 1
        } else {
          return Nil
        }
      }
    }

    // add inverted ranges to the result until one crosses totalRange.endMs
    while (irange < ranges.size && ranges(irange).endMs < totalRange.endMs) {
      invertedRanges(invertedRanges.size - 1) =
        TimeRange(invertedRanges.last.startMs, ranges(irange).startMs - 1)
      invertedRanges.append(TimeRange(ranges(irange).endMs + 1, totalRange.endMs))
      irange += 1
    }

    // check if a range overlaps totalRange.endMs; if it does, adjust final inverted range
    if (irange < ranges.size && ranges(irange).startMs < totalRange.endMs) {
      invertedRanges(invertedRanges.size - 1) =
        TimeRange(invertedRanges.last.startMs, ranges(irange).startMs - 1)
    }

    invertedRanges
  }

  private def getRoutingKeys(logicalPlan: LogicalPlan) = {
    val columnFilterGroup = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val routingKeys = dataset.options.nonMetricShardColumns
      .map(x => (x, LogicalPlan.getColumnValues(columnFilterGroup, x)))
    if (routingKeys.flatMap(_._2).isEmpty) Seq.empty else routingKeys.filter(x => x._2.nonEmpty)
  }

  private def generateRemoteExecParams(queryContext: QueryContext, startMs: Long, endMs: Long) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(origQueryParams = queryParams.copy(startSecs = startMs/1000, endSecs = endMs / 1000),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false))
  }

  private def generateRemoteExecParamsWithStep(
    queryContext: QueryContext, startSecs: Long, stepSecs: Long, endSecs: Long
  ) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(
      origQueryParams = queryParams.copy(startSecs = startSecs, stepSecs = stepSecs, endSecs = endSecs),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false)
    )
  }

  /**
   * Gets the partition Assignment for the given plan
   */
  private def getPartitions(logicalPlan: LogicalPlan,
                            queryParams: PromQlQueryParams,
                            infiniteTimeRange: Boolean = false) : Seq[PartitionAssignment] = {

    //1.  Get a Seq of all Leaf node filters
    val leafFilters = LogicalPlan.getColumnFilterGroup(logicalPlan)
    val nonMetricColumnSet = dataset.options.nonMetricShardColumns.toSet
    //2. Filter from each leaf node filters to keep only nonShardKeyColumns and convert them to key value map
    val routingKeyMap = leafFilters.map(cf => {
      cf.filter(col => nonMetricColumnSet.contains(col.column)).map(
        x => (x.column, x.filter.valuesStrings.head.toString)).toMap
    })

    // 3. Determine the query time range
    val queryTimeRange = if (infiniteTimeRange) {
      TimeRange(0, Long.MaxValue)
    } else {
      // 3a. Get the start and end time is ms based on the lookback, offset and the user provided start and end time
      val (maxOffsetMs, minOffsetMs) = LogicalPlanUtils.getOffsetMillis(logicalPlan)
        .foldLeft((Long.MinValue, Long.MaxValue)) {
          case ((accMax, accMin), currValue) => (accMax.max(currValue), accMin.min(currValue))
        }

      val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - maxOffsetMs,
        (queryParams.endSecs * 1000) - minOffsetMs)
      val lookBackMs = getLookBackMillis(logicalPlan).max

      //3b Get the Query time range based on user provided range, offsets in previous steps and lookback
      TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
        periodicSeriesTimeWithOffset.endMs)
    }

    //4. Based on the map in 2 and time range in 5, get the partitions to query
    routingKeyMap.flatMap(metricMap =>
      partitionLocationProvider.getPartitions(metricMap, queryTimeRange))
  }

  /**
   * Checks if all the PartitionAssignments belong to same partition
   */
  private def isSinglePartition(partitions: Seq[PartitionAssignment]) : Boolean = {
    if (partitions.isEmpty)
      true
    else {
      val partName = partitions.head.partitionName
      partitions.forall(_.partitionName.equals(partName))
    }
  }


  /**
   *
   * @param logicalPlan Logical plan
   * @param queryParams PromQlQueryParams having query details
   * @return Returns PartitionAssignment, lookback, offset and routing keys
   */
  private def resolvePartitionsAndRoutingKeys(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) = {

    val routingKeys = getRoutingKeys(logicalPlan)

    val offsetMs = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    // To cover entire time range for queries like sum(foo offset 2d) - sum(foo)
    val periodicSeriesTimeWithOffset = TimeRange((queryParams.startSecs * 1000) - offsetMs.max,
      (queryParams.endSecs * 1000) - offsetMs.min)
    val lookBackMs = getLookBackMillis(logicalPlan).max

    // Time at which raw data would be retrieved which is used to get partition assignments.
    // It should have time with offset and lookback as we need raw data at time including offset and lookback.
    val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackMs,
      periodicSeriesTimeWithOffset.endMs)

    val partitions = if (routingKeys.isEmpty) List.empty
    else {
      val routingKeyMap = routingKeys.map(x => (x._1, x._2.head)).toMap
      partitionLocationProvider.getPartitions(routingKeyMap, queryTimeRange).
        sortBy(_.timeRange.startMs)
    }
    if (partitions.isEmpty && routingKeys.nonEmpty)
      logger.warn(s"No partitions found for routing keys: $routingKeys")

    (partitions, lookBackMs, offsetMs, routingKeys)
  }

  /**
    * Materialize all queries except Binary Join and Metadata
    */
  def materializePeriodicAndRawSeries(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, lookBackMs, offsetMs, routingKeys) = resolvePartitionsAndRoutingKeys(logicalPlan, queryParams)
    val execPlan = if (partitions.isEmpty || routingKeys.forall(_._2.isEmpty))
      localPartitionPlanner.materialize(logicalPlan, qContext)
    else {
      val stepMs = queryParams.stepSecs * 1000
      val isInstantQuery: Boolean = if (queryParams.startSecs == queryParams.endSecs) true else false
      var prevPartitionStart = queryParams.startSecs * 1000
      val execPlans = partitions.zipWithIndex.map { case (p, i) =>
        // First partition should start from query start time, no need to calculate time according to step for instant
        // queries
        val startMs =
          if (i == 0 || isInstantQuery) {
            queryParams.startSecs * 1000
          } else {
            // The logic below does not work for partitions split across time as we encounter a hole
            // in the produced result. The size of the hole is lookBackMs + stepMs
            val numStepsInPrevPartition = (p.timeRange.startMs - prevPartitionStart + lookBackMs) / stepMs
            val lastPartitionInstant = prevPartitionStart + numStepsInPrevPartition * stepMs
            val start = lastPartitionInstant + stepMs
            // If query duration is less than or equal to lookback start will be greater than query end time
            if (start > (queryParams.endSecs * 1000)) queryParams.endSecs * 1000 else start
          }
        prevPartitionStart = startMs
        // we assume endMs should be equal partition endMs but if the query's end is smaller than partition endMs,
        // why would we want to stretch the query??
        val endMs = if (isInstantQuery) queryParams.endSecs * 1000 else p.timeRange.endMs + offsetMs.min
        logger.debug(s"partitionInfo=$p; updated startMs=$startMs, endMs=$endMs")
        if (p.partitionName.equals(localPartitionName)) {
          val lpWithUpdatedTime = copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(startMs, endMs))
          localPartitionPlanner.materialize(lpWithUpdatedTime, qContext)
        } else {
          val httpEndpoint = p.endPoint + queryParams.remoteQueryPath.getOrElse("")
          PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, generateRemoteExecParams(qContext, startMs, endMs),
            inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else {
        // TODO: Do we pass in QueryContext in LogicalPlan's helper rvRangeForPlan?
        StitchRvsExec(qContext, inProcessPlanDispatcher, rvRangeFromPlan(logicalPlan),
          execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]))
      }
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
    PlanResult(execPlan:: Nil)
  }

  private def materializeMultiPartitionBinaryJoin(logicalPlan: BinaryJoin, qContext: QueryContext): PlanResult = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsExec = walkLogicalPlanTree(logicalPlan.lhs, lhsQueryContext).plans
    val rhsExec = walkLogicalPlanTree(logicalPlan.rhs, rhsQueryContext).plans

    val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher(queryConfig), lhsExec, rhsExec, logicalPlan.operator,
        LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
    else
      BinaryJoinExec(qContext, inProcessPlanDispatcher, lhsExec, rhsExec, logicalPlan.operator,
        logicalPlan.cardinality, LogicalPlanUtils.renameLabels(logicalPlan.on, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn),
        LogicalPlanUtils.renameLabels(logicalPlan.include, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
    PlanResult(execPlan :: Nil)
  }

  private def copy(lp: MetadataQueryPlan, startMs: Long, endMs: Long): MetadataQueryPlan = lp match {
    case sk: SeriesKeysByFilters       => sk.copy(startMs = startMs, endMs = endMs)
    case lv: LabelValues               => lv.copy(startMs = startMs, endMs = endMs)
    case ln: LabelNames                => ln.copy(startMs = startMs, endMs = endMs)
    case lc: LabelCardinality          => lc.copy(startMs = startMs, endMs = endMs)
  }

  def materializeMetadataQueryPlan(lp: MetadataQueryPlan, qContext: QueryContext): PlanResult = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]


    // LabelCardinality is a special case, here the partitions to send this query to is not  the authorized partition
    // but the actual one where data resides, similar to how non metadata plans work, however, getting label cardinality
    // is a metadata operation and shares common components with other metadata endpoints.
    val partitions = lp match {
      case lc: LabelCardinality       => getPartitions(lc, qContext.origQueryParams.asInstanceOf[PromQlQueryParams])
      case _                          => getMetadataPartitions(lp.filters,
        TimeRange(queryParams.startSecs * 1000, queryParams.endSecs * 1000))
    }

    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"No partitions found for ${queryParams.startSecs}, ${queryParams.endSecs}")
      localPartitionPlanner.materialize(lp, qContext)
    }
    else {
      val execPlans = partitions.map { p =>
        logger.debug(s"partitionInfo=$p; queryParams=$queryParams")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(
            copy(lp, startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
        else {
          val params: Map[String, String] = lp match {
            case _: SeriesKeysByFilters |
                 _: LabelNames |
                 _: LabelCardinality    => Map("match[]" -> LogicalPlanParser.metatadataMatchToQuery(lp))
            case lv: LabelValues        => PlannerUtil.getLabelValuesUrlParams(lv, queryParams)
          }
          createMetadataRemoteExec(qContext, p, params)
        }
      }
      if (execPlans.size == 1) execPlans.head
      else lp match {
        case _: SeriesKeysByFilters => PartKeysDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelValues => LabelValuesDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelNames => LabelNamesDistConcatExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
        case _: LabelCardinality => LabelCardinalityReduceExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
      }
    }
    PlanResult(execPlan::Nil)
  }

  def materializeTsCardinalities(lp: TsCardinalities, qContext: QueryContext): PlanResult = {

    import TsCardinalities._

    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val partitions = if (lp.shardKeyPrefix.size >= 2) {
      // At least a ws/ns pair is required to select specific partitions.
      getPartitions(lp, queryParams, infiniteTimeRange = true)
    } else {
      logger.info(s"(ws, ns) pair not provided in prefix=${lp.shardKeyPrefix};" +
                  s"dispatching to all authorized partitions")
      getMetadataPartitions(lp.filters(), TimeRange(0, Long.MaxValue))
    }
    val execPlan = if (partitions.isEmpty) {
      logger.warn(s"no partitions found for $lp; defaulting to local planner")
      localPartitionPlanner.materialize(lp, qContext)
    } else {
      val execPlans = partitions.map { p =>
        logger.debug(s"partition=$p; plan=$lp")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(lp, qContext)
        else {
          val params = Map(
            "match[]" -> ("{" + SHARD_KEY_LABELS.zip(lp.shardKeyPrefix)
                           .map{ case (label, value) => s"""$label="$value""""}
                           .mkString(",") + "}"),
            "numGroupByFields" -> lp.numGroupByFields.toString)
          createMetadataRemoteExec(qContext, p, params)
        }
      }
      if (execPlans.size == 1) {
        execPlans.head
      } else {
        TsCardReduceExec(qContext, inProcessPlanDispatcher,
          execPlans.sortWith((x, _) => !x.isInstanceOf[MetadataRemoteExec]))
      }
    }
    PlanResult(execPlan::Nil)
  }

  def getMetadataPartitions(filters: Seq[ColumnFilter], timeRange: TimeRange): List[PartitionAssignment] = {
    val nonMetricShardKeyFilters = filters.filter(f => dataset.options.nonMetricShardColumns.contains(f.column))
    partitionLocationProvider.getMetadataPartitions(nonMetricShardKeyFilters, timeRange)
  }

  private def createMetadataRemoteExec(qContext: QueryContext, partitionAssignment: PartitionAssignment,
                                       urlParams: Map[String, String]) = {
    val finalQueryContext = generateRemoteExecParams(
      qContext, partitionAssignment.timeRange.startMs, partitionAssignment.timeRange.endMs)
    val httpEndpoint = partitionAssignment.endPoint + finalQueryContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      remoteQueryPath.getOrElse("")
    MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
      urlParams, finalQueryContext, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
  }
}

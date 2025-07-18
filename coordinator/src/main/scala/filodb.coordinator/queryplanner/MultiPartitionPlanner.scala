package filodb.coordinator.queryplanner

import java.util.concurrent.ConcurrentHashMap

import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import com.typesafe.scalalogging.StrictLogging
import io.grpc.ManagedChannel

import filodb.coordinator.queryplanner.LogicalPlanUtils._
import filodb.coordinator.queryplanner.PlannerUtil.rewritePlanWithRemoteRawExport
import filodb.core.{StaticTargetSchemaProvider, TargetSchemaProvider}
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryConfig, QueryContext, QueryUtils, RangeParams, RvRange}
import filodb.core.query.Filter.{Equals, EqualsRegex}
import filodb.grpc.GrpcCommonUtils
import filodb.query._
import filodb.query.LogicalPlan._
import filodb.query.RangeFunctionId.AbsentOverTime
import filodb.query.exec._

//scalastyle:off file.size.limit
case class PartitionDetails(partitionName: String, httpEndPoint: String,
                            grpcEndPoint: Option[String], proportion: Float)
trait PartitionAssignmentTrait {
  val proportionMap: Map[String, PartitionDetails]
  val timeRange: TimeRange
}

case class PartitionAssignment(partitionName: String, httpEndPoint: String, timeRange: TimeRange,
                               grpcEndPoint: Option[String] = None) extends PartitionAssignmentTrait {
  val proportionMap: Map[String, PartitionDetails] =
    Map(partitionName -> PartitionDetails(partitionName, httpEndPoint, grpcEndPoint, 1.0f))
}

/**
 * The partition assignment case class.
 * It is a different assignment partition if the proportionMap is different.
 * @param proportionMap the map from a partition to its proportion.
 *                      For example {p1: 0.1, p2:0.9} means this assingment has two partitions
 *                      with around 10% data goes to p1 and 90% data goes to p2.
 * @param timeRange the timeRange when the partition assignment is effective.
 */
case class PartitionAssignmentV2(proportionMap: Map[String, PartitionDetails],
                                 timeRange: TimeRange) extends PartitionAssignmentTrait {
  assert(proportionMap.map(_._2.proportion).sum == 1.0f, s"the total proportion of proportionMap should be 1.0f." +
    s" However the actual is $proportionMap")
}


trait PartitionLocationProvider {

  // keep this function for backward compatibility.
  def getPartitions(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignment]
  def getMetadataPartitions(nonMetricShardKeyFilters: Seq[ColumnFilter],
                            timeRange: TimeRange): List[PartitionAssignment]
  def getPartitionsTrait(routingKey: Map[String, String], timeRange: TimeRange): List[PartitionAssignmentTrait] = {
    getPartitions(routingKey, timeRange)
  }
  def getMetadataPartitionsTrait(nonMetricShardKeyFilters: Seq[ColumnFilter],
                                 timeRange: TimeRange): List[PartitionAssignmentTrait] = {
        getMetadataPartitions(nonMetricShardKeyFilters, timeRange)
  }
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
class MultiPartitionPlanner(val partitionLocationProvider: PartitionLocationProvider,
                            localPartitionPlanner: QueryPlanner,
                            localPartitionName: String,
                            val dataset: Dataset,
                            val queryConfig: QueryConfig,
                            remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient,
                            channels: ConcurrentMap[String, ManagedChannel] =
                            new ConcurrentHashMap[String, ManagedChannel]().asScala,
                            _targetSchemaProvider: TargetSchemaProvider = StaticTargetSchemaProvider())
  extends PartitionLocationPlanner(dataset, partitionLocationProvider) with StrictLogging {

  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options

  val plannerSelector: String = queryConfig.plannerSelector
    .getOrElse(throw new IllegalArgumentException("plannerSelector is mandatory"))

  val remoteHttpTimeoutMs: Long = queryConfig.remoteHttpTimeoutMs.getOrElse(60000)

  val datasetMetricColumn: String = dataset.options.metricColumn

  private def targetSchemaProvider(qContext: QueryContext): TargetSchemaProvider = {
    qContext.plannerParams.targetSchemaProviderOverride.getOrElse(_targetSchemaProvider)
  }

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
      case mqp: MetadataQueryPlan             => materializeMetadataQueryPlan(mqp, qContext).plans.head
      case lp: TsCardinalities                => materializeTsCardinalities(lp, qContext).plans.head
      case _                                  => walkLogicalPlanTree(logicalPlan, qContext).plans.head
    }
  }

  // scalastyle:off method.length
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                   qContext: QueryContext,
                                   forceInProcess: Boolean = false): PlanResult = {
    // Should avoid this asInstanceOf, far many places where we do this now.
    // MultiPartitionPlanner has capability to stitch across time partitions, however, the logic is mostly broken
    // and not well tested. The logic below would not work well for any kind of subquery since their actual
    // start and ends are different from the start/end parameter of the query context. If we are to implement
    // stitching across time, we need to to pass proper parameters to getPartitions() call
    if (forceInProcess) {
      // If inprocess is required, we will rely on the DefaultPlanner's implementation as the expectation is that the
      // raw series is doing a remote call to get all the data.
      logicalPlan match {
        case lp: RawSeries    if lp.supportsRemoteDataCall=>
            val params = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
            val rs = lp.rangeSelector.asInstanceOf[IntervalSelector]

            val (rawExportStart, rawExportEnd) =
              (rs.from - lp.offsetMs.getOrElse(0L) - lp.lookbackMs.getOrElse(0L), rs.to - lp.offsetMs.getOrElse(0L))

            val partition = getPartitions(lp, params)
            assert(partition.nonEmpty, s"Unexpected to see partitions empty for logicalPlan=$lp and param=$params")
            // Raw export from both involved partitions for the entire time duration as the shard-key migration
            // is not guaranteed to happen exactly at the time of split
            val execPlans = partition.map(pa => {
              val (thisPartitionStartMs, thisPartitionEndMs) = (rawExportStart, rawExportEnd)
              val timeRangeOverride = TimeRange(thisPartitionEndMs, thisPartitionEndMs)
              val totalOffsetThisPartitionMs = thisPartitionEndMs - thisPartitionStartMs
              val thisPartitionLp = lp.copy(offsetMs = None, lookbackMs = Some(totalOffsetThisPartitionMs))
              val newPromQlParams = params.copy(promQl = LogicalPlanParser.convertToQuery(thisPartitionLp),
                  startSecs = timeRangeOverride.startMs / 1000L,
                  endSecs = timeRangeOverride.endMs / 1000L,
                  stepSecs = 1
                )
              val newContext = qContext.copy(origQueryParams = newPromQlParams)
              materializeForAssignment(thisPartitionLp, pa, newContext, Some(timeRangeOverride))
            })
          PlanResult(
            Seq( if (execPlans.tail == Seq.empty) execPlans.head
            else {
              val newPromQlParams = params.copy(promQl = LogicalPlanParser.convertToQuery(lp))
                StitchRvsExec(qContext.copy(origQueryParams = newPromQlParams)
                  , inProcessPlanDispatcher, None,
                  execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]),
                  enableApproximatelyEqualCheck = queryConfig.routingConfig.enableApproximatelyEqualCheckInStitch)
            }
            )
          )
        case _ : LogicalPlan  => super.defaultWalkLogicalPlanTree(logicalPlan, qContext, forceInProcess)
      }
    } else {
      val params = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val paramToCheckPartitions = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val partitions = getPartitions(logicalPlan, paramToCheckPartitions)
      if (isSinglePartition(partitions)) {
        val (partitionName, startMs, endMs, grpcEndpoint) = partitions.headOption match {
          case Some(pa: PartitionAssignmentTrait)
          => (pa.proportionMap.keys.head, params.startSecs * 1000L,
            params.endSecs * 1000L, pa.proportionMap.values.head.grpcEndPoint)
          case None => (localPartitionName, params.startSecs * 1000L, params.endSecs * 1000L, None)
        }

        // If the plan is on a single partition, then depending on partition name we either delegate to local or
        // remote planner
        val execPlan = if (partitionName.equals(localPartitionName)) {
          localPartitionPlanner.materialize(logicalPlan, qContext)
        } else {
          val remoteContext = logicalPlan match {
            case tls: TopLevelSubquery =>
              val instantTime = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].startSecs
              val stepSecs = tls.stepMs / 1000
              generateRemoteExecParamsWithStep(qContext, instantTime, stepSecs, instantTime, tls)
            case psp: PeriodicSeriesPlan =>
              val startSecs = psp.startMs / 1000
              val stepSecs = psp.stepMs / 1000
              val endSecs = psp.endMs / 1000
              generateRemoteExecParamsWithStep(qContext, startSecs, stepSecs, endSecs, psp)
            case _ =>
              generateRemoteExecParams(qContext, startMs, endMs, logicalPlan)
          }
          // Single partition but remote, send the entire plan remotely
          if (grpcEndpoint.isDefined && !(queryConfig.grpcPartitionsDenyList.contains("*") ||
            queryConfig.grpcPartitionsDenyList.contains(partitionName.toLowerCase))) {
            val endpoint = grpcEndpoint.get
            val channel = channels.getOrElseUpdate(endpoint, GrpcCommonUtils.buildChannelFromEndpoint(endpoint))
            PromQLGrpcRemoteExec(channel, remoteHttpTimeoutMs, remoteContext, inProcessPlanDispatcher,
              dataset.ref, plannerSelector)
          } else {
            val remotePartitionEndpoint = partitions.head.proportionMap.values.head.httpEndPoint
            val httpEndpoint = remotePartitionEndpoint + params.remoteQueryPath.getOrElse("")
            PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs, remoteContext, inProcessPlanDispatcher,
              dataset.ref, remoteExecHttpClient)
          }
        }
        PlanResult(Seq(execPlan))
      } else walkMultiPartitionPlan(logicalPlan, qContext)
    }
  }
  // scalastyle:on method.length

  // scalastyle:off cyclomatic.complexity
  /**
   * Invoked when the plan tree spans multiple plans
   *
   * @param logicalPlan The multi partition assignment LogicalPlan tree
   * @param qContext the QueryContext object
   * @return
   */
  private def walkMultiPartitionPlan(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    logicalPlan match {
      case lp: BinaryJoin                  => materializePlanHandleSplitLeaf(lp, qContext)
      case _: MetadataQueryPlan            => throw new IllegalArgumentException(
                                                          "MetadataQueryPlan unexpected here")
      case lp: ApplyInstantFunction        => materializePlanHandleSplitLeaf(lp, qContext)
      case lp: ApplyInstantFunctionRaw     => super.materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                   => materializePlanHandleSplitLeaf(lp, qContext)
      case lp: ScalarVectorBinaryOperation => materializePlanHandleSplitLeaf(lp, qContext)
      case lp: ApplyMiscellaneousFunction  => super.materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => super.materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => materializePlanHandleSplitLeaf(lp, qContext)
      case _: ScalarTimeBasedPlan          => throw new IllegalArgumentException(
                                                          "ScalarTimeBasedPlan unexpected here")
      case lp: VectorPlan                  => super.materializeVectorPlan(qContext, lp)
      case _: ScalarFixedDoublePlan        => throw new IllegalArgumentException(
                                                          "ScalarFixedDoublePlan unexpected here")
      case lp: ApplyAbsentFunction         => materializePlanHandleSplitLeaf(lp, qContext)
      case lp: ScalarBinaryOperation       => super.materializeScalarBinaryOperation(qContext, lp)
      case lp: ApplyLimitFunction          => super.materializeLimitFunction(qContext, lp)
      case lp: TsCardinalities             => materializeTsCardinalities(lp, qContext)
      case lp: SubqueryWithWindowing       => materializePlanHandleSplitLeaf(lp, qContext)
      case lp: TopLevelSubquery            => super.materializeTopLevelSubquery(qContext, lp)
      case _: BinaryJoin |
           _: ApplyInstantFunction |
           _: Aggregate |
           _: ScalarVectorBinaryOperation |
           _: ScalarVaryingDoublePlan |
           _: ApplyAbsentFunction |
           _: SubqueryWithWindowing |
           _: PeriodicSeriesWithWindowing |
           _: PeriodicSeries |
           _: RawChunkMeta => materializePlanHandleSplitLeaf(logicalPlan, qContext)
      case raw: RawSeries                  =>
                                              val params = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
                                              if(getPartitions(raw, params).tail.nonEmpty
                                                && queryConfig.routingConfig.supportRemoteRawExport)
                                                this.walkLogicalPlanTree(
                                                  raw.copy(supportsRemoteDataCall = true),
                                                  qContext,
                                                  forceInProcess = true)
                                              else
                                                materializePlanHandleSplitLeaf(logicalPlan, qContext)
    }
  }
  // scalastyle:on cyclomatic.complexity

  def getRoutingKeys(logicalPlan: LogicalPlan): Set[Map[String, String]] = {
    val rawSeriesPlans = LogicalPlan.findLeafLogicalPlans(logicalPlan)
      .filter(_.isInstanceOf[RawSeries])
    rawSeriesPlans.flatMap { rs =>
      val filterGroups = LogicalPlan.getNonMetricShardKeyFilters(rs, dataset.options.nonMetricShardColumns)
      assert(filterGroups.size == 1, "RawSeries does not have exactly one filter group: " + rs)
      // Map each key to all values given by the filters.
      val keyToValues = filterGroups.head.foldLeft(new mutable.HashMap[String, mutable.HashSet[String]])
        { case (acc, colFilter) =>
          val valueSet = acc.getOrElseUpdate(colFilter.column, new mutable.HashSet[String])
          colFilter.filter match {
            case eq: Equals => valueSet.add(eq.value.toString)
            case re: EqualsRegex if QueryUtils.containsPipeOnlyRegex(re.value.toString) =>
              val values = QueryUtils.splitAtUnescapedPipes(re.value.toString)
              values.foreach(valueSet.add)
            case _ => throw new IllegalArgumentException(
              s"""shard keys must be filtered by equality or "|"-only regex. filter=${colFilter}""")
          }
          acc
        }
      QueryUtils.makeAllKeyValueCombos(keyToValues.map(pair => (pair._1, pair._2.toSeq)).toMap)
    }.toSet
  }

  private def generateRemoteExecParams(queryContext: QueryContext, startMs: Long,
                                       endMs: Long, logicalPlan: LogicalPlan) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]

    val remotePlan = logicalPlan match {
      case _ : TsCardinalities =>  queryParams.promQl
      case _ => LogicalPlanParser.convertToQuery(logicalPlan)
    }
    queryContext.copy(
      origQueryParams = queryParams.copy(promQl = remotePlan,
        startSecs = startMs / 1000, endSecs = endMs / 1000),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false))
  }

  private def generateRemoteExecParamsWithStep(
    queryContext: QueryContext, startSecs: Long, stepSecs: Long, endSecs: Long,
    logicalPlan: LogicalPlan
  ) = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    queryContext.copy(
      origQueryParams = queryParams.copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan),
        startSecs = startSecs, stepSecs = stepSecs, endSecs = endSecs),
      plannerParams = queryContext.plannerParams.copy(processMultiPartition = false)
    )
  }

  /**
   *
   * @param logicalPlan Logical plan
   * @param queryParams PromQlQueryParams having query details
   * @return Returns PartitionAssignment, lookback, offset and routing keys
   */
  private def resolveAssignmentsAndRoutingKeys(logicalPlan: LogicalPlan, queryParams: PromQlQueryParams) = {

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

    val assignments = if (routingKeys.isEmpty) List.empty
    else {
      routingKeys.flatMap{ keys =>
        partitionLocationProvider.getPartitionsTrait(keys, queryTimeRange).
          sortBy(_.timeRange.startMs)
      }.toList
    }
    if (assignments.isEmpty && routingKeys.nonEmpty)
      logger.warn(s"No partitions found for routing keys: $routingKeys")

    (assignments, lookBackMs, offsetMs, routingKeys)
  }

  /**
    * Materialize all queries except Binary Join and Metadata
    */
  private def materializePeriodicAndRawSeries(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val (partitions, lookBackMs, offsetMs, routingKeys) = resolveAssignmentsAndRoutingKeys(logicalPlan, queryParams)
    val execPlan = if (partitions.isEmpty || routingKeys.isEmpty)
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
        // TODO: playing it safe for now with the TimeRange override; the parameter can eventually be removed.
        materializeForAssignment(logicalPlan, p, qContext, timeRangeOverride = Some(TimeRange(startMs, endMs)))
      }
      if (execPlans.size == 1) execPlans.head
      else {
        // TODO: Do we pass in QueryContext in LogicalPlan's helper rvRangeForPlan?
        StitchRvsExec(qContext, inProcessPlanDispatcher, rvRangeFromPlan(logicalPlan),
          execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]),
          enableApproximatelyEqualCheck = queryConfig.routingConfig.enableApproximatelyEqualCheckInStitch)
      }
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
    PlanResult(execPlan:: Nil)
  }

  private def materializeForBinaryJoinAssignment(binaryJoin: BinaryJoin,
                                                 assignment: PartitionAssignmentTrait,
                                                 queryContext: QueryContext,
                                                 timeRangeOverride: Option[TimeRange] = None): ExecPlan = {
    val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val proportionMap = assignment.proportionMap
    val leftContext = queryContext.copy(origQueryParams =
      queryParams.copy(promQl = LogicalPlanParser.convertToQuery(binaryJoin.lhs)))
    val lhsList = proportionMap.map(entry => {
      val partitionDetails = entry._2
      materializeForPartition(binaryJoin.lhs, partitionDetails.partitionName,
        partitionDetails.grpcEndPoint, partitionDetails.httpEndPoint, leftContext, timeRangeOverride)
    }).toSeq
    val rightContext = queryContext.copy(origQueryParams =
      queryParams.copy(promQl = LogicalPlanParser.convertToQuery(binaryJoin.rhs)))
    val rhsList = proportionMap.map(entry => {
      val partitionDetails = entry._2
      materializeForPartition(binaryJoin.rhs, partitionDetails.partitionName,
        partitionDetails.grpcEndPoint, partitionDetails.httpEndPoint, rightContext, timeRangeOverride)
    }).toSeq
    val dispatcher = PlannerUtil.pickDispatcher(lhsList ++ rhsList)
    if (binaryJoin.operator.isInstanceOf[SetOperator])
      exec.SetOperatorExec(queryContext, dispatcher, lhsList, rhsList, binaryJoin.operator,
        binaryJoin.on.map(LogicalPlanUtils.renameLabels(_, dsOptions.metricColumn)),
        LogicalPlanUtils.renameLabels(binaryJoin.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
        rvRangeFromPlan(binaryJoin))
    else
      BinaryJoinExec(queryContext, dispatcher, lhsList, rhsList, binaryJoin.operator,
        binaryJoin.cardinality,
        binaryJoin.on.map(LogicalPlanUtils.renameLabels(_, dsOptions.metricColumn)),
        LogicalPlanUtils.renameLabels(binaryJoin.ignoring, dsOptions.metricColumn), binaryJoin.include,
        dsOptions.metricColumn, rvRangeFromPlan(binaryJoin))
  }

  private def materializeForAggregateAssignment(aggregate: Aggregate,
                                                assignment: PartitionAssignmentTrait,
                                                queryContext: QueryContext,
                                                timeRangeOverride: Option[TimeRange] = None): ExecPlan = {
    val proportionMap = assignment.proportionMap
    val childQueryContext = queryContext.copy(
      plannerParams = queryContext.plannerParams.copy(skipAggregatePresent = true))
    val plans = proportionMap.values.map(details => {
        materializeForPartition(aggregate, details.partitionName,
          details.grpcEndPoint, details.httpEndPoint, childQueryContext, timeRangeOverride)
    }).toSeq
    if (plans.size == 1) plans.head
    else {
      plans.filter(_.isInstanceOf[PromQlRemoteExec]).foreach(
        _.addRangeVectorTransformer(AggregateMapReduce(aggregate.operator, aggregate.params, aggregate.clauseOpt))
      )
      val dispatcher = PlannerUtil.pickDispatcher(plans)
      val reducer = MultiPartitionReduceAggregateExec(queryContext, dispatcher,
        plans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
      if (!queryContext.plannerParams.skipAggregatePresent) {
        val promQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
        reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params,
          RangeParams(promQlQueryParams.startSecs, promQlQueryParams.stepSecs, promQlQueryParams.endSecs)))
      }
      reducer
    }
  }
  /**
   * materialize a plan that based on a multi-partition assignment.
   *
   * @param logicalPlan the logic plan.
   * @param assignment the partition assignment that may contains one or more partitions.
   * @param queryContext the query context.
   * @param timeRangeOverride if given, the plan will be materialized to this range. Otherwise, the
   *                          range is computed from the PromQlQueryParams.
   * @return an ExecPlan.
   */
  private def materializeForAssignment(logicalPlan: LogicalPlan,
                                       assignment: PartitionAssignmentTrait,
                                       queryContext: QueryContext,
                                       timeRangeOverride: Option[TimeRange] = None): ExecPlan = {
    assignment match {
      case PartitionAssignment(partitionName, httpEndPoint, _, grpcEndPoint) =>
        materializeForPartition(logicalPlan, partitionName, grpcEndPoint, httpEndPoint, queryContext, timeRangeOverride)
      case PartitionAssignmentV2(proportionMap, _) if proportionMap.size == 1 =>
        val details = proportionMap.head._2
        materializeForPartition(logicalPlan, details.partitionName, details.grpcEndPoint, details.httpEndPoint,
          queryContext, timeRangeOverride)
      case PartitionAssignmentV2(proportionMap, _) =>
        logicalPlan match {
          case aggregate: Aggregate =>
            materializeForAggregateAssignment(aggregate, assignment, queryContext, timeRangeOverride)
          case binaryJoin: BinaryJoin =>
            materializeForBinaryJoinAssignment(binaryJoin, assignment, queryContext, timeRangeOverride)
          case psw: PeriodicSeriesWithWindowing if psw.function == AbsentOverTime =>
            val plans = proportionMap.map(entry => {
              val partitionDetails = entry._2
              materializeForPartition(logicalPlan, partitionDetails.partitionName,
                partitionDetails.grpcEndPoint, partitionDetails.httpEndPoint, queryContext, timeRangeOverride)
            }).toSeq
            val dispatcher = PlannerUtil.pickDispatcher(plans)
            // 0 present 1 absent => 01/10/00 are present. 11 is absent.
            val reducer = MultiPartitionReduceAggregateExec(queryContext, dispatcher,
              plans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]), AggregationOperator.Absent, Nil)
            if (!queryContext.plannerParams.skipAggregatePresent) {
              val promQlQueryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
              reducer.addRangeVectorTransformer(AggregatePresenter(AggregationOperator.Absent, Nil,
                RangeParams(promQlQueryParams.startSecs, promQlQueryParams.stepSecs, promQlQueryParams.endSecs)))
            }
            reducer
          case _ =>
            val plans = proportionMap.map(entry => {
              val partitionDetails = entry._2
              materializeForPartition(logicalPlan, partitionDetails.partitionName,
                partitionDetails.grpcEndPoint, partitionDetails.httpEndPoint, queryContext, timeRangeOverride)
            }).toSeq
            val dispatcher = PlannerUtil.pickDispatcher(plans)
            MultiPartitionDistConcatExec(queryContext, dispatcher, plans)
        }
    }
  }

  /**
   * If the argument partition is local, materialize the LogicalPlan with the local planner.
   * Otherwise, create a PromQlRemoteExec.
   *
   * @param timeRangeOverride : if given, the plan will be materialized to this range. Otherwise, the
   *                          range is computed from the PromQlQueryParams.
   */
  private def materializeForPartition(logicalPlan: LogicalPlan,
                                      partition: PartitionDetails,
                                      queryContext: QueryContext,
                                      timeRangeOverride: Option[TimeRange] = None): ExecPlan = {
    val qContextWithOverride = timeRangeOverride.map { r =>
      val oldParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val newParams = oldParams.copy(startSecs = r.startMs / 1000, endSecs = r.endMs / 1000)
      queryContext.copy(origQueryParams = newParams)
    }.getOrElse(queryContext)
    val queryParams = qContextWithOverride.origQueryParams.asInstanceOf[PromQlQueryParams]
    val timeRange = timeRangeOverride.getOrElse(TimeRange(1000 * queryParams.startSecs, 1000 * queryParams.endSecs))
    val (partitionName, grpcEndpoint) = (partition.partitionName, partition.grpcEndPoint)
    if (partitionName.equals(localPartitionName)) {
      // FIXME: the below check is needed because subquery tests fail when their
      //   time-ranges are updated even with the original query params.
      val lpWithUpdatedTime = if (timeRangeOverride.isDefined) {
        copyLogicalPlanWithUpdatedSeconds(logicalPlan, timeRange.startMs / 1000, timeRange.endMs / 1000)
      } else logicalPlan
      localPartitionPlanner.materialize(lpWithUpdatedTime, qContextWithOverride)
    } else {
      val ctx = generateRemoteExecParams(qContextWithOverride, timeRange.startMs, timeRange.endMs, logicalPlan)
      if (grpcEndpoint.isDefined &&
        !(queryConfig.grpcPartitionsDenyList.contains("*") ||
          queryConfig.grpcPartitionsDenyList.contains(partitionName.toLowerCase))) {
        val channel = channels.getOrElseUpdate(grpcEndpoint.get,
          GrpcCommonUtils.buildChannelFromEndpoint(grpcEndpoint.get))
        PromQLGrpcRemoteExec(channel, remoteHttpTimeoutMs, ctx, inProcessPlanDispatcher,
          dataset.ref, plannerSelector)
      } else {
        val httpEndpoint = partition.httpEndPoint + queryParams.remoteQueryPath.getOrElse("")
        PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
          ctx, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
      }
    }
  }
  /**
   * If the argument partition is local, materialize the LogicalPlan with the local planner.
   *   Otherwise, create a PromQlRemoteExec.
   * @param timeRangeOverride: if given, the plan will be materialized to this range. Otherwise, the
   *                           range is computed from the PromQlQueryParams.
   */
  private def materializeForPartition(logicalPlan: LogicalPlan,
                                      partitionName: String,
                                      grpcEndpoint: Option[String],
                                      httpEndPoint: String,
                                      queryContext: QueryContext,
                                      timeRangeOverride: Option[TimeRange]): ExecPlan = {
    val qContextWithOverride = timeRangeOverride.map{ r =>
      val oldParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val newParams = oldParams.copy(startSecs = r.startMs / 1000, endSecs = r.endMs / 1000)
      queryContext.copy(origQueryParams = newParams)
    }.getOrElse(queryContext)
    val queryParams = qContextWithOverride.origQueryParams.asInstanceOf[PromQlQueryParams]
    val timeRange = timeRangeOverride.getOrElse(TimeRange(1000 * queryParams.startSecs, 1000 * queryParams.endSecs))
    if (partitionName.equals(localPartitionName)) {
      // FIXME: the below check is needed because subquery tests fail when their
      //   time-ranges are updated even with the original query params.
      val lpWithUpdatedTime = if (timeRangeOverride.isDefined) {
        copyLogicalPlanWithUpdatedSeconds(logicalPlan, timeRange.startMs / 1000, timeRange.endMs / 1000)
      } else logicalPlan
      localPartitionPlanner.materialize(lpWithUpdatedTime, qContextWithOverride)
    } else {
      val ctx = generateRemoteExecParams(qContextWithOverride, timeRange.startMs, timeRange.endMs, logicalPlan)
      if (grpcEndpoint.isDefined &&
        !(queryConfig.grpcPartitionsDenyList.contains("*") ||
          queryConfig.grpcPartitionsDenyList.contains(partitionName.toLowerCase))) {
        val channel = channels.getOrElseUpdate(grpcEndpoint.get,
          GrpcCommonUtils.buildChannelFromEndpoint(grpcEndpoint.get))
        PromQLGrpcRemoteExec(channel, remoteHttpTimeoutMs, ctx, inProcessPlanDispatcher,
          dataset.ref, plannerSelector)
      } else {
        val httpEndpoint = httpEndPoint + queryParams.remoteQueryPath.getOrElse("")
        PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
          ctx, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient)
      }
    }
  }

  /**
   * Given a sequence of assignments and a time-range to query, returns (assignment, range)
   *   pairs that describe the time-ranges to be queried for each assignment such that:
   *     (a) the returned ranges lie within the argument time-range, and
   *     (b) lookbacks do not cross partition splits (where the "lookback" is defined only by the argument)
   * @param assignments must be sorted and time-disjoint
   * @param queryRange the complete time-range. Does not include the offset.
   * @param lookbackMs a query's maximum lookback. The time to skip immediately after a partition split.
   * @param offsetMs a query's offsets. Offsets the "skipped" ranges (forward in time) after partition splits.
   * @param stepMsOpt occupied iff the returned ranges should describe periodic steps
   *                  (i.e. all range start times (except the first) should be snapped to a step)
   */
  private def getAssignmentQueryRanges(assignments: Seq[PartitionAssignmentTrait], queryRange: TimeRange,
                               lookbackMs: Long = 0L, offsetMs: Seq[Long] = Seq(0L),
                               stepMsOpt: Option[Long] = None): Seq[(PartitionAssignmentTrait, TimeRange)] = {
    // Construct a sequence of Option[TimeRange]; the ith range is None iff the ith partition has no range to query.
    // First partition doesn't need its start snapped to a periodic step, so deal with it separately.
    val filteredAssignments = assignments
      .dropWhile(_.timeRange.endMs < queryRange.startMs)
      .takeWhile(_.timeRange.startMs <= queryRange.endMs)
    if (filteredAssignments.isEmpty) {
      return Nil
    }
    val headRange = {
      val partRange = assignments.head.timeRange
      if (queryRange.startMs <= partRange.endMs) {
        // At least, some part of the query ends up in this first partition
        Some(TimeRange(math.max(queryRange.startMs, partRange.startMs),
                       math.min(partRange.endMs + offsetMs.min, queryRange.endMs)))
      } else
        None
    }
    // Snap remaining range starts to a step (if a step is provided).
    // Add the constant period of uncertainity to the start of the Assignment time
    // TODO: Move to config later
    val periodOfUncertaintyMs = if (queryConfig.routingConfig.supportRemoteRawExport)
      queryConfig.routingConfig.periodOfUncertaintyMs
    else
      0
    val tailRanges = filteredAssignments.tail.map { assign =>
      val startMs = if (stepMsOpt.nonEmpty) {
        snapToStep(timestamp = assign.timeRange.startMs + lookbackMs + offsetMs.max + periodOfUncertaintyMs,
                   step = stepMsOpt.get,
                   origin = queryRange.startMs)
      } else {
        assign.timeRange.startMs + lookbackMs + offsetMs.max
      }
      val endMs = math.min(queryRange.endMs, assign.timeRange.endMs + offsetMs.max)

      if (startMs <= endMs) {
        Some(TimeRange(startMs, endMs))
      } else
        None
    }
    // Filter out the Nones and flatten the Somes.
    (Seq(headRange) ++ tailRanges).zip(filteredAssignments).filter(_._1.nonEmpty).map{ case (rangeOpt, part) =>
      (part, rangeOpt.get)
    }
  }

  /**
   * Retuns an occupied Option iff the plan can be pushed-down according to the set of labels.
   */
  private def getTschemaLabelsIfCanPushdown(lp: LogicalPlan, qContext: QueryContext): Option[Seq[String]] = {
    val nonMetricShardKeyCols = dataset.options.nonMetricShardColumns
    val canTschemaPushdown = getPushdownKeys(lp, targetSchemaProvider(qContext), nonMetricShardKeyCols,
      rs => LogicalPlanUtils.resolvePipeConcatenatedShardKeyFilters(rs, nonMetricShardKeyCols).map(_.toSet).toSet,
      rs => LogicalPlanUtils.resolvePipeConcatenatedShardKeyFilters(rs, nonMetricShardKeyCols)).isDefined
    if (canTschemaPushdown) {
      LogicalPlanUtils.sameRawSeriesTargetSchemaColumns(lp, targetSchemaProvider(qContext),
        rs => LogicalPlan.getNonMetricShardKeyFilters(rs, dataset.options.nonMetricShardColumns))
    } else None
  }

  /**
   * check if the plan has join or aggregation clause.
   * This is a helper function to decide if a plan should be pushed down.
   * For binary join and aggregation they cannot be pushed down when they have on or by clauses.
   * sum(foo) by(label) + sum(bar) by(label) cannot be pushed down
   * because label can be distributed in different partitions.
   *
   * @param logicalPlan the logic plan.
   * @return true if the binary join or aggregation has clauses.
   */
  private def hasJoinOrAggClause(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case binaryJoin: BinaryJoin => binaryJoin.on.nonEmpty || binaryJoin.ignoring.nonEmpty
      case aggregate: Aggregate => hasJoinOrAggClause(aggregate.vectors)
      // AbsentOverTime is a special case that is converted to aggregation.
      case psw: PeriodicSeriesWithWindowing if psw.function == AbsentOverTime => true
      case nonLeaf: NonLeafLogicalPlan  => nonLeaf.children.exists(hasJoinOrAggClause)
      case _ => false
    }
  }

  // FIXME: this is a near-exact copy-paste of a method in the ShardKeyRegexPlanner --
  //  more evidence that these two classes should be merged.
  private def materializeAggregate(aggregate: Aggregate, queryContext: QueryContext,
                                   hasMultiPartitionNamespace: Boolean): PlanResult = {
    val tschemaLabels = getTschemaLabelsIfCanPushdown(aggregate.vectors, queryContext)
    // TODO have a more accurate pushdown after location rule is define.
    // Right now do not push down any multi-partition namespace plans when on clause exists.
    val canPushdown = !(hasMultiPartitionNamespace && hasJoinOrAggClause(aggregate)) &&
      canPushdownAggregate(aggregate, tschemaLabels, queryContext)
    val plan = if (!canPushdown) {
      val childPlanRes = walkLogicalPlanTree(aggregate.vectors, queryContext.copy(
        plannerParams = queryContext.plannerParams.copy(skipAggregatePresent = false)))
      addAggregator(aggregate, queryContext, childPlanRes)
    } else {
      val queryParams = queryContext.origQueryParams.asInstanceOf[PromQlQueryParams]
      val (assignments, _, _, _) = resolveAssignmentsAndRoutingKeys(aggregate, queryParams)
      val execPlans = assignments.map(
        assignment => materializeForAggregateAssignment(aggregate, assignment, queryContext)
      )
      val exec = if (execPlans.size == 1) execPlans.head
      else {
        if ((aggregate.operator.equals(AggregationOperator.TopK)
          || aggregate.operator.equals(AggregationOperator.BottomK)
          || aggregate.operator.equals(AggregationOperator.CountValues)
          ) && !canSupportMultiPartitionCalls(execPlans))
          throw new UnsupportedOperationException(s"Shard Key regex not supported for ${aggregate.operator}")
        else {
          val reducer = MultiPartitionReduceAggregateExec(queryContext, inProcessPlanDispatcher,
            execPlans.sortWith((x, _) => !x.isInstanceOf[PromQlRemoteExec]), aggregate.operator, aggregate.params)
          if (!queryContext.plannerParams.skipAggregatePresent) {
            reducer.addRangeVectorTransformer(AggregatePresenter(aggregate.operator, aggregate.params,
              RangeParams(queryParams.startSecs, queryParams.stepSecs, queryParams.endSecs)))
          }
          reducer
        }
      }
      exec
    }
    PlanResult(Seq(plan))
  }

  /**
   * Materialize any plan whose materialization strategy is governed by whether-or-not it
   *   contains leaves that individually span partitions.
   */
  private def materializePlanHandleSplitLeaf(logicalPlan: LogicalPlan,
                                             qContext: QueryContext): PlanResult = {
    val qParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    val assignments = LogicalPlan.findLeafLogicalPlans(logicalPlan)
      .filter(_.isInstanceOf[RawSeries])
      .map(getPartitions(_, qParams))
   val hasMultiAssignmentLeaves = assignments.exists(_.size > 1)

    if (hasMultiAssignmentLeaves) {
      materializeSplitLeafPlan(logicalPlan, qContext)
    } else { logicalPlan match {
      case agg: Aggregate => materializeAggregate(agg, qContext,
        assignments.flatMap(_.map(_.proportionMap)).exists(_.size > 1))
      case psw: PeriodicSeriesWithWindowing => materializePeriodicAndRawSeries(psw, qContext)
      case sqw: SubqueryWithWindowing => super.materializeSubqueryWithWindowing(qContext, sqw)
      case bj: BinaryJoin => materializeMultiPartitionBinaryJoinNoSplitLeaf(bj, qContext)
      case sv: ScalarVectorBinaryOperation => super.materializeScalarVectorBinOp(qContext, sv)
      case aif: ApplyInstantFunction => super.materializeApplyInstantFunction(qContext, aif)
      case svdp: ScalarVaryingDoublePlan => super.materializeScalarPlan(qContext, svdp)
      case aaf: ApplyAbsentFunction => super.materializeAbsentFunction(qContext, aaf)
      case ps: PeriodicSeries => materializePeriodicAndRawSeries(ps, qContext)
      case rcm: RawChunkMeta => materializePeriodicAndRawSeries(rcm, qContext)
      case rs: RawSeries => materializePeriodicAndRawSeries(rs, qContext)
      case x => throw new IllegalArgumentException(s"unhandled type: ${x.getClass}")
    }}
  }

  /**
   * Materializes a LogicalPlan with leaves that individually span multiple partitions.
   * All "split-leaf" plans will fail to materialize (throw a BadQueryException) if they
   *   span more than one non-metric shard key prefix.
   */
  //scalastyle:off method.length
  private def materializeSplitLeafPlan(logicalPlan: LogicalPlan,
                                       qContext: QueryContext): PlanResult = {
    val qParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
    // get a mapping of assignments to time-ranges to query
    val lookbackMs = getLookBackMillis(logicalPlan).max
    val offsetMs = getOffsetMillis(logicalPlan)
    val timeRange = TimeRange(1000 * qParams.startSecs, 1000 * qParams.endSecs)
    val stepMsOpt = if (qParams.startSecs == qParams.endSecs) None else Some(1000 * qParams.stepSecs)
    val partitions = getPartitions(logicalPlan, qParams).distinct.sortBy(_.timeRange.startMs)
    require(partitions.nonEmpty, s"Partition assignments is not expected to be empty for query ${qParams.promQl}")
    val assignmentRanges = getAssignmentQueryRanges(partitions, timeRange,
        lookbackMs = lookbackMs, offsetMs = offsetMs, stepMsOpt = stepMsOpt)
    val execPlans = if (assignmentRanges.isEmpty) {
      // Assignment ranges empty means we cant run this query fully on one partition and needs
      // remote raw export Check if the total time of raw export is within the limits, if not return Empty result
      // While it may seem we don't tune the lookback of the leaf raw queries to exactly what we need from each
      // partition, in reality it doesnt matter as despite a longer lookback, the actual data exported will be at most
      // what that partition contains.
      val (startTime, endTime) = (qParams.startSecs, qParams.endSecs)
      val totalExpectedRawExport = (endTime - startTime) + lookbackMs + offsetMs.max
      if (queryConfig.routingConfig.supportRemoteRawExport &&
        queryConfig.routingConfig.maxRemoteRawExportTimeRange.toMillis > totalExpectedRawExport) {
        val newLp = rewritePlanWithRemoteRawExport(logicalPlan, IntervalSelector(startTime * 1000, endTime * 1000))
        walkLogicalPlanTree(newLp, qContext, forceInProcess = true).plans
      } else {
        if (queryConfig.routingConfig.supportRemoteRawExport) {
          logger.warn(
            s"Remote raw export is supported and the $totalExpectedRawExport ms" +
              s" is greater than the max allowed raw export duration of " +
              s"${queryConfig.routingConfig.maxRemoteRawExportTimeRange}" +
              s" for promQl=${qParams.promQl}")
        } else {
          logger.warn(s"Remote raw export not enabled for promQl=${qParams.promQl}")
        }
        Seq(EmptyResultExec(qContext, dataset.ref, inProcessPlanDispatcher))
      }
    } else {
      // materialize a plan for each range/assignment pair
      val (_, execPlans) = assignmentRanges.foldLeft(
        (None: Option[(PartitionAssignmentTrait, TimeRange)], ListBuffer.empty[ExecPlan])) {
        case (acc, next) => acc match {
          case (Some((_, prevTimeRange)), ep: ListBuffer[ExecPlan]) =>
            val (currentAssignment, currentTimeRange) = next
            // Start and end is the next and previous second of the previous and current time range respectively
            val (gapStartTimeMs, gapEndTimeMs) = (
              if (stepMsOpt.isDefined)
                snapToStep(prevTimeRange.endMs / 1000L * 1000L + 1, stepMsOpt.get, timeRange.startMs)
              else
                prevTimeRange.endMs / 1000L * 1000L + 1000L,
              (currentTimeRange.startMs / 1000L * 1000L) - 1000L)


            // If we enable stitching the missing part of time range between the previous time range's end time and
            // current time range's start time, we will perform remote/local partition raw data export
            if (queryConfig.routingConfig.supportRemoteRawExport && gapStartTimeMs < gapEndTimeMs) {
              //  We need to perform raw data export from two partitions, for simplicity we will assume the time range
              //  spans 2 partition, one partition is on the left and one on the right
              // Walk the plan to make all RawSeries support remote export fetching the data from previous partition
              // When we rewrite the RawSeries's rangeSelector, we will make the start and end time same as the end
              // of the previous partition's end time and then do a raw query for the duration of the
              //  (currentTimeRange.startMs - currentAssignment.timeRange.startMs) + offset + lookback.
              //
              //           Partition split   end time for queries in partition 1
              //                       V(p)  V(t1)
              //  |----o---------------|-----x------x-----------------------------o-------|
              //       ^(s)                         ^(t2)                         ^(e)
              //    Query start time     Start time in new partition          Query end time
              //
              // Given we have two offsets [10 mins, 5 mins],
              // the query range from partition P1 (left of the partition split point) is [s, p + 5m].
              // The offset looks at data 5 mins back and 10 mins back, so we can extend the time range in p1 to 5
              // mins after the split point p. We want to now provide results for time range t1 - t2, which is missing
              //
              // Lets assume the query is sum(rate(foo{}[5m] offset 5m) + sum(rate(foo{}[10m] offset [10m])
              // Given the offset is [5m, 10m], lookback is [5m, 10m], we would need raw data in the range
              // [t1 - 10m - 10m, t2], this range for raw queries span two partitions and we will let the RawSeries (the
              // leaf logical plan) with supportsRemoteDataCall = true figure out if this range can entirely be selected
              // from partition p1 or p2
              //

              // Do not perform raw exports if the export is beyond a certain value for example
              // foo{}[10d] or foo[2d] offset 8d  both will export 10 days of raw data which might cause heap pressure
              // and OOMs. The max cross partition raw export config can control such queries from bring the process
              // down but simpler queries with few minutes or even hour or two of lookback/offset will continue to work
              // seamlessly with no data gaps
              // Note that at the moment, while planning, we only can look at whats the max time range we can support.
              // We still dont have capabilities to check the expected number of timeseries scanned or bytes scanned
              // and adding capabilities to give up a "part" of query execution if the runtime number of bytes of ts
              // scanned goes high isn't available. To start with the time range scanned as a static configuration will
              // be good enough and can be enhanced in future as required.
              val rawExportStartDurationThisPartition = prevTimeRange.endMs / 1000L * 1000L
              val totalExpectedRawExport = ((gapEndTimeMs - rawExportStartDurationThisPartition)
                + lookbackMs + offsetMs.max)
              if (queryConfig.routingConfig.maxRemoteRawExportTimeRange.toMillis > totalExpectedRawExport) {
                // Only if the raw export is completely within the previous partition's timerange
                val newParams = qParams.copy(
                  startSecs = gapStartTimeMs / 1000,
                  endSecs = gapEndTimeMs / 1000)
                val newContext = qContext.copy(origQueryParams = newParams)
                val newLp = {
                  val rawExportPlan = rewritePlanWithRemoteRawExport(logicalPlan,
                    IntervalSelector(gapStartTimeMs, gapEndTimeMs),
                    additionalLookbackMs = 0L.max(gapStartTimeMs - rawExportStartDurationThisPartition))
                  // FIXME: This extra copy is used to second-align the plan start/end
                  //   timestamps (in milliseconds). This is required because some plans (correctly)
                  //   validate that their children have similar timestamps, and some plans (namely
                  //   scalars) do not support millisecond precision. rewritePlanWithRemoteRawExport should
                  //   be updated to second-align timestamps, then this extra copy can be removed.
                  copyLogicalPlanWithUpdatedSeconds(rawExportPlan, newParams.startSecs, newParams.endSecs)
                }
                ep ++= walkLogicalPlanTree(newLp, newContext, forceInProcess = true).plans
              } else {
                logger.warn(
                  s"Remote raw export is supported but the expected raw export for $totalExpectedRawExport ms" +
                  s" is greater than the max allowed raw export duration " +
                    s"${queryConfig.routingConfig.maxRemoteRawExportTimeRange}")
              }
            }
            val timeRangeOverride = TimeRange(
              1000 * qParams.startSecs.max(currentTimeRange.startMs / 1000),
              1000 * qParams.endSecs.min(currentTimeRange.endMs / 1000)
            )
            ep += materializeForAssignment(logicalPlan, currentAssignment, qContext, Some(timeRangeOverride))
            (Some(next), ep)
          //
          case (None, ep: ListBuffer[ExecPlan]) =>
            val (assignment, range) = next
            val timeRangeOverride = TimeRange(
              1000 * qParams.startSecs,
              range.endMs
            )
            ep += materializeForAssignment(logicalPlan, assignment, qContext, Some(timeRangeOverride))
            (Some(next), ep)
        }
      }
      // Edge case with namespace across multiple partitions includes a case where the a part of time range
      // of the original params fall in the new partition but that duration is less than the lookback. For example,
      // Consider the timesplit happening at 1:00, the lookback of the query is 20 mins and the original query params
      // have end date of 1:10. Then the period of 1 - 1:20 will not have any results, this is due to that fact the
      // period 1 - 1:10 can not be served from one partition alone and needs to be computed on query service. Here
      // we will handle this case and add the missing execPlan if needed

      if (partitions.length > 1 && queryConfig.routingConfig.supportRemoteRawExport) {
        // Here we check if the assignment ranges cover the entire query duration
        val (_, lastTimeRange) = assignmentRanges.last
        if (lastTimeRange.endMs < timeRange.endMs) {
          // this means we need to add the missing time range to the end to execute the bit on Query service
          val (gapStartTimeMs, gapEndTimeMs) = stepMsOpt match {
            case Some(step)   =>   (snapToStep(lastTimeRange.endMs + 1, step, timeRange.startMs),
              timeRange.endMs)
            case None           => (lastTimeRange.endMs, timeRange.endMs)
          }
          if (gapStartTimeMs <= gapEndTimeMs){
            // The opposite happens when we snap a large step to the query start and the result/gapStartTimeMs is
            // larger than the query end time/gapEndTimeMs. That means there is no gap so we skip this block of code
            // for handling gap range
            val newParams = qParams.copy(startSecs = gapStartTimeMs / 1000, endSecs = gapEndTimeMs / 1000)
            val newContext = qContext.copy(origQueryParams = newParams)
            val newLp = rewritePlanWithRemoteRawExport(logicalPlan,
              IntervalSelector(gapStartTimeMs, gapEndTimeMs),
              additionalLookbackMs = 0L.max(gapStartTimeMs - lastTimeRange.startMs))
            execPlans ++ walkLogicalPlanTree(newLp, newContext, forceInProcess = true).plans
          } else {
            execPlans
          }
        } else {
          execPlans
        }
      } else {
        execPlans
      }
    }

    // stitch if necessary
    val resPlan = if (execPlans.size == 1) {
      execPlans.head
    } else {
      // returns NaNs for missing timestamps
      val rvRange = RvRange(1000 * qParams.startSecs,
                            1000 * qParams.stepSecs,
                            1000 * qParams.endSecs)
      StitchRvsExec(qContext, inProcessPlanDispatcher, Some(rvRange), execPlans,
        enableApproximatelyEqualCheck = queryConfig.routingConfig.enableApproximatelyEqualCheckInStitch)
    }
    PlanResult(Seq(resPlan))
  }
  //scalastyle:on method.length
  /**
   * Materialize a BinaryJoin whose individual leaf plans do not span partitions.
   */
  private def materializeMultiPartitionBinaryJoinNoSplitLeaf(logicalPlan: BinaryJoin,
                                                             qContext: QueryContext): PlanResult = {
    val lhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.lhs)))
    val rhsQueryContext = qContext.copy(origQueryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams].
      copy(promQl = LogicalPlanParser.convertToQuery(logicalPlan.rhs)))

    val lhsPlanRes = walkLogicalPlanTree(logicalPlan.lhs, lhsQueryContext)
    val rhsPlanRes = walkLogicalPlanTree(logicalPlan.rhs, rhsQueryContext)

    val execPlan = if (logicalPlan.operator.isInstanceOf[SetOperator])
      SetOperatorExec(qContext, InProcessPlanDispatcher(queryConfig), lhsPlanRes.plans, rhsPlanRes.plans,
        logicalPlan.operator, logicalPlan.on.map(LogicalPlanUtils.renameLabels(_, datasetMetricColumn)),
        LogicalPlanUtils.renameLabels(logicalPlan.ignoring, datasetMetricColumn), datasetMetricColumn,
        rvRangeFromPlan(logicalPlan))
    else
      BinaryJoinExec(qContext, inProcessPlanDispatcher, lhsPlanRes.plans, rhsPlanRes.plans, logicalPlan.operator,
        logicalPlan.cardinality, logicalPlan.on.map(LogicalPlanUtils.renameLabels(_, datasetMetricColumn)),
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

  private def materializeMetadataQueryPlan(lp: MetadataQueryPlan, qContext: QueryContext): PlanResult = {
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
      val execPlans = partitions.flatMap(ps => ps.proportionMap.values.map(pd =>
        PartitionAssignment(pd.partitionName, pd.httpEndPoint, ps.timeRange, pd.grpcEndPoint))).map { p =>
        logger.debug(s"partitionInfo=$p; queryParams=$queryParams")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(
            copy(lp, startMs = p.timeRange.startMs, endMs = p.timeRange.endMs), qContext)
        else {
          val params: Map[String, String] = lp match {
            case _: SeriesKeysByFilters |
                 _: LabelNames |
                 _: LabelCardinality    => Map("match[]" -> LogicalPlanParser.metadataMatchToQuery(lp))
            case lv: LabelValues        => PlannerUtil.getLabelValuesUrlParams(lv, queryParams)
          }
          createMetadataRemoteExec(qContext, p, params, lp)
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
      val execPlans = partitions.flatMap(ps => ps.proportionMap.values.map(pd =>
        PartitionAssignment(pd.partitionName, pd.httpEndPoint, ps.timeRange, pd.grpcEndPoint))).map { p =>
        logger.debug(s"partition=$p; plan=$lp")
        if (p.partitionName.equals(localPartitionName))
          localPartitionPlanner.materialize(lp, qContext)
        else {
          val newQueryContext = qContext.copy(origQueryParams = queryParams.copy(verbose = true))
          createMetadataRemoteExec(newQueryContext, p, lp.queryParams(), lp)
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

  private def getMetadataPartitions(filters: Seq[ColumnFilter],
                                    timeRange: TimeRange): List[PartitionAssignmentTrait] = {
    val nonMetricShardKeyFilters = filters.filter(f => dataset.options.nonMetricShardColumns.contains(f.column))
    partitionLocationProvider.getMetadataPartitionsTrait(nonMetricShardKeyFilters, timeRange)
  }

  private def createMetadataRemoteExec(qContext: QueryContext, partitionAssignment: PartitionAssignment,
                                       urlParams: Map[String, String], logicalPlan: LogicalPlan) = {
    val finalQueryContext = generateRemoteExecParams(
      qContext, partitionAssignment.timeRange.startMs, partitionAssignment.timeRange.endMs, logicalPlan)
    val httpEndpoint = partitionAssignment.httpEndPoint +
      finalQueryContext.origQueryParams.asInstanceOf[PromQlQueryParams].remoteQueryPath.getOrElse("")
    MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
      urlParams, finalQueryContext, inProcessPlanDispatcher, dataset.ref, remoteExecHttpClient, queryConfig)
  }
}

//scalastyle:on file.size.limit

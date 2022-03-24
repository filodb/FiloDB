package filodb.coordinator.queryplanner

import scala.concurrent.duration._
import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.coordinator.queryplanner.optimize.{BinaryJoinPushdownOpt, CommonDispatcherOpt}
import filodb.core.{SpreadProvider, StaticTargetSchemaProvider, TargetSchemaChange, TargetSchemaProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query._
import filodb.core.query.Filter.Equals
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.prometheus.ast.WindowConstants
import filodb.query.{exec, _}
import filodb.query.InstantFunctionId.HistogramBucket
import filodb.query.LogicalPlan._
import filodb.query.exec.{LocalPartitionDistConcatExec, _}
import filodb.query.exec.InternalRangeFunction.Last

object SingleClusterPlanner extends StrictLogging {
  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests").withoutTags

  // Is TargetSchema changing during query window.
  private def isTargetSchemaChanging(targetSchemaChanges: Seq[TargetSchemaChange],
                                     startMs: Long, endMs: Long): Boolean =
    targetSchemaChanges.nonEmpty && targetSchemaChanges.exists(c => c.time >= startMs && c.time <= endMs)

  // Find the TargetSchema that is applicable i.e effective for the current query window
  def findTargetSchema(targetSchemaChanges: Seq[TargetSchemaChange],
                               startMs: Long, endMs: Long): Option[TargetSchemaChange] = {
    val tsIndex = targetSchemaChanges.lastIndexWhere(t => t.time <= startMs)
    if(tsIndex > -1)
      Some(targetSchemaChanges(tsIndex))
    else
      None
  }

  /**
   * If TargetSchema exists and all of the target-schema label filters (equals) are provided in the query,
   * then return true.
   * @param filters Query Column Filters
   * @param targetSchema TargetSchema
   * @return useTargetSchema - use target-schema to calculate query shards
   */
  private def useTargetSchemaForShards(filters: Seq[ColumnFilter], targetSchema: Option[TargetSchemaChange]): Boolean =
    targetSchema match {
      case Some(ts) if ts.schema.nonEmpty => ts.schema
        .forall(s => filters.exists(cf => cf.column == s && cf.filter.isInstanceOf[Filter.Equals]))
      case _ => false
    }

//  /**
//   * Given a BinaryJoin and target schema for one of its children, returns true iff
//   *   each of the child's eventual join keys describe a superset of the target schema columns.
//   */
//  private def joinKeyIsTargetSchemaSuperset(binaryJoin: BinaryJoin,
//                                            targetSchema: Seq[String]): Boolean = {
//    // We cannot know the exact RangeVectorKeys a child will eventually yield,
//    //   but-- since it has a target schema-- we do know that those keys will include
//    //   the target schema columns.
//    if (binaryJoin.on.nonEmpty) {
//      // "on" must include at least the set of target schema columns
//      targetSchema.toSet.diff(binaryJoin.on.toSet).isEmpty
//    } else {
//      // "ignore" can't include any of the target schema columns
//      targetSchema.toSet.intersect(binaryJoin.ignoring.toSet).isEmpty
//    }
//  }
}

/**
  * Responsible for query planning within single FiloDB cluster
  * @param schemas          schema instance, used to extract partKey schema
  * @param spreadProvider  used to get spread
  * @param shardMapperFunc used to get shard locality
  * @param timeSplitEnabled split based on longer time range
  * @param minTimeRangeForSplitMs if time range is longer than this, zplan will be split into multiple plans
  * @param splitSizeMs time range for each split, if plan needed to be split
  */

class SingleClusterPlanner(val dataset: Dataset,
                           val schemas: Schemas,
                           shardMapperFunc: => ShardMapper,
                           earliestRetainedTimestampFn: => Long,
                           val queryConfig: QueryConfig,
                           clusterName: String,
                           spreadProvider: SpreadProvider = StaticSpreadProvider(),
                           targetSchemaProvider: TargetSchemaProvider = StaticTargetSchemaProvider(),
                           timeSplitEnabled: Boolean = false,
                           minTimeRangeForSplitMs: => Long = 1.day.toMillis,
                           splitSizeMs: => Long = 1.day.toMillis)
                           extends QueryPlanner with StrictLogging with DefaultPlanner {
  override val dsOptions: DatasetOptions = schemas.part.options
  private val shardColumns = dsOptions.shardKeyColumns.sorted
  private val dsRef = dataset.ref

  import SingleClusterPlanner._

  private def dispatcherForShard(shard: Int): PlanDispatcher = {
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) {
      logger.debug(s"ShardMapper: $shardMapperFunc")
      throw new RuntimeException(s"Shard: $shard is not available") // TODO fix this
    }
    ActorPlanDispatcher(targetActor, clusterName)
  }

  /**
   * Change start time of logical plan according to earliestRetainedTimestampFn
   */
  private def updateStartTime(logicalPlan: LogicalPlan): Option[LogicalPlan] = {
    val periodicSeriesPlan = LogicalPlanUtils.getPeriodicSeriesPlan(logicalPlan)
    if (periodicSeriesPlan.isEmpty) Some(logicalPlan)
    else {
      //For binary join LHS & RHS should have same times
      val boundParams = periodicSeriesPlan.get.head match {
        case p: PeriodicSeries => (p.startMs, p.stepMs, WindowConstants.staleDataLookbackMillis,
          p.offsetMs.getOrElse(0L), p.endMs)
        case w: PeriodicSeriesWithWindowing => (w.startMs, w.stepMs, w.window, w.offsetMs.getOrElse(0L), w.endMs)
        case _  => throw new UnsupportedOperationException(s"Invalid plan: ${periodicSeriesPlan.get.head}")
      }

      val newStartMs = boundToStartTimeToEarliestRetained(boundParams._1, boundParams._2, boundParams._3,
        boundParams._4)
      if (newStartMs <= boundParams._5) { // if there is an overlap between query and retention ranges
        if (newStartMs != boundParams._1)
          Some(LogicalPlanUtils.copyLogicalPlanWithUpdatedTimeRange(logicalPlan, TimeRange(newStartMs, boundParams._5)))
        else Some(logicalPlan)
      } else { // query is outside retention period, simply return empty result
        None
      }
    }
  }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val plannerParams = qContext.plannerParams
    val updatedPlan = updateStartTime(logicalPlan)
    if (updatedPlan.isEmpty) EmptyResultExec(qContext, dsRef)
    else {
     val logicalPlan = updatedPlan.get
      val timeSplitConfig = if (plannerParams.timeSplitEnabled)
        (plannerParams.timeSplitEnabled, plannerParams.minTimeRangeForSplitMs, plannerParams.splitSizeMs)
      else (timeSplitEnabled, minTimeRangeForSplitMs, splitSizeMs)

      if (shardMapperFunc.numShards <= 0) throw new IllegalStateException("No shards available")
      val logicalPlans = if (updatedPlan.get.isInstanceOf[PeriodicSeriesPlan])
        LogicalPlanUtils.splitPlans(updatedPlan.get, qContext, timeSplitConfig._1,
          timeSplitConfig._2, timeSplitConfig._3)
      else
        Seq(logicalPlan)
      val materialized = logicalPlans match {
        case Seq(one) => materializeTimeSplitPlan(one, qContext)
        case many =>
          val materializedPlans = many.map(materializeTimeSplitPlan(_, qContext))
          val targetActor = PlannerUtil.pickDispatcher(materializedPlans)

          // create SplitLocalPartitionDistConcatExec that will execute child execplanss sequentially and stitches
          // results back with StitchRvsMapper transformer.
          val stitchPlan = SplitLocalPartitionDistConcatExec(qContext, targetActor, materializedPlans,
            rvRangeFromPlan(logicalPlan))
          stitchPlan
      }
      logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
        s" $logicalPlan to \n${materialized.printTree()}")
      var matFinal = materialized
      matFinal = BinaryJoinPushdownOpt.optimize(matFinal)
      matFinal = CommonDispatcherOpt.optimize(matFinal)
      matFinal
    }
  }

  private def materializeTimeSplitPlan(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val materialized = walkLogicalPlanTree(logicalPlan, qContext)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(StitchRvsMapper(rvRangeFromPlan(logicalPlan)))
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = PlannerUtil.pickDispatcher(many)
        many.head match {
          case _: LabelValuesExec => LabelValuesDistConcatExec(qContext, targetActor, many)
          case _: LabelNamesExec => LabelNamesDistConcatExec(qContext, targetActor, many)
          case _: TsCardExec => TsCardReduceExec(qContext, targetActor, many)
          case _: LabelCardinalityExec =>
            val reduceExec = LabelCardinalityReduceExec(qContext, targetActor, many)
            // Presenter here is added separately which use the bytes representing the sketch to get an estimate
            // of cardinality. The presenter is kept separate from DistConcatExec to enable multi partition queries
            // later if needed. The DistConcatExec's from multiple partitions can still return the map of label names
            // and bytes and they can be merged to create a new sketch. Only the top level exec needs to then add the
            // presenter to display the final mapping of label name and the count based on the sketch bytes.
            if (!qContext.plannerParams.skipAggregatePresent) {
              reduceExec.addRangeVectorTransformer(new LabelCardinalityPresenter())
            }
            reduceExec
          case _: PartKeysExec => PartKeysDistConcatExec(qContext, targetActor, many)
          case _: ExecPlan =>
            val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(StitchRvsMapper(rvRangeFromPlan(logicalPlan)))
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized
  }

  def shardsFromFilters(filters: Seq[ColumnFilter],
                        qContext: QueryContext,
                        useTargetSchemaForShards: Boolean = false): Seq[Int] = {

    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)

    require(shardColumns.nonEmpty || qContext.plannerParams.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    qContext.plannerParams.shardOverrides.getOrElse {
      val shardVals = shardColumns.map { shardCol =>
        // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
        filters.find(f => f.column == shardCol) match {
          case Some(ColumnFilter(_, Filter.Equals(filtVal: String))) =>
            shardCol -> RecordBuilder.trimShardColumn(dsOptions, shardCol, filtVal)
          case Some(ColumnFilter(_, filter)) =>
            throw new BadQueryException(s"Found filter for shard column $shardCol but " +
              s"$filter cannot be used for shard key routing")
          case _ =>
            throw new BadQueryException(s"Could not find filter for shard key column " +
              s"$shardCol, shard key hashing disabled")
        }
      }
      val metric = shardVals.find(_._1 == dsOptions.metricColumn)
        .map(_._2)
        .getOrElse(throw new BadQueryException(s"Could not find metric value"))
      val shardValues = shardVals.filterNot(_._1 == dsOptions.metricColumn).map(_._2)
      logger.debug(s"For shardColumns $shardColumns, extracted metric $metric and shard values $shardValues")
      val targetSchemaChange = qContext.plannerParams.targetSchemaProvider
        .getOrElse(targetSchemaProvider)
        .targetSchemaFunc(filters)
      val targetSchema = if (targetSchemaChange.nonEmpty) targetSchemaChange.last.schema else Seq.empty
      val shardHash = RecordBuilder.shardKeyHash(shardValues, dsOptions.metricColumn, metric, targetSchema)
      if(useTargetSchemaForShards) {
        val nonShardKeyLabelPairs = filters.filter(f => !shardColumns.contains(f.column)
                                                          && f.filter.isInstanceOf[Filter.Equals])
                                            .map(cf => cf.column ->
                                              cf.filter.asInstanceOf[Filter.Equals].value.toString).toMap
        val partitionHash = RecordBuilder.partitionKeyHash(nonShardKeyLabelPairs, shardVals.toMap, targetSchema,
          dsOptions.metricColumn, metric)
        // since target-schema filter is provided in the query, ingestionShard can be used to find the single shard
        // that can answer the query.
        Seq(shardMapperFunc.ingestionShard(shardHash, partitionHash, spreadProvToUse.spreadFunc(filters).last.spread))
      } else {
        shardMapperFunc.queryShards(shardHash, spreadProvToUse.spreadFunc(filters).last.spread)
      }
    }
  }

  private def toChunkScanMethod(rangeSelector: RangeSelector): ChunkScanMethod = {
    rangeSelector match {
      case IntervalSelector(from, to) => TimeRangeChunkScan(from, to)
      case AllChunksSelector          => AllChunkScan
      case WriteBufferSelector        => WriteBufferChunkScan
      case InMemoryChunksSelector     => InMemoryChunkScan
      case x @ _                      => throw new IllegalArgumentException(s"Unsupported range selector '$x' found")
    }
  }

  /**
    * Renames Prom AST __name__ metric name filters to one based on the actual metric column of the dataset,
    * if it is not the prometheus standard
    */
  def renameMetricFilter(filters: Seq[ColumnFilter]): Seq[ColumnFilter] =
    if (dsOptions.metricColumn != PromMetricLabel) {
      filters map {
        case ColumnFilter(PromMetricLabel, filt) => ColumnFilter(dsOptions.metricColumn, filt)
        case other: ColumnFilter                 => other
      }
    } else {
      filters
    }

  /**
    * Walk logical plan tree depth-first and generate execution plans starting from the bottom
    *
    * @return ExecPlans that answer the logical plan provided
    */
  // scalastyle:off cyclomatic.complexity
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                  qContext: QueryContext): PlanResult = {
     logicalPlan match {
      case lp: RawSeries                   => materializeRawSeries(qContext, lp)
      case lp: RawChunkMeta                => materializeRawChunkMeta(qContext, lp)
      case lp: PeriodicSeries              => materializePeriodicSeries(qContext, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(qContext, lp)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(qContext, lp)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(qContext, lp)
      case lp: Aggregate                   => materializeAggregate(qContext, lp)
      case lp: BinaryJoin                  => materializeBinaryJoin(qContext, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(qContext, lp)
      case lp: LabelValues                 => materializeLabelValues(qContext, lp)
      case lp: LabelNames                  => materializeLabelNames(qContext, lp)
      case lp: TsCardinalities             => materializeTsCardinalities(qContext, lp)
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(qContext, lp)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp)
      case lp: ApplyLimitFunction          => materializeLimitFunction(qContext, lp)
      case lp: ScalarBinaryOperation       => materializeScalarBinaryOperation(qContext, lp)
      case lp: SubqueryWithWindowing       => materializeSubqueryWithWindowing(qContext, lp)
      case lp: TopLevelSubquery            => materializeTopLevelSubquery(qContext, lp)
      case lp: LabelCardinality            => materializeLabelCardinality(qContext, lp)
      //case _                               => throw new BadQueryException("Invalid logical plan")
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def materializeBinaryJoin(qContext: QueryContext,
                                     lp: BinaryJoin): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, qContext)
    val rhs = walkLogicalPlanTree(lp.rhs, qContext)

    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(qContext,
      PlannerUtil.pickDispatcher(lhs.plans), rvRangeFromPlan(lp), lhs.plans))
    else lhs.plans
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(qContext,
      PlannerUtil.pickDispatcher(rhs.plans), rvRangeFromPlan(lp), rhs.plans))
    else rhs.plans

    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = PlannerUtil.pickDispatcher(stitchedLhs ++ stitchedRhs)
    val execPlan = if (lp.operator.isInstanceOf[SetOperator])
      exec.SetOperatorExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn,
        rvRangeFromPlan(lp))
    else
      BinaryJoinExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        LogicalPlanUtils.renameLabels(lp.on, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.include, dsOptions.metricColumn), dsOptions.metricColumn, rvRangeFromPlan(lp))
    PlanResult(Seq(execPlan))
  }

  private def materializePeriodicSeriesWithWindowing(qContext: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing): PlanResult = {
    val logicalPlanWithoutBucket = if (queryConfig.translatePromToFilodbHistogram) {
       removeBucket(Right(lp))._3.right.get
    } else lp

    val series = walkLogicalPlanTree(logicalPlanWithoutBucket.series, qContext)
    val rawSource = logicalPlanWithoutBucket.series.isRaw

    /* Last function is used to get the latest value in the window for absent_over_time
    If no data is present AbsentFunctionMapper will return range vector with value 1 */

    val execRangeFn = if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) Last
                      else InternalRangeFunction.lpToInternalFunc(logicalPlanWithoutBucket.function)

    val paramsExec = materializeFunctionArgs(logicalPlanWithoutBucket.functionArgs, qContext)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(logicalPlanWithoutBucket.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(logicalPlanWithoutBucket.startMs,
      logicalPlanWithoutBucket.stepMs, logicalPlanWithoutBucket.endMs, window, Some(execRangeFn), qContext,
      logicalPlanWithoutBucket.stepMultipleNotationUsed,
      paramsExec, logicalPlanWithoutBucket.offsetMs, rawSource)))
    if (logicalPlanWithoutBucket.function == RangeFunctionId.AbsentOverTime) {
      val aggregate = Aggregate(AggregationOperator.Sum, logicalPlanWithoutBucket, Nil, Seq("job"))
      // Add sum to aggregate all child responses
      // If all children have NaN value, sum will yield NaN and AbsentFunctionMapper will yield 1
      val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext.copy(plannerParams =
        qContext.plannerParams.copy(skipAggregatePresent = true)), series)))
      addAbsentFunctionMapper(aggregatePlanResult, logicalPlanWithoutBucket.columnFilters,
        RangeParams(logicalPlanWithoutBucket.startMs / 1000, logicalPlanWithoutBucket.stepMs / 1000,
          logicalPlanWithoutBucket.endMs / 1000), qContext)
    } else series
  }

  private def removeBucket(lp: Either[PeriodicSeries, PeriodicSeriesWithWindowing]) = {
    val rawSeries = lp match {
      case Right(value) => value.series
      case Left(value)  => value.rawSeries
    }

    rawSeries match {
      case rawSeriesLp: RawSeries =>

        val nameFilter = rawSeriesLp.filters.find(_.column.equals(PromMetricLabel)).
          map(_.filter.valuesStrings.head.toString)
        val leFilter = rawSeriesLp.filters.find(_.column == "le").map(_.filter.valuesStrings.head.toString)

        if (nameFilter.isEmpty) (nameFilter, leFilter, lp)
        else {
          val filtersWithoutBucket = rawSeriesLp.filters.filterNot(_.column.equals(PromMetricLabel)).
            filterNot(_.column == "le") :+ ColumnFilter(PromMetricLabel,
            Equals(nameFilter.get.replace("_bucket", "")))
          val newLp =
            if (lp.isLeft)
             Left(lp.left.get.copy(rawSeries = rawSeriesLp.copy(filters = filtersWithoutBucket)))
            else
             Right(lp.right.get.copy(series = rawSeriesLp.copy(filters = filtersWithoutBucket)))
          (nameFilter, leFilter, newLp)
        }
      case _ => (None, None, lp)
    }
  }
  private def materializePeriodicSeries(qContext: QueryContext,
                                        lp: PeriodicSeries): PlanResult = {

   // Convert to FiloDB histogram by removing le label and bucket prefix
   // _sum and _count are removed in MultiSchemaPartitionsExec since we need to check whether there is a metric name
   // with _sum/_count as suffix
    val (nameFilter: Option[String], leFilter: Option[String], lpWithoutBucket: PeriodicSeries) =
    if (queryConfig.translatePromToFilodbHistogram) {
     val result = removeBucket(Left(lp))
      (result._1, result._2, result._3.left.get)

    } else (None, None, lp)

    val rawSeries = walkLogicalPlanTree(lpWithoutBucket.rawSeries, qContext)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs, lp.endMs,
      None, None, qContext, stepMultipleNotationUsed = false, Nil, lp.offsetMs)))

    if (nameFilter.isDefined && nameFilter.head.endsWith("_bucket") && leFilter.isDefined) {
      val paramsExec = StaticFuncArgs(leFilter.head.toDouble, RangeParams(lp.startMs/1000, lp.stepMs/1000,
        lp.endMs/1000))
      rawSeries.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(HistogramBucket,
        Seq(paramsExec))))
    }
   rawSeries
  }

  /**
    * Calculates the earliest startTime possible given the query's start/window/step/offset.
    * This is used to bound the startTime of queries so we dont create possibility of aggregating
    * partially expired data and return incomplete results.
    *
    * @return new startTime to be used for query in ms. If original startTime is within retention
    *         period, returns it as is.
    */
  private def boundToStartTimeToEarliestRetained(startMs: Long, stepMs: Long,
                                                 windowMs: Long, offsetMs: Long): Long = {
    // In case query is earlier than earliestRetainedTimestamp then we need to drop the first few instants
    // to prevent inaccurate results being served. Inaccuracy creeps in because data can be in memory for which
    // equivalent data may not be in cassandra. Aggregations cannot be guaranteed to be complete.
    // Also if startMs < 500000000000 (before 1985), it is usually a unit test case with synthetic data, so don't fiddle
    // around with those queries
    val earliestRetainedTimestamp = earliestRetainedTimestampFn
    if (startMs - windowMs - offsetMs < earliestRetainedTimestamp && startMs > 500000000000L) {
      // We calculate below number of steps/instants to drop. We drop instant if data required for that instant
      // doesnt fully fall into the retention period. Data required for that instant involves
      // going backwards from that instant up to windowMs + offsetMs milli-seconds.
      val numStepsBeforeRetention = (earliestRetainedTimestamp - startMs + windowMs + offsetMs) / stepMs
      val lastInstantBeforeRetention = startMs + numStepsBeforeRetention * stepMs
      lastInstantBeforeRetention + stepMs
    } else {
      startMs
    }
  }

  /**
    * If there is a _type_ filter, remove it and populate the schema name string.  This is because while
    * the _type_ filter is a real filter on time series, we don't index it using Lucene at the moment.
    */
  private def extractSchemaFilter(filters: Seq[ColumnFilter]): (Seq[ColumnFilter], Option[String]) = {
    var schemaOpt: Option[String] = None
    val newFilters = filters.filterNot { case ColumnFilter(label, filt) =>
      val isTypeFilt = label == TypeLabel
      if (isTypeFilt) filt match {
        case Filter.Equals(schema) => schemaOpt = Some(schema.asInstanceOf[String])
        case x: Any                 => throw new IllegalArgumentException(s"Illegal filter $x on _type_")
      }
      isTypeFilt
    }
    (newFilters, schemaOpt)
  }

  private def materializeRawSeries(qContext: QueryContext,
                                   lp: RawSeries): PlanResult = {
    val spreadProvToUse = qContext.plannerParams.spreadOverride.getOrElse(spreadProvider)
    val offsetMillis: Long = lp.offsetMs.getOrElse(0)
    val colName = lp.columns.headOption
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val spreadChanges = spreadProvToUse.spreadFunc(renamedFilters)

    val targetSchemaChanges = qContext.plannerParams.targetSchemaProvider
      .getOrElse(targetSchemaProvider)
      .targetSchemaFunc(renamedFilters)
    val rangeSelectorWithOffset = lp.rangeSelector match {
      case IntervalSelector(fromMs, toMs) => IntervalSelector(fromMs - offsetMillis - lp.lookbackMs.getOrElse(
                                             queryConfig.staleSampleAfterMs), toMs - offsetMillis)
      case _                              => lp.rangeSelector
    }
    val needsStitch = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }

    // Change in Target Schema in query window, do not use target schema to find query shards
    val tsChangeExists = (rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => isTargetSchemaChanging(targetSchemaChanges, from, to)
      case _                          => false
    })
    val targetSchemaOpt = (rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => findTargetSchema(targetSchemaChanges, from, to)
      case _                          => None
    })
    val allTSLabelsPresent = useTargetSchemaForShards(renamedFilters, targetSchemaOpt)

    // Whether to use target-schema for calculating the target shards.
    // If there is change in target-schema or target-schema not defined or query doesn't have all the labels of
    // target-schema, use shardKeyHash to find the target shards.
    // If a target-schema is defined and is not changing during the query window and all the target-schema labels are
    // provided in query filters (Equals), then find the target shard to route to using ingestionShard helper method.
    val useTSForQueryShards = !tsChangeExists && allTSLabelsPresent

    val execPlans = shardsFromFilters(renamedFilters, qContext, useTSForQueryShards).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      MultiSchemaPartitionsExec(qContext, dispatcher, dsRef, shard, renamedFilters,
        toChunkScanMethod(rangeSelectorWithOffset), dsOptions.metricColumn, schemaOpt, colName)
    }
    // Stitch only if spread and/or target-schema changes during the query-window.
    // when target-schema changes during query window, data might be ingested in different shards after the change.
    PlanResult(execPlans, needsStitch || tsChangeExists)
  }

  private def materializeLabelValues(qContext: QueryContext,
                                     lp: LabelValues): PlanResult = {
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dsOptions.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dsOptions.metricColumn) else lp.labelNames

    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(qContext, dispatcher, dsRef, shard, renamedFilters, labelNames, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  // allow metadataQueries to get list of shards from shardKeyFilters only if all shardCols have Equals filter
  private def canGetShardsFromFilters(renamedFilters: Seq[ColumnFilter],
                                      qContext: QueryContext): Boolean = {
    if (qContext.plannerParams.shardOverrides.isEmpty && shardColumns.nonEmpty) {
      shardColumns.toSet.subsetOf(renamedFilters.map(_.column).toSet) &&
        shardColumns.forall { shardCol =>
          // So to compute the shard hash we need shardCol == value filter (exact equals) for each shardColumn
          renamedFilters.find(f => f.column == shardCol) match {
            case Some(ColumnFilter(_, Filter.Equals(_: String))) => true
            case _ => false
          }
        }
    } else false
  }

  private def materializeLabelNames(qContext: QueryContext,
                                    lp: LabelNames): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }

    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelNamesExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeLabelCardinality(qContext: QueryContext,
                                    lp: LabelCardinality): PlanResult = {
    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = shardsFromFilters(renamedFilters, qContext)

    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelCardinalityExec(qContext, dispatcher, dsRef, shard, renamedFilters, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeTsCardinalities(qContext: QueryContext,
                                         lp: TsCardinalities): PlanResult = {
    val metaExec = shardMapperFunc.assignedShards.map{ shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.TsCardExec(qContext, dispatcher, dsRef, shard, lp.shardKeyPrefix, lp.numGroupByFields)
    }
    PlanResult(metaExec)
  }

  private def materializeSeriesKeysByFilters(qContext: QueryContext,
                                             lp: SeriesKeysByFilters): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, _) = extractSchemaFilter(renameMetricFilter(lp.filters))

    val targetSchemaChanges = qContext.plannerParams.targetSchemaProvider
      .getOrElse(targetSchemaProvider)
      .targetSchemaFunc(renamedFilters)
    // Change in Target Schema in query window, do not use target schema to find query shards
    val tsChangeExists = isTargetSchemaChanging(targetSchemaChanges, lp.startMs, lp.endMs)
    val targetSchemaOpt = findTargetSchema(targetSchemaChanges, lp.startMs, lp.endMs)
    val allTSLabelsPresent = useTargetSchemaForShards(renamedFilters, targetSchemaOpt)

    // No change in TargetSchema or not all target-schema labels are provided in the query
    val useTSForQueryShards = !tsChangeExists && allTSLabelsPresent
    val shardsToHit = if (useTSForQueryShards || canGetShardsFromFilters(renamedFilters, qContext)) {
      shardsFromFilters(renamedFilters, qContext, useTSForQueryShards)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(qContext, dispatcher, dsRef, shard, renamedFilters,
                   lp.fetchFirstLastSampleTimes, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec)
  }

  private def materializeRawChunkMeta(qContext: QueryContext,
                                      lp: RawChunkMeta): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) None else Some(lp.column)
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val metaExec = shardsFromFilters(renamedFilters, qContext).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectChunkInfosExec(qContext, dispatcher, dsRef, shard, renamedFilters, toChunkScanMethod(lp.rangeSelector),
        schemaOpt, colName)
    }
    PlanResult(metaExec)
  }
}

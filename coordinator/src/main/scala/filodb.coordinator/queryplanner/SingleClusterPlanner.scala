package filodb.coordinator.queryplanner

import scala.concurrent.duration._

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, QueryConfig, QueryContext}
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.prometheus.ast.WindowConstants
import filodb.query.{exec, _}
import filodb.query.exec.{LocalPartitionDistConcatExec, _}

object SingleClusterPlanner {
  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests").withoutTags
}

/**
  * Responsible for query planning within single FiloDB cluster
  *
  * @param dsRef           dataset
  * @param schema          schema instance, used to extract partKey schema
  * @param spreadProvider  used to get spread
  * @param shardMapperFunc used to get shard locality
  * @param timeSplitEnabled split based on longer time range
  * @param minTimeRangeForSplitMs if time range is longer than this, plan will be split into multiple plans
  * @param splitSizeMs time range for each split, if plan needed to be split
  */
class SingleClusterPlanner(dsRef: DatasetRef,
                           schema: Schemas,
                           shardMapperFunc: => ShardMapper,
                           earliestRetainedTimestampFn: => Long,
                           queryConfig: QueryConfig,
                           spreadProvider: SpreadProvider = StaticSpreadProvider(),
                           timeSplitEnabled: Boolean = false,
                           minTimeRangeForSplitMs: => Long = 1.day.toMillis,
                           splitSizeMs: => Long = 1.day.toMillis)
                           extends QueryPlanner with StrictLogging with PlannerMaterializer {

  override val schemas = schema
  val shardColumns = dsOptions.shardKeyColumns.sorted

  import SingleClusterPlanner._

  private def dispatcherForShard(shard: Int): PlanDispatcher = {
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) {
      logger.debug(s"ShardMapper: $shardMapperFunc")
      throw new RuntimeException(s"Shard: $shard is not available") // TODO fix this
    }
    ActorPlanDispatcher(targetActor)
  }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    if (shardMapperFunc.numShards <= 0) throw new IllegalStateException("No shards available")
    val logicalPlans = if (logicalPlan.isInstanceOf[PeriodicSeriesPlan])
      LogicalPlanUtils.splitPlans(logicalPlan, qContext, timeSplitEnabled, minTimeRangeForSplitMs, splitSizeMs)
    else
      Seq(logicalPlan)
    val materialized = logicalPlans match {
      case Seq(one) => materializeTimeSplitPlan(one, qContext)
      case many =>
        val meterializedPlans = many.map(materializeTimeSplitPlan(_, qContext))
        val targetActor = pickDispatcher(meterializedPlans)

        // create SplitLocalPartitionDistConcatExec that will execute child execplanss sequentially and stitches
        // results back with StitchRvsMapper transformer.
        val stitchPlan = SplitLocalPartitionDistConcatExec(qContext, targetActor, meterializedPlans)
        stitchPlan
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized
  }

  private def materializeTimeSplitPlan(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val materialized = walkLogicalPlanTree(logicalPlan, qContext)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(StitchRvsMapper())
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = pickDispatcher(many)
        many.head match {
          case lve: LabelValuesExec => LabelValuesDistConcatExec(qContext, targetActor, many)
          case ske: PartKeysExec => PartKeysDistConcatExec(qContext, targetActor, many)
          case ep: ExecPlan =>
            val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(StitchRvsMapper())
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized
  }

  private def shardsFromFilters(filters: Seq[ColumnFilter],
                                qContext: QueryContext): Seq[Int] = {

    val spreadProvToUse = qContext.spreadOverride.getOrElse(spreadProvider)

    require(shardColumns.nonEmpty || qContext.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    qContext.shardOverrides.getOrElse {
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
      val shardHash = RecordBuilder.shardKeyHash(shardValues, metric)
      shardMapperFunc.queryShards(shardHash, spreadProvToUse.spreadFunc(filters).last.spread)
    }
  }

  private def toChunkScanMethod(rangeSelector: RangeSelector): ChunkScanMethod = {
    rangeSelector match {
      case IntervalSelector(from, to) => TimeRangeChunkScan(from, to)
      case AllChunksSelector          => AllChunkScan
      case EncodedChunksSelector      => ???
      case WriteBufferSelector        => WriteBufferChunkScan
      case InMemoryChunksSelector     => InMemoryChunkScan
      case _                          => ???
    }
  }

  /**
    * Renames Prom AST __name__ metric name filters to one based on the actual metric column of the dataset,
    * if it is not the prometheus standard
    */
  private def renameMetricFilter(filters: Seq[ColumnFilter]): Seq[ColumnFilter] =
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
  def walkLogicalPlanTree(logicalPlan: LogicalPlan,
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
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(qContext, lp)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(qContext, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(qContext, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(qContext, lp)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(qContext, lp)
      case lp: VectorPlan                  => materializeVectorPlan(qContext, lp)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(qContext, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(qContext, lp)
      case lp: ScalarBinaryOperation       => materializeScalarBinaryOperation(qContext, lp)
      case _                               => throw new BadQueryException("Invalid logical plan")
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def materializeBinaryJoin(qContext: QueryContext,
                                    lp: BinaryJoin): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, qContext)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(qContext, pickDispatcher(lhs.plans), lhs.plans))
    else lhs.plans
    val rhs = walkLogicalPlanTree(lp.rhs, qContext)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(qContext, pickDispatcher(rhs.plans), rhs.plans))
    else rhs.plans

    val onKeysReal = ExtraOnByKeysUtil.getRealOnLabels(lp, queryConfig.addExtraOnByKeysTimeRanges)

    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = pickDispatcher(stitchedLhs ++ stitchedRhs)
    val joined = if (lp.operator.isInstanceOf[SetOperator])
      Seq(exec.SetOperatorExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        LogicalPlanUtils.renameLabels(onKeysReal, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn), dsOptions.metricColumn))
    else
      Seq(BinaryJoinExec(qContext, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        LogicalPlanUtils.renameLabels(onKeysReal, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.ignoring, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(lp.include, dsOptions.metricColumn), dsOptions.metricColumn))
    PlanResult(joined, false)
  }

  private def materializeAggregate(qContext: QueryContext,
                                   lp: Aggregate): PlanResult = {
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, qContext)
    val byKeysReal = ExtraOnByKeysUtil.getRealByLabels(lp, queryConfig.addExtraOnByKeysTimeRanges)

    // Now we have one exec plan per shard
    /*
     * Note that in order for same overlapping RVs to not be double counted when spread is increased,
     * one of the following must happen
     * 1. Step instants must be chosen so time windows dont span shards.
     * 2. We pump data into multiple shards for sometime so atleast one shard will fully contain any time window
     *
     * Pulling all data into one node and stich before reducing (not feasible, doesnt scale). So we will
     * not stitch
     *
     * Starting off with solution 1 first until (2) or some other approach is decided on.
     */
    toReduceLevel1.plans.foreach {
      _.addRangeVectorTransformer(AggregateMapReduce(lp.operator, lp.params,
        LogicalPlanUtils.renameLabels(lp.without, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(byKeysReal, dsOptions.metricColumn)))
    }

    val toReduceLevel2 =
      if (toReduceLevel1.plans.size >= 16) {
        // If number of children is above a threshold, parallelize aggregation
        val groupSize = Math.sqrt(toReduceLevel1.plans.size).ceil.toInt
        toReduceLevel1.plans.grouped(groupSize).map { nodePlans =>
          val reduceDispatcher = nodePlans.head.dispatcher
          LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel1.plans

    val reduceDispatcher = pickDispatcher(toReduceLevel2)
    val reducer = LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, toReduceLevel2, lp.operator, lp.params)
    reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params))
    PlanResult(Seq(reducer), false) // since we have aggregated, no stitching
  }

  private def materializePeriodicSeriesWithWindowing(qContext: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing): PlanResult = {
    val series = walkLogicalPlanTree(lp.series, qContext)
    val rawSource = lp.series.isRaw
    val execRangeFn = InternalRangeFunction.lpToInternalFunc(lp.function)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)

    val newStartMs = boundToStartTimeToEarliestRetained(lp.startMs, lp.stepMs, lp.window, lp.offsetMs.getOrElse(0))
    if (newStartMs <= lp.endMs) { // if there is an overlap between query and retention ranges
      val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(lp.window)
      series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(newStartMs, lp.stepMs,
        lp.endMs, window, Some(execRangeFn), qContext, lp.stepMultipleNotationUsed,
        paramsExec, lp.offsetMs, rawSource)))
      series
    } else { // query is outside retention period, simply return empty result
      PlanResult(Seq(EmptyResultExec(qContext, dsRef)))
    }
  }

  private def materializePeriodicSeries(qContext: QueryContext,
                                        lp: PeriodicSeries): PlanResult = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, qContext)
    val newStartMs = boundToStartTimeToEarliestRetained(lp.startMs, lp.stepMs,
      WindowConstants.staleDataLookbackMillis, lp.offsetMs.getOrElse(0))
    if (newStartMs <= lp.endMs) {  // if there is an overlap between query and retention ranges
      rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(newStartMs, lp.stepMs, lp.endMs,
        None, None, qContext, false, Nil, lp.offsetMs)))
      rawSeries
    } else { // query is outside retention period, simply return empty result
      PlanResult(Seq(EmptyResultExec(qContext, dsRef)))
    }
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
    val spreadProvToUse = qContext.spreadOverride.getOrElse(spreadProvider)
    val offsetMillis: Long = lp.offsetMs.getOrElse(0)
    val colName = lp.columns.headOption
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val spreadChanges = spreadProvToUse.spreadFunc(renamedFilters)
    val rangeSelectorWithOffset = lp.rangeSelector match {
      case IntervalSelector(fromMs, toMs) => IntervalSelector(fromMs - offsetMillis - lp.lookbackMs.getOrElse(
                                             queryConfig.staleSampleAfterMs), toMs - offsetMillis)
      case _                              => lp.rangeSelector
    }
    val needsStitch = rangeSelectorWithOffset match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }
    val execPlans = shardsFromFilters(renamedFilters, qContext).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      MultiSchemaPartitionsExec(qContext, dispatcher, dsRef, shard, renamedFilters,
        toChunkScanMethod(rangeSelectorWithOffset), schemaOpt, colName)
    }
    PlanResult(execPlans, needsStitch)
  }

  private def materializeLabelValues(qContext: QueryContext,
                                     lp: LabelValues): PlanResult = {
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dsOptions.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dsOptions.metricColumn) else lp.labelNames

    val renamedFilters = renameMetricFilter(lp.filters)
    val shardsToHit = if (shardColumns.toSet.subsetOf(renamedFilters.map(_.column).toSet)) {
      shardsFromFilters(renamedFilters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(qContext, dispatcher, dsRef, shard, renamedFilters, labelNames, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec, false)
  }

  private def materializeSeriesKeysByFilters(qContext: QueryContext,
                                             lp: SeriesKeysByFilters): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val filterCols = lp.filters.map(_.column).toSet
    val shardsToHit = if (shardColumns.toSet.subsetOf(filterCols)) {
      shardsFromFilters(lp.filters, qContext)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(qContext, dispatcher, dsRef, shard, renamedFilters,
                   lp.fetchFirstLastSampleTimes, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec, false)
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
    PlanResult(metaExec, false)
  }

  private def materializeScalarTimeBased(qContext: QueryContext,
                                         lp: ScalarTimeBasedPlan): PlanResult = {
    val scalarTimeBasedExec = TimeScalarGeneratorExec(qContext, dsRef, lp.rangeParams, lp.function)
    PlanResult(Seq(scalarTimeBasedExec), false)
  }

  private def materializeFixedScalar(qContext: QueryContext,
                                     lp: ScalarFixedDoublePlan): PlanResult = {
    val scalarFixedDoubleExec = ScalarFixedDoubleExec(qContext, dsRef, lp.timeStepParams, lp.scalar)
    PlanResult(Seq(scalarFixedDoubleExec), false)
  }

  private def materializeScalarBinaryOperation(qContext: QueryContext,
                                               lp: ScalarBinaryOperation): PlanResult = {
    val lhs = if (lp.lhs.isRight) {
      // Materialize as lhs is a logical plan
      val lhsExec = walkLogicalPlanTree(lp.lhs.right.get, qContext)
      Right(lhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.lhs.left.get)

    val rhs = if (lp.rhs.isRight) {
      val rhsExec = walkLogicalPlanTree(lp.rhs.right.get, qContext)
      Right(rhsExec.plans.map(_.asInstanceOf[ScalarBinaryOperationExec]).head)
    } else Left(lp.rhs.left.get)

    val scalarBinaryExec = ScalarBinaryOperationExec(qContext, dsRef, lp.rangeParams, lhs, rhs, lp.operator)
    PlanResult(Seq(scalarBinaryExec), false)
  }

}

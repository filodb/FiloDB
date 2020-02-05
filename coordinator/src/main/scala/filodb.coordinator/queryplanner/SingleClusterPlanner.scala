package filodb.coordinator.queryplanner

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.coordinator.ShardMapper
import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter, RangeParams}
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.query._
import filodb.query.exec._

object SingleClusterPlanner {
  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests").withoutTags
}

/**
  * Responsible for query planning within single FiloDB cluster
  *
  * @param dsRef dataset
  * @param schemas schema instance, used to extract partKey schema
  * @param spreadProvider used to get spread
  * @param shardMapperFunc used to get shard locality
  */
class SingleClusterPlanner(dsRef: DatasetRef,
                           schemas: Schemas,
                           shardMapperFunc: => ShardMapper,
                           spreadProvider: SpreadProvider = StaticSpreadProvider())
                                extends QueryPlanner with StrictLogging {

  private val dsOptions = schemas.part.options
  private val shardColumns = dsOptions.shardKeyColumns.sorted

  import SingleClusterPlanner._

  /**
    * Intermediate Plan Result includes the exec plan(s) along with any state to be passed up the
    * plan building call tree during query planning.
    *
    * Not for runtime use.
    */
  private case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

  /**
    * Picks one dispatcher randomly from child exec plans passed in as parameter
    */
  private def pickDispatcher(children: Seq[ExecPlan]): PlanDispatcher = {
    val childTargets = children.map(_.dispatcher)
    // Above list can contain duplicate dispatchers, and we don't make them distinct.
    // Those with more shards must be weighed higher
    val rnd = ThreadLocalRandom.current()
    childTargets.iterator.drop(rnd.nextInt(childTargets.size)).next
  }

  private def dispatcherForShard(shard: Int): PlanDispatcher = {
    val targetActor = shardMapperFunc.coordForShard(shard)
    if (targetActor == ActorRef.noSender) {
      logger.debug(s"ShardMapper: $shardMapperFunc")
      throw new RuntimeException(s"Shard: $shard is not available") // TODO fix this
    }
    ActorPlanDispatcher(targetActor)
  }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    val materialized = walkLogicalPlanTree(logicalPlan, qContext)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(StitchRvsMapper())
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = pickDispatcher(many)
        many.head match {
          case lve: LabelValuesExec => LabelValuesDistConcatExec(qContext.queryId, targetActor, many)
          case ske: PartKeysExec => PartKeysDistConcatExec(qContext.queryId, targetActor, many)
          case ep: ExecPlan =>
            val topPlan = DistConcatExec(qContext.queryId, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(StitchRvsMapper())
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized

  }


  private def shardsFromFilters(filters: Seq[ColumnFilter],
                                options: QueryContext): Seq[Int] = {

    val spreadProvToUse = options.spreadOverride.getOrElse(spreadProvider)

    require(shardColumns.nonEmpty || options.shardOverrides.nonEmpty,
      s"Dataset $dsRef does not have shard columns defined, and shard overrides were not mentioned")

    options.shardOverrides.getOrElse {
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
  private def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                                  options: QueryContext): PlanResult = {
    logicalPlan match {
      case lp: RawSeries                   => materializeRawSeries(options, lp)
      case lp: RawChunkMeta                => materializeRawChunkMeta(options, lp)
      case lp: PeriodicSeries              => materializePeriodicSeries(options, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(options, lp)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(options, lp)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(options, lp)
      case lp: Aggregate                   => materializeAggregate(options, lp)
      case lp: BinaryJoin                  => materializeBinaryJoin(options, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(options, lp)
      case lp: LabelValues                 => materializeLabelValues(options, lp)
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(options, lp)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(options, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(options, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(options, lp)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(options, lp)
      case lp: VectorPlan                  => materializeVectorPlan(options, lp)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(options, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(options, lp)
      case _                               => throw new BadQueryException("Invalid logical plan")
    }
  }

  private def materializeScalarVectorBinOp(options: QueryContext,
                                           lp: ScalarVectorBinaryOperation): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vector, options)
    val funcArg = materializeFunctionArgs(Seq(lp.scalarArg), options)
    vectors.plans.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalarIsLhs, funcArg)))
    vectors
  }

  private def materializeBinaryJoin(options: QueryContext,
                                    lp: BinaryJoin): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, options)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(options.queryId, pickDispatcher(lhs.plans), lhs.plans))
    else lhs.plans
    val rhs = walkLogicalPlanTree(lp.rhs, options)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(options.queryId, pickDispatcher(rhs.plans), rhs.plans))
    else rhs.plans
    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = pickDispatcher(stitchedLhs ++ stitchedRhs)
    val joined = if (lp.operator.isInstanceOf[SetOperator])
      Seq(exec.SetOperatorExec(options.queryId, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        lp.on, lp.ignoring, dsOptions.metricColumn))
    else
      Seq(BinaryJoinExec(options.queryId, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        lp.on, lp.ignoring, lp.include, dsOptions.metricColumn))
    PlanResult(joined, false)
  }

  private def materializeAggregate(options: QueryContext,
                                   lp: Aggregate): PlanResult = {
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, options)
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
      _.addRangeVectorTransformer(AggregateMapReduce(lp.operator, lp.params, lp.without, lp.by))
    }

    val toReduceLevel2 =
      if (toReduceLevel1.plans.size >= 16) {
        // If number of children is above a threshold, parallelize aggregation
        val groupSize = Math.sqrt(toReduceLevel1.plans.size).ceil.toInt
        toReduceLevel1.plans.grouped(groupSize).map { nodePlans =>
          val reduceDispatcher = nodePlans.head.dispatcher
          ReduceAggregateExec(options.queryId, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel1.plans

    val reduceDispatcher = pickDispatcher(toReduceLevel2)
    val reducer = ReduceAggregateExec(options.queryId, reduceDispatcher, toReduceLevel2, lp.operator, lp.params)
    reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params))
    PlanResult(Seq(reducer), false) // since we have aggregated, no stitching
  }

  private def materializeApplyInstantFunction(options: QueryContext,
                                              lp: ApplyInstantFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, options)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
    vectors
  }

  private def materializeApplyInstantFunctionRaw(options: QueryContext,
                                                 lp: ApplyInstantFunctionRaw): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, options)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
    vectors
  }

  private def materializePeriodicSeriesWithWindowing(options: QueryContext,
                                                     lp: PeriodicSeriesWithWindowing): PlanResult = {
    val series = walkLogicalPlanTree(lp.series, options)
    val rawSource = lp.series.isRaw
    val execRangeFn = InternalRangeFunction.lpToInternalFunc(lp.function)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, options)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(lp.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs,
      lp.endMs, window, Some(execRangeFn), paramsExec, lp.offset, rawSource)))
    series
  }

  private def materializePeriodicSeries(options: QueryContext,
                                        lp: PeriodicSeries): PlanResult = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, options)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.startMs, lp.stepMs, lp.endMs,
      None, None, Nil, lp.offset)))
    rawSeries
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

  private def materializeRawSeries(options: QueryContext,
                                   lp: RawSeries): PlanResult = {

    val spreadProvToUse = options.spreadOverride.getOrElse(spreadProvider)

    val colName = lp.columns.headOption
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val spreadChanges = spreadProvToUse.spreadFunc(renamedFilters)
    val needsStitch = lp.rangeSelector match {
      case IntervalSelector(from, to) => spreadChanges.exists(c => c.time >= from && c.time <= to)
      case _                          => false
    }
    val execPlans = shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      MultiSchemaPartitionsExec(options.queryId, options.submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), schemaOpt, colName)
    }
    PlanResult(execPlans, needsStitch)
  }

  private def materializeLabelValues(options: QueryContext,
                                     lp: LabelValues): PlanResult = {
    val filters = lp.labelConstraints.map { case (k, v) => ColumnFilter(k, Filter.Equals(v)) }.toSeq
    // If the label is PromMetricLabel and is different than dataset's metric name,
    // replace it with dataset's metric name. (needed for prometheus plugins)
    val metricLabelIndex = lp.labelNames.indexOf(PromMetricLabel)
    val labelNames = if (metricLabelIndex > -1 && dsOptions.metricColumn != PromMetricLabel)
      lp.labelNames.updated(metricLabelIndex, dsOptions.metricColumn) else lp.labelNames

    val shardsToHit = if (shardColumns.toSet.subsetOf(lp.labelConstraints.keySet)) {
      shardsFromFilters(filters, options)
    } else {
      mdNoShardKeyFilterRequests.increment
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(options.queryId, options.submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        filters, labelNames, lp.lookbackTimeMs)
    }
    PlanResult(metaExec, false)
  }

  private def materializeSeriesKeysByFilters(options: QueryContext,
                                             lp: SeriesKeysByFilters): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val filterCols = lp.filters.map(_.column).toSet
    val shardsToHit = if (shardColumns.toSet.subsetOf(filterCols)) {
      shardsFromFilters(lp.filters, options)
    } else {
      mdNoShardKeyFilterRequests.increment
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(options.queryId, options.submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        schemas.part, renamedFilters, lp.startMs, lp.endMs)
    }
    PlanResult(metaExec, false)
  }

  private def materializeRawChunkMeta(options: QueryContext,
                                      lp: RawChunkMeta): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) None else Some(lp.column)
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val metaExec = shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectChunkInfosExec(options.queryId, options.submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), schemaOpt, colName)
    }
    PlanResult(metaExec, false)
  }

  private def materializeApplyMiscellaneousFunction(options: QueryContext,
                                                    lp: ApplyMiscellaneousFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs)))
    vectors
  }

  private def materializeFunctionArgs(functionParams: Seq[FunctionArgsPlan],
                                      options: QueryContext): Seq[FuncArgs] = {
    if (functionParams.isEmpty){
      Nil
    } else {
      functionParams.map { param =>
        param match {
          case num: ScalarFixedDoublePlan => StaticFuncArgs(num.scalar, num.timeStepParams)
          case s: ScalarVaryingDoublePlan => ExecPlanFuncArgs(materialize(s, options),
                                                              RangeParams(s.startMs, s.stepMs, s.endMs))
          case  t: ScalarTimeBasedPlan    => TimeFuncArgs(t.rangeParams)
          case _                          => throw new UnsupportedOperationException("Invalid logical plan")
        }
      }
    }
  }

  private def materializeScalarPlan(options: QueryContext,
                                    lp: ScalarVaryingDoublePlan): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, options)
    if (vectors.plans.length > 1) {
      val targetActor = pickDispatcher(vectors.plans)
      val topPlan = DistConcatExec(options.queryId, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer(ScalarFunctionMapper(lp.function, RangeParams(lp.startMs, lp.stepMs, lp.endMs)))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(ScalarFunctionMapper(lp.function,
                                                               RangeParams(lp.startMs, lp.stepMs, lp.endMs))))
      vectors
    }
  }

  private def materializeApplySortFunction(options: QueryContext,
                                           lp: ApplySortFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, options)
    if(vectors.plans.length > 1) {
      val targetActor = pickDispatcher(vectors.plans)
      val topPlan = DistConcatExec(options.queryId, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer(SortFunctionMapper(lp.function))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(SortFunctionMapper(lp.function)))
      vectors
    }
  }

  private def materializeAbsentFunction(options: QueryContext,
                                        lp: ApplyAbsentFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, options)
    if (vectors.plans.length > 1) {
      val targetActor = pickDispatcher(vectors.plans)
      val topPlan = DistConcatExec(options.queryId, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer(AbsentFunctionMapper(lp.columnFilters, lp.rangeParams,
        PromMetricLabel))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(AbsentFunctionMapper(lp.columnFilters, lp.rangeParams,
        dsOptions.metricColumn )))
      vectors
    }
  }

  private def materializeScalarTimeBased(options: QueryContext,
                                         lp: ScalarTimeBasedPlan): PlanResult = {
    val scalarTimeBasedExec = TimeScalarGeneratorExec(options.queryId, dsRef, lp.rangeParams, lp.function,
      options.sampleLimit)
    PlanResult(Seq(scalarTimeBasedExec), false)
  }

  private def materializeVectorPlan(options: QueryContext,
                                    lp: VectorPlan): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.scalars, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(VectorFunctionMapper()))
    vectors
  }

  private def materializeFixedScalar(options: QueryContext,
                                     lp: ScalarFixedDoublePlan): PlanResult = {
    val scalarFixedDoubleExec = ScalarFixedDoubleExec(options.queryId, dsRef, lp.timeStepParams, lp.scalar,
      options.sampleLimit)
    PlanResult(Seq(scalarFixedDoubleExec), false)
  }

}

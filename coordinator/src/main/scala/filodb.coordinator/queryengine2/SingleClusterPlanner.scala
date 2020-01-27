package filodb.coordinator.queryengine2

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon

import filodb.coordinator.ShardMapper
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.query.{ColumnFilter, Filter}
import filodb.core.store.{AllChunkScan, ChunkScanMethod, InMemoryChunkScan, TimeRangeChunkScan, WriteBufferChunkScan}
import filodb.prometheus.ast.Vectors.{PromMetricLabel, TypeLabel}
import filodb.query._
import filodb.query.exec._

object SingleClusterPlanner {
  private val mdNoShardKeyFilterRequests = Kamon.counter("queryengine-metadata-no-shardkey-requests")
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
                           spreadProvider: SpreadProvider,
                           shardMapperFunc: => ShardMapper) extends QueryPlanner with StrictLogging {

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

  def materialize(queryId: String,
                  submitTime: Long,
                  logicalPlan: LogicalPlan,
                  options: QueryOptions): ExecPlan = {

    // TODO downsampling planning goes here

    val materialized = walkLogicalPlanTree(logicalPlan, queryId, System.currentTimeMillis(), options)
    match {
      case PlanResult(Seq(justOne), stitch) =>
        if (stitch) justOne.addRangeVectorTransformer(StitchRvsMapper())
        justOne
      case PlanResult(many, stitch) =>
        val targetActor = pickDispatcher(many)
        many.head match {
          case lve: LabelValuesExec => LabelValuesDistConcatExec(queryId, targetActor, many)
          case ske: PartKeysExec => PartKeysDistConcatExec(queryId, targetActor, many)
          case ep: ExecPlan =>
            val topPlan = DistConcatExec(queryId, targetActor, many)
            if (stitch) topPlan.addRangeVectorTransformer(StitchRvsMapper())
            topPlan
        }
    }
    logger.debug(s"Materialized logical plan for dataset=$dsRef :" +
      s" $logicalPlan to \n${materialized.printTree()}")
    materialized

  }

  private def shardsFromFilters(filters: Seq[ColumnFilter],
                                options: QueryOptions): Seq[Int] = {

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
      case _ => ???
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
                                  queryId: String,
                                  submitTime: Long,
                                  options: QueryOptions): PlanResult = {
    logicalPlan match {
      case lp: RawSeries                   => materializeRawSeries(queryId, submitTime, options, lp)
      case lp: RawChunkMeta                => materializeRawChunkMeta(queryId, submitTime, options, lp)
      case lp: PeriodicSeries              => materializePeriodicSeries(queryId, submitTime, options, lp)
      case lp: PeriodicSeriesWithWindowing => materializePeriodicSeriesWithWindowing(queryId, submitTime, options, lp)
      case lp: ApplyInstantFunction        => materializeApplyInstantFunction(queryId, submitTime, options, lp)
      case lp: ApplyInstantFunctionRaw     => materializeApplyInstantFunctionRaw(queryId, submitTime, options, lp)
      case lp: Aggregate                   => materializeAggregate(queryId, submitTime, options, lp)
      case lp: BinaryJoin                  => materializeBinaryJoin(queryId, submitTime, options, lp)
      case lp: ScalarVectorBinaryOperation => materializeScalarVectorBinOp(queryId, submitTime, options, lp)
      case lp: LabelValues                 => materializeLabelValues(queryId, submitTime, options, lp)
      case lp: SeriesKeysByFilters         => materializeSeriesKeysByFilters(queryId, submitTime, options, lp)
      case lp: ApplyMiscellaneousFunction  => materializeApplyMiscellaneousFunction(queryId, submitTime, options, lp)
      case lp: ApplySortFunction           => materializeApplySortFunction(queryId, submitTime, options, lp)
      case lp: ScalarVaryingDoublePlan     => materializeScalarPlan(queryId, submitTime, options, lp)
      case lp: ScalarTimeBasedPlan         => materializeScalarTimeBased(queryId, submitTime, options, lp)
      case lp: VectorPlan                  => materializeVectorPlan(queryId, submitTime, options, lp)
      case lp: ScalarFixedDoublePlan       => materializeFixedScalar(queryId, submitTime, options, lp)
      case lp: ApplyAbsentFunction         => materializeAbsentFunction(queryId, submitTime, options, lp)
      case _                               => throw new BadQueryException("Invalid logical plan")
    }
  }

  private def materializeScalarVectorBinOp(queryId: String,
                                           submitTime: Long,
                                           options: QueryOptions,
                                           lp: ScalarVectorBinaryOperation): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vector, queryId, submitTime, options)
    val funcArg = materializeFunctionArgs(Seq(lp.scalarArg), queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalarIsLhs, funcArg)))
    vectors
  }

  private def materializeBinaryJoin(queryId: String,
                                    submitTime: Long,
                                    options: QueryOptions,
                                    lp: BinaryJoin): PlanResult = {
    val lhs = walkLogicalPlanTree(lp.lhs, queryId, submitTime, options)
    val stitchedLhs = if (lhs.needsStitch) Seq(StitchRvsExec(queryId, pickDispatcher(lhs.plans), lhs.plans))
    else lhs.plans
    val rhs = walkLogicalPlanTree(lp.rhs, queryId, submitTime, options)
    val stitchedRhs = if (rhs.needsStitch) Seq(StitchRvsExec(queryId, pickDispatcher(rhs.plans), rhs.plans))
    else rhs.plans
    // TODO Currently we create separate exec plan node for stitching.
    // Ideally, we can go one step further and add capability to NonLeafNode plans to pre-process
    // and transform child results individually before composing child results together.
    // In theory, more efficient to use transformer than to have separate exec plan node to avoid IO.
    // In the interest of keeping it simple, deferring decorations to the ExecPlan. Add only if needed after measuring.

    val targetActor = pickDispatcher(stitchedLhs ++ stitchedRhs)
    val joined = if (lp.operator.isInstanceOf[SetOperator])
      Seq(exec.SetOperatorExec(queryId, targetActor, stitchedLhs, stitchedRhs, lp.operator,
        lp.on, lp.ignoring, dsOptions.metricColumn))
    else
      Seq(BinaryJoinExec(queryId, targetActor, stitchedLhs, stitchedRhs, lp.operator, lp.cardinality,
        lp.on, lp.ignoring, lp.include, dsOptions.metricColumn))
    PlanResult(joined, false)
  }

  private def materializeAggregate(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
                                   lp: Aggregate): PlanResult = {
    val toReduceLevel1 = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
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
          ReduceAggregateExec(queryId, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel1.plans

    val reduceDispatcher = pickDispatcher(toReduceLevel2)
    val reducer = ReduceAggregateExec(queryId, reduceDispatcher, toReduceLevel2, lp.operator, lp.params)
    reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params))
    PlanResult(Seq(reducer), false) // since we have aggregated, no stitching
  }

  private def materializeApplyInstantFunction(queryId: String,
                                              submitTime: Long,
                                              options: QueryOptions,
                                              lp: ApplyInstantFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
    vectors
  }

  private def materializeApplyInstantFunctionRaw(queryId: String,
                                                 submitTime: Long,
                                                 options: QueryOptions,
                                                 lp: ApplyInstantFunctionRaw): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
    vectors
  }

  private def materializePeriodicSeriesWithWindowing(queryId: String,
                                                     submitTime: Long,
                                                     options: QueryOptions,
                                                     lp: PeriodicSeriesWithWindowing): PlanResult = {
    val series = walkLogicalPlanTree(lp.series, queryId, submitTime, options)
    val rawSource = lp.series.isRaw
    val execRangeFn = InternalRangeFunction.lpToInternalFunc(lp.function)
    val paramsExec = materializeFunctionArgs(lp.functionArgs, queryId, submitTime, options)
    val window = if (execRangeFn == InternalRangeFunction.Timestamp) None else Some(lp.window)
    series.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step,
      lp.end, window, Some(execRangeFn), paramsExec, lp.offset, rawSource)))
    series
  }

  private def materializePeriodicSeries(queryId: String,
                                        submitTime: Long,
                                        options: QueryOptions,
                                        lp: PeriodicSeries): PlanResult = {
    val rawSeries = walkLogicalPlanTree(lp.rawSeries, queryId, submitTime, options)
    rawSeries.plans.foreach(_.addRangeVectorTransformer(PeriodicSamplesMapper(lp.start, lp.step, lp.end,
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

  private def materializeRawSeries(queryId: String,
                                   submitTime: Long,
                                   options: QueryOptions,
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
      MultiSchemaPartitionsExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), schemaOpt, colName)
    }
    PlanResult(execPlans, needsStitch)
  }

  private def materializeLabelValues(queryId: String,
                                     submitTime: Long,
                                     options: QueryOptions,
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
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      exec.LabelValuesExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        filters, labelNames, lp.lookbackTimeInMillis)
    }
    PlanResult(metaExec, false)
  }

  private def materializeSeriesKeysByFilters(queryId: String,
                                             submitTime: Long,
                                             options: QueryOptions,
                                             lp: SeriesKeysByFilters): PlanResult = {
    // NOTE: _type_ filter support currently isn't there in series keys queries
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val filterCols = lp.filters.map(_.column).toSet
    val shardsToHit = if (shardColumns.toSet.subsetOf(filterCols)) {
      shardsFromFilters(lp.filters, options)
    } else {
      mdNoShardKeyFilterRequests.increment()
      shardMapperFunc.assignedShards
    }
    val metaExec = shardsToHit.map { shard =>
      val dispatcher = dispatcherForShard(shard)
      PartKeysExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        schemas.part, renamedFilters, lp.start, lp.end)
    }
    PlanResult(metaExec, false)
  }

  private def materializeRawChunkMeta(queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions,
                                      lp: RawChunkMeta): PlanResult = {
    // Translate column name to ID and validate here
    val colName = if (lp.column.isEmpty) None else Some(lp.column)
    val (renamedFilters, schemaOpt) = extractSchemaFilter(renameMetricFilter(lp.filters))
    val metaExec = shardsFromFilters(renamedFilters, options).map { shard =>
      val dispatcher = dispatcherForShard(shard)
      SelectChunkInfosExec(queryId, submitTime, options.sampleLimit, dispatcher, dsRef, shard,
        renamedFilters, toChunkScanMethod(lp.rangeSelector), schemaOpt, colName)
    }
    PlanResult(metaExec, false)
  }

  private def materializeApplyMiscellaneousFunction(queryId: String,
                                                    submitTime: Long,
                                                    options: QueryOptions,
                                                    lp: ApplyMiscellaneousFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs)))
    vectors
  }

  private def materializeFunctionArgs(functionParams: Seq[FunctionArgsPlan],
                                      queryId: String,
                                      submitTime: Long,
                                      options: QueryOptions): Seq[FuncArgs] = {
    if (functionParams.isEmpty){
      Nil
    } else {
      functionParams.map { param =>
        param match {
          case num: ScalarFixedDoublePlan => StaticFuncArgs(num.scalar, num.timeStepParams)
          case s: ScalarVaryingDoublePlan => ExecPlanFuncArgs(materialize(queryId, submitTime, s, options),
                                                              s.timeStepParams)
          case  t: ScalarTimeBasedPlan    => TimeFuncArgs(t.rangeParams)
          case _                          => throw new UnsupportedOperationException("Invalid logical plan")
        }
      }
    }
  }

  private def materializeScalarPlan(queryId: String, submitTime: Long,
                                    options: QueryOptions,
                                    lp: ScalarVaryingDoublePlan): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    if (vectors.plans.length > 1) {
      val targetActor = pickDispatcher(vectors.plans)
      val topPlan = DistConcatExec(queryId, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer(ScalarFunctionMapper(lp.function, lp.timeStepParams))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(ScalarFunctionMapper(lp.function, lp.timeStepParams)))
      vectors
    }
  }

  private def materializeApplySortFunction(queryId: String,
                                           submitTime: Long,
                                           options: QueryOptions,
                                           lp: ApplySortFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    if(vectors.plans.length > 1) {
      val targetActor = pickDispatcher(vectors.plans)
      val topPlan = DistConcatExec(queryId, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer(SortFunctionMapper(lp.function))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(SortFunctionMapper(lp.function)))
      vectors
    }
  }

  private def materializeAbsentFunction(queryId: String,
                                        submitTime: Long,
                                        options: QueryOptions,
                                        lp: ApplyAbsentFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, queryId, submitTime, options)
    if (vectors.plans.length > 1) {
      val targetActor = pickDispatcher(vectors.plans)
      val topPlan = DistConcatExec(queryId, targetActor, vectors.plans)
      topPlan.addRangeVectorTransformer(AbsentFunctionMapper(lp.columnFilters, lp.rangeParams,
        PromMetricLabel))
      PlanResult(Seq(topPlan), vectors.needsStitch)
    } else {
      vectors.plans.foreach(_.addRangeVectorTransformer(AbsentFunctionMapper(lp.columnFilters, lp.rangeParams,
        dsOptions.metricColumn )))
      vectors
    }
  }

  private def materializeScalarTimeBased(queryId: String,
                                         submitTime: Long,
                                         options: QueryOptions,
                                         lp: ScalarTimeBasedPlan): PlanResult = {
    val scalarTimeBasedExec = TimeScalarGeneratorExec(queryId, dsRef, lp.rangeParams, lp.function,
      options.sampleLimit)
    PlanResult(Seq(scalarTimeBasedExec), false)
  }

  private def materializeVectorPlan(queryId: String,
                                    submitTime: Long,
                                    options: QueryOptions,
                                    lp: VectorPlan): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.scalars, queryId, submitTime, options)
    vectors.plans.foreach(_.addRangeVectorTransformer(VectorFunctionMapper()))
    vectors
  }

  private def materializeFixedScalar(queryId: String,
                                     submitTime: Long,
                                     options: QueryOptions,
                                     lp: ScalarFixedDoublePlan): PlanResult = {
    val scalarFixedDoubleExec = ScalarFixedDoubleExec(queryId, dsRef, lp.timeStepParams, lp.scalar,
      options.sampleLimit)
    PlanResult(Seq(scalarFixedDoubleExec), false)
  }

}

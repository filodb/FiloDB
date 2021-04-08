package filodb.coordinator.queryplanner

import java.util.concurrent.ThreadLocalRandom

import com.typesafe.scalalogging.StrictLogging

import filodb.core.metadata.{DatasetOptions, Schemas}
import filodb.core.query.{ColumnFilter, PromQlQueryParams, QueryContext, RangeParams}
import filodb.query._
import filodb.query.exec._

/**
  * Intermediate Plan Result includes the exec plan(s) along with any state to be passed up the
  * plan building call tree during query planning.
  *
  * Not for runtime use.
  */
case class PlanResult(plans: Seq[ExecPlan], needsStitch: Boolean = false)

trait  PlannerMaterializer {
    def schemas: Schemas
    def dsOptions: DatasetOptions = schemas.part.options

    /**
      * Picks one dispatcher randomly from child exec plans passed in as parameter
      */
    def pickDispatcher(children: Seq[ExecPlan]): PlanDispatcher = {
      val childTargets = children.map(_.dispatcher)
      // Above list can contain duplicate dispatchers, and we don't make them distinct.
      // Those with more shards must be weighed higher
      val rnd = ThreadLocalRandom.current()
      childTargets.iterator.drop(rnd.nextInt(childTargets.size)).next
    }

    def materializeVectorPlan(qContext: QueryContext,
                              lp: VectorPlan): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.scalars, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(VectorFunctionMapper()))
      vectors
    }

    def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan
    def materializeFunctionArgs(functionParams: Seq[FunctionArgsPlan],
                                qContext: QueryContext): Seq[FuncArgs] = {
      if (functionParams.isEmpty) {
        Nil
      } else {
        functionParams.map { param =>
          param match {
            case num: ScalarFixedDoublePlan => StaticFuncArgs(num.scalar, num.timeStepParams)
            case s: ScalarVaryingDoublePlan => ExecPlanFuncArgs(materialize(s, qContext),
                                               RangeParams(s.startMs, s.stepMs, s.endMs))
            case  t: ScalarTimeBasedPlan    => TimeFuncArgs(t.rangeParams)
            case s: ScalarBinaryOperation   => ExecPlanFuncArgs(materialize(s, qContext),
                                               RangeParams(s.startMs, s.stepMs, s.endMs))
          }
        }
      }
    }

    def walkLogicalPlanTree(logicalPlan: LogicalPlan,
                            qContext: QueryContext): PlanResult

    def materializeApplyInstantFunction(qContext: QueryContext,
                                        lp: ApplyInstantFunction): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
      vectors
    }

    def materializeApplyMiscellaneousFunction(qContext: QueryContext,
                                              lp: ApplyMiscellaneousFunction): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      if (lp.function == MiscellaneousFunctionId.HistToPromVectors)
        vectors.plans.foreach(_.addRangeVectorTransformer(HistToPromSeriesMapper(schemas.part)))
      else
        vectors.plans.foreach(_.addRangeVectorTransformer(MiscellaneousFunctionMapper(lp.function, lp.stringArgs)))
      vectors
    }

    def materializeApplyInstantFunctionRaw(qContext: QueryContext,
                                           lp: ApplyInstantFunctionRaw): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      val paramsExec = materializeFunctionArgs(lp.functionArgs, qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(InstantVectorFunctionMapper(lp.function, paramsExec)))
      vectors
    }

    def materializeScalarVectorBinOp(qContext: QueryContext,
                                     lp: ScalarVectorBinaryOperation): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vector, qContext)
      val funcArg = materializeFunctionArgs(Seq(lp.scalarArg), qContext)
      vectors.plans.foreach(_.addRangeVectorTransformer(ScalarOperationMapper(lp.operator, lp.scalarIsLhs, funcArg)))
      vectors
    }

    def materializeApplySortFunction(qContext: QueryContext,
                                     lp: ApplySortFunction): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      if (vectors.plans.length > 1) {
        val targetActor = pickDispatcher(vectors.plans)
        val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, vectors.plans)
        topPlan.addRangeVectorTransformer(SortFunctionMapper(lp.function))
        PlanResult(Seq(topPlan), vectors.needsStitch)
      } else {
        vectors.plans.foreach(_.addRangeVectorTransformer(SortFunctionMapper(lp.function)))
        vectors
      }
    }

    def materializeScalarPlan(qContext: QueryContext,
                              lp: ScalarVaryingDoublePlan): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      if (vectors.plans.length > 1) {
        val targetActor = pickDispatcher(vectors.plans)
        val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, vectors.plans)
        topPlan.addRangeVectorTransformer(ScalarFunctionMapper(lp.function,
          RangeParams(lp.startMs, lp.stepMs, lp.endMs)))
        PlanResult(Seq(topPlan), vectors.needsStitch)
      } else {
        vectors.plans.foreach(_.addRangeVectorTransformer(ScalarFunctionMapper(lp.function,
          RangeParams(lp.startMs, lp.stepMs, lp.endMs))))
        vectors
      }
    }

   def addAbsentFunctionMapper(vectors: PlanResult,
                               columnFilters: Seq[ColumnFilter],
                               rangeParams: RangeParams,
                               queryContext: QueryContext): PlanResult = {
      vectors.plans.foreach(_.addRangeVectorTransformer(AbsentFunctionMapper(columnFilters, rangeParams,
        dsOptions.metricColumn )))
      vectors
  }

   def addAggregator(lp: Aggregate, qContext: QueryContext, toReduceLevel: PlanResult,
                     extraOnByKeysTimeRanges: Seq[Seq[Long]]):
   LocalPartitionReduceAggregateExec = {

    val byKeysReal = ExtraOnByKeysUtil.getRealByLabels(lp, extraOnByKeysTimeRanges)
    // Now we have one exec plan per shard
    /*
     * Note that in order for same overlapping RVs to not be double counted when spread is increased,
     * one of the following must happen
     * 1. Step instants must be chosen so time windows dont span shards.
     * 2. We pump data into multiple shards for sometime so atleast one shard will fully contain any time window
     *
     * Pulling all data into one node and stitch before reducing (not feasible, doesnt scale). So we will
     * not stitch
     *
     * Starting off with solution 1 first until (2) or some other approach is decided on.
     */
    toReduceLevel.plans.foreach {
      _.addRangeVectorTransformer(AggregateMapReduce(lp.operator, lp.params,
        LogicalPlanUtils.renameLabels(lp.without, dsOptions.metricColumn),
        LogicalPlanUtils.renameLabels(byKeysReal, dsOptions.metricColumn)))
    }

    val toReduceLevel2 =
      if (toReduceLevel.plans.size >= 16) {
        // If number of children is above a threshold, parallelize aggregation
        val groupSize = Math.sqrt(toReduceLevel.plans.size).ceil.toInt
        toReduceLevel.plans.grouped(groupSize).map { nodePlans =>
          val reduceDispatcher = nodePlans.head.dispatcher
          LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, nodePlans, lp.operator, lp.params)
        }.toList
      } else toReduceLevel.plans

    val reduceDispatcher = pickDispatcher(toReduceLevel2)
    val reducer = LocalPartitionReduceAggregateExec(qContext, reduceDispatcher, toReduceLevel2, lp.operator, lp.params)

    if (!qContext.plannerParams.skipAggregatePresent)
      reducer.addRangeVectorTransformer(AggregatePresenter(lp.operator, lp.params, RangeParams(
        lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000)))

    reducer
  }
  def materializeAbsentFunction(qContext: QueryContext,
                                  lp: ApplyAbsentFunction): PlanResult = {
    val vectors = walkLogicalPlanTree(lp.vectors, qContext)
    val aggregate = Aggregate(AggregationOperator.Sum, lp, Nil, Seq("job"))
    val aggregatePlanResult = PlanResult(Seq(addAggregator(aggregate, qContext, vectors, Seq.empty)))
    addAbsentFunctionMapper(aggregatePlanResult, lp.columnFilters,
      RangeParams(lp.startMs / 1000, lp.stepMs / 1000, lp.endMs / 1000), qContext)
    }
}

object PlannerUtil extends StrictLogging {

   /**
   * Returns URL params for label values which is used to create Metadata remote exec plan
   */
   def getLabelValuesUrlParams(lp: LabelValues, queryParams: PromQlQueryParams): Map[String, String] = {
    val quote = if (queryParams.remoteQueryPath.get.contains("""/v2/label/""")) """"""" else ""
    // Filter value should be enclosed in quotes for label values v2 endpoint
    val filters = lp.filters.map{ f => s"""${f.column}${f.filter.operatorString}$quote${f.filter.valuesStrings.
      head}$quote"""}.mkString(",")
    Map("filter" -> filters, "labels" -> lp.labelNames.mkString(","))
  }

}

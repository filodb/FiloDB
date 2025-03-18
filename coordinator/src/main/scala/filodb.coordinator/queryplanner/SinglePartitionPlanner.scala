package filodb.coordinator.queryplanner

import filodb.core.metadata.{Dataset, DatasetOptions, Schemas}
import filodb.core.query.{QueryConfig, QueryContext}
import filodb.query._
import filodb.query.exec._


/**
  * SinglePartitionPlanner is responsible for planning in situations where time series data is
  * distributed across multiple clusters.
  *
  * @param planners map of clusters names in the local partition to their Planner objects
  * @param plannerSelector a function that selects the planner name given the metric name
  * @param dataset a function that selects the planner name given the metric name
  */
class SinglePartitionPlanner(planners: Map[String, QueryPlanner],
                             plannerSelector: String => String,
                             val dataset: Dataset,
                             val queryConfig: QueryConfig)
  extends QueryPlanner with DefaultPlanner {

  def childPlanners(): Seq[QueryPlanner] = planners.values.toSeq
  private var rootPlanner: Option[QueryPlanner] = None
  def getRootPlanner(): Option[QueryPlanner] = rootPlanner
  def setRootPlanner(rootPlanner: QueryPlanner): Unit = {
    this.rootPlanner = Some(rootPlanner)
  }
  initRootPlanner()

  override val schemas: Schemas = Schemas(dataset.schema)
  override val dsOptions: DatasetOptions = schemas.part.options

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    require(getRootPlanner().isDefined, "Root planner not set. Internal error.")
    walkLogicalPlanTree(logicalPlan, qContext).plans.head
  }

  /**
    * Returns planner for first metric in logical plan
    * If logical plan does not have metric, first planner present in planners is returned
    */
  private def getPlanner(logicalPlan: LogicalPlan): QueryPlanner = getAllPlanners(logicalPlan).head

  /**
   * Given a logical plan gets all the planners needed to materialize the leaf level metrics
   *
   * @param logicalPlan The logical plan instance
   * @return The Seq of QueryPlanners. It is guaranteed this Seq will not be empty and at the minimum the first
   *         planner in the planners will be returned
   */
  private def getAllPlanners(logicalPlan: LogicalPlan): Seq[QueryPlanner] =
    LogicalPlanUtils.getMetricName(logicalPlan, dsOptions.metricColumn)
      .map(x => planners(plannerSelector(x))).toList match {
          case Nil     => Seq(planners.values.head)
          case x @ _   => x
    }

  private def materializeOthers(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult =
    PlanResult(Seq(getPlanner(logicalPlan).materialize(logicalPlan, qContext)))


  private def materializeLabelValues(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    val execPlans = planners.values.toList.distinct.map(_.materialize(logicalPlan, qContext))
    PlanResult(Seq(if (execPlans.size == 1) execPlans.head
    else LabelValuesDistConcatExec(qContext, inProcessPlanDispatcher, execPlans)))
  }

  private def materializeLabelNames(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    val execPlans = planners.values.toList.distinct.map(_.materialize(logicalPlan, qContext))
    PlanResult(Seq(if (execPlans.size == 1) execPlans.head
    else LabelNamesDistConcatExec(qContext, inProcessPlanDispatcher, execPlans)))
  }

  private def materializeSeriesKeysFilters(logicalPlan: LogicalPlan, qContext: QueryContext): PlanResult = {
    val execPlans = planners.values.toList.distinct.map(_.materialize(logicalPlan, qContext))
    PlanResult(Seq(if (execPlans.size == 1) execPlans.head
    else PartKeysDistConcatExec(qContext, inProcessPlanDispatcher, execPlans)))
  }

  private def materializeTsCardinalities(logicalPlan: TsCardinalities, qContext: QueryContext): PlanResult = {
    val execPlans = logicalPlan.datasets.map(d => planners.get(d))
      .map(x => x.get.materialize(logicalPlan, qContext))
    PlanResult(Seq(TsCardReduceExec(qContext, inProcessPlanDispatcher, execPlans)))
  }


  /**
   * Apart from some metadata queries, SinglePartitionPlanner should either push down to underlying planner or perform
   * the operation inprocess if these operations span across different planners. The implementation execute plans
   * inprocess are defined in DefaultPlanner and thus we delegate the materialization of these plan to the default
   * walk implementation in DefaultPlanner
   *
   * @param logicalPlan    The LogicalPlan instance
   * @param qContext       The QueryContext
   * @param forceInProcess if true, all materialized plans for this entire
   *                       logical plan will dispatch via an InProcessDispatcher
   * @return The PlanResult containing the ExecPlan
   */
  override def walkLogicalPlanTree(logicalPlan: LogicalPlan, qContext: QueryContext, forceInProcess: Boolean)
  : PlanResult = logicalPlan match {
        case lp: LabelValues                  => this.materializeLabelValues(lp, qContext)
        case lp: TsCardinalities              => this.materializeTsCardinalities(lp, qContext)
        case lp: SeriesKeysByFilters          => this.materializeSeriesKeysFilters(lp, qContext)
        case lp: LabelNames                   => this.materializeLabelNames(lp, qContext)

        case _: ApplyInstantFunction         |
             _: ApplyInstantFunctionRaw      |
             _: Aggregate                    |
             _: BinaryJoin                   |
             _: ScalarVectorBinaryOperation  |
             _: ApplyMiscellaneousFunction   |
             _: ApplySortFunction            |
             _: ScalarVaryingDoublePlan      |
             _: ScalarTimeBasedPlan          |
             _: VectorPlan                   |
             _: ScalarFixedDoublePlan        |
             _: ApplyAbsentFunction          |
             _: ScalarBinaryOperation        |
             _: SubqueryWithWindowing        |
             _: TopLevelSubquery             |
             _: ApplyLimitFunction            =>
                                              val leafPlanners = getAllPlanners(logicalPlan)
                                              // Check if only one planner is needed to materialize the PromQL, if yes
                                              // simply let that planner materialize
                                              if (leafPlanners.tail.isEmpty)
                                                PlanResult(Seq(leafPlanners.head.materialize(logicalPlan, qContext)))
                                              else
                                                super.defaultWalkLogicalPlanTree(logicalPlan, qContext, forceInProcess)

        case  _: LabelCardinality            |
              _: PeriodicSeriesWithWindowing |
              _: PeriodicSeries              |
              _: RawSeries                   |
              _: RawChunkMeta                 => this.materializeOthers(logicalPlan, qContext)
      }
}


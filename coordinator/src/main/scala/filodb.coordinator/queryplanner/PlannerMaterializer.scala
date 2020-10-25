package filodb.coordinator.queryplanner

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Future

import com.softwaremill.sttp.SttpBackend
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import filodb.core.metadata.{DatasetOptions, Schemas}
import filodb.core.query.{PromQlQueryParams, QueryContext, RangeParams}
import filodb.prometheus.ast.Vectors.PromMetricLabel
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

    def materializeAbsentFunction(qContext: QueryContext,
                                  lp: ApplyAbsentFunction): PlanResult = {
      val vectors = walkLogicalPlanTree(lp.vectors, qContext)
      if (vectors.plans.length > 1) {
        val targetActor = pickDispatcher(vectors.plans)
        val topPlan = LocalPartitionDistConcatExec(qContext, targetActor, vectors.plans)
        topPlan.addRangeVectorTransformer(AbsentFunctionMapper(lp.columnFilters, lp.rangeParams,
          PromMetricLabel))
        PlanResult(Seq(topPlan), vectors.needsStitch)
      } else {
        vectors.plans.foreach(_.addRangeVectorTransformer(AbsentFunctionMapper(lp.columnFilters, lp.rangeParams,
          dsOptions.metricColumn )))
        vectors
      }
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

  def getSttpBackend(queryConfig: Config): SttpBackend[Future, Nothing] = {
    val sttpBackendConfig = queryConfig.getConfig("remote.http.client.sttp-backend")
    val sttpBackendFactoryClass = sttpBackendConfig.getString("factory")
    val sttBackendConstructor = Class.forName(sttpBackendFactoryClass).getConstructors.head
    val sttpBackendFactory = sttBackendConstructor.newInstance().asInstanceOf[RemoteExecSttpBackendFactory]
    logger.info(s"Using sttpBackend factory $sttpBackendFactory with config ${sttpBackendConfig}")
    val sttpBackend = sttpBackendFactory.create(sttpBackendConfig)
    sttpBackend
  }

}

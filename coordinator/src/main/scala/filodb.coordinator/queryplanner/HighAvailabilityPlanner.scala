package filodb.coordinator.queryplanner

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import filodb.query.{LogicalPlan, PromQlInvocationParams, PromQlQueryParams, QueryContext}
import filodb.query.exec.{ExecPlan, InProcessPlanDispatcher, PromQlExec, StitchRvsExec}

/**
  * HighAvailabilityPlanner responsible for using underlying local planner and FailureProvider
  * to come up with a plan that orchestrates query execution between multiple
  * replica clusters. If there are failures in one cluster then query is routed
  * to other cluster.
  *
  * @param dsRef dataset
  * @param localPlanner the planner to generate plans for local pod
  * @param failureProvider the provider that helps route plan execution to HA cluster
  * @param queryEngineConfig config that determines query engine behavior
  */
class HighAvailabilityPlanner(dsRef: DatasetRef,
                              localPlanner: QueryPlanner,
                              failureProvider: FailureProvider,
                              queryEngineConfig: Config = ConfigFactory.empty) extends QueryPlanner with StrictLogging {

  import LogicalPlanUtils._
  import QueryFailureRoutingStrategy._

  /**
    * Converts Route objects returned by FailureProvider to ExecPlan
    */
  private def routeExecPlanMapper(routes: Seq[Route], rootLogicalPlan: LogicalPlan,
                                  qContext: QueryContext, lookBackTime: Long): ExecPlan = {

    val execPlans: Seq[ExecPlan] = routes.map { route =>
      route match {
        case route: LocalRoute => if (route.timeRange.isEmpty)
          localPlanner.materialize(rootLogicalPlan, qContext)
        else
          localPlanner.materialize(
            copyWithUpdatedTimeRange(rootLogicalPlan, route.asInstanceOf[LocalRoute].timeRange.get, lookBackTime),
            qContext)
        case route: RemoteRoute =>
          val timeRange = route.timeRange.get
          val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
          val routingConfig = queryEngineConfig.getConfig("routing")
          // Divide by 1000 to convert millis to seconds. PromQL params are in seconds.
          val promQlInvocationParams = PromQlInvocationParams(routingConfig, queryParams.promQl,
            timeRange.startInMillis / 1000, queryParams.step, timeRange.endInMillis / 1000,
            queryParams.spread, processFailure = false)
          logger.debug("PromQlExec params:" + promQlInvocationParams)
          PromQlExec(qContext.queryId, InProcessPlanDispatcher(), dsRef, promQlInvocationParams, qContext.submitTime)
      }
    }

    if (execPlans.size == 1) execPlans.head
    else StitchRvsExec(qContext.queryId,
                       InProcessPlanDispatcher(),
                       execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlExec]))
    // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
    // Sort to move RemoteExec in end as it does not have schema

  }

  def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    // lazy because we want to fetch failures only if needed
    lazy val periodicSeriesTime = getPeriodicSeriesTimeFromLogicalPlan(logicalPlan)
    lazy val lookBackTime = getRawSeriesStartTime(logicalPlan).map(periodicSeriesTime.startInMillis - _).get
    lazy val routingTime = TimeRange(periodicSeriesTime.startInMillis - lookBackTime, periodicSeriesTime.endInMillis)
    lazy val failures = failureProvider.getFailures(dsRef, routingTime).sortBy(_.timeRange.startInMillis)

    val tsdbQueryParams = qContext.origQueryParams
    if (!logicalPlan.isRoutable ||
        !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
        (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
          && !tsdbQueryParams.asInstanceOf[PromQlQueryParams].processFailure) || // This is a query that was part of
        !hasSingleTimeRange(logicalPlan) || // Sub queries have different time ranges (unusual)
        failures.isEmpty) { // no failures in query time range
      localPlanner.materialize(logicalPlan, qContext)
    } else {
      val promQlQueryParams = tsdbQueryParams.asInstanceOf[PromQlQueryParams]
      val routes = if (promQlQueryParams.start == promQlQueryParams.end) { // Instant Query
        if (failures.forall(!_.isRemote)) {
          Seq(RemoteRoute(Some(TimeRange(periodicSeriesTime.startInMillis, periodicSeriesTime.endInMillis))))
        } else {
          Seq(LocalRoute(None))
        }
      } else {
        plan(failures, periodicSeriesTime, lookBackTime, promQlQueryParams.step * 1000)
      }
      logger.debug("Routes: " + routes)
      routeExecPlanMapper(routes, logicalPlan, qContext, lookBackTime)
    }
  }
}

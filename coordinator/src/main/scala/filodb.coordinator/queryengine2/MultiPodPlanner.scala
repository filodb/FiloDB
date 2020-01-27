package filodb.coordinator.queryengine2

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging

import filodb.coordinator.client.QueryCommands.StaticSpreadProvider
import filodb.core.{DatasetRef, SpreadProvider}
import filodb.query.{LogicalPlan, PromQlInvocationParams, QueryOptions}
import filodb.query.exec.{ExecPlan, InProcessPlanDispatcher, PromQlExec, StitchRvsExec}

class MultiPodPlanner(dsRef: DatasetRef,
                      localPlanner: SinglePodPlanner,
                      failureProvider: FailureProvider,
                      spreadProvider: SpreadProvider,
                      queryEngineConfig: Config = ConfigFactory.empty()) extends StrictLogging {

  /**
    * Converts Routes to ExecPlan
    */
  private def routeExecPlanMapper(routes: Seq[Route], rootLogicalPlan: LogicalPlan, queryId: String, submitTime: Long,
                                  options: QueryOptions, lookBackTime: Long,
                                  tsdbQueryParams: TsdbQueryParams): ExecPlan = {

    val execPlans: Seq[ExecPlan]= routes.map { route =>
      route match {
        case route: LocalRoute => if (route.timeRange.isEmpty)
          localPlanner.generateLocalExecPlan(rootLogicalPlan, queryId, submitTime, options, spreadProvider)
        else
          localPlanner.generateLocalExecPlan(QueryRoutingPlanner.copyWithUpdatedTimeRange(rootLogicalPlan,
            route.asInstanceOf[LocalRoute].timeRange.get, lookBackTime), queryId, submitTime, options, spreadProvider)
        case route: RemoteRoute =>
          val timeRange = route.timeRange.get
          val queryParams = tsdbQueryParams.asInstanceOf[PromQlQueryParams]
          val routingConfig = queryEngineConfig.getConfig("routing")
          val promQlInvocationParams = PromQlInvocationParams(routingConfig, queryParams.promQl,
            timeRange.startInMillis / 1000, queryParams.step, timeRange.endInMillis / 1000, queryParams.spread,
            processFailure = false)
          logger.debug("PromQlExec params:" + promQlInvocationParams)
          PromQlExec(queryId, InProcessPlanDispatcher(), dsRef, promQlInvocationParams, submitTime)
      }
    }

    if (execPlans.size == 1)
      execPlans.head
    else
    // Stitch RemoteExec plan results with local using InProcessorDispatcher
    // Sort to move RemoteExec in end as it does not have schema
      StitchRvsExec(queryId, InProcessPlanDispatcher(),
        execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlExec]))
  }

  def materialize(rootLogicalPlan: LogicalPlan,
                  options: QueryOptions,
                  tsdbQueryParams: TsdbQueryParams): ExecPlan = {

    val queryId = UUID.randomUUID().toString
    val submitTime = System.currentTimeMillis()

    // lazy because we want to fetch failures only if needed
    lazy val periodicSeriesTime = QueryRoutingPlanner.getPeriodicSeriesTimeFromLogicalPlan(rootLogicalPlan)
    lazy val lookBackTime = QueryRoutingPlanner.getRawSeriesStartTime(rootLogicalPlan)
      .map(periodicSeriesTime.startInMillis - _).get

    lazy val routingTime = TimeRange(periodicSeriesTime.startInMillis - lookBackTime, periodicSeriesTime.endInMillis)
    lazy val failures = failureProvider.getFailures(dsRef, routingTime).sortBy(_.timeRange.startInMillis)

    if (!QueryRoutingPlanner.isPeriodicSeriesPlan(rootLogicalPlan) || // It is a raw data query
      !rootLogicalPlan.isRoutable ||
      !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
      (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
        && !tsdbQueryParams.asInstanceOf[PromQlQueryParams].processFailure) || // This is a query that was part of
      !QueryRoutingPlanner.hasSingleTimeRange(rootLogicalPlan) || // Sub queries have different time ranges (unusual)
      failures.isEmpty) { // no failures in query time range
      localPlanner.generateLocalExecPlan(rootLogicalPlan, queryId, submitTime, options)
    } else {
      val promQlQueryParams = tsdbQueryParams.asInstanceOf[PromQlQueryParams]
      val routes = if (promQlQueryParams.start == promQlQueryParams.end) { // Instant Query
        if (failures.exists(_.isRemote)) {
          Seq(RemoteRoute(Some(TimeRange(periodicSeriesTime.startInMillis, periodicSeriesTime.endInMillis))))
        } else {
          Seq(LocalRoute(None))
        }
      } else {
        QueryRoutingPlanner.plan(failures, periodicSeriesTime, lookBackTime, promQlQueryParams.step * 1000)
      }
      logger.debug("Routes: " + routes)
      routeExecPlanMapper(routes, rootLogicalPlan, queryId, submitTime,
        options, querySpreadProvider, lookBackTime, tsdbQueryParams)
    }
  }

}

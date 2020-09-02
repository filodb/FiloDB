package filodb.coordinator.queryplanner

import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.query.{LabelValues, LogicalPlan, SeriesKeysByFilters}
import filodb.query.exec._

/**
  * HighAvailabilityPlanner responsible for using underlying local planner and FailureProvider
  * to come up with a plan that orchestrates query execution between multiple
  * replica clusters. If there are failures in one cluster then query is routed
  * to other cluster.
  *
  * @param dsRef dataset
  * @param localPlanner the planner to generate plans for local pod
  * @param failureProvider the provider that helps route plan execution to HA cluster
  * @param queryConfig config that determines query engine behavior
  */
class HighAvailabilityPlanner(dsRef: DatasetRef,
                              localPlanner: QueryPlanner,
                              failureProvider: FailureProvider,
                              queryConfig: QueryConfig) extends QueryPlanner with StrictLogging {

  import net.ceedubs.ficus.Ficus._
  import LogicalPlanUtils._
  import QueryFailureRoutingStrategy._

  val remoteHttpEndpoint: String = queryConfig.routingConfig.getString("remote.http.endpoint")

  val remoteHttpTimeoutMs: Long =
    queryConfig.routingConfig.config.as[Option[Long]]("remote.http.timeout").getOrElse(60000)

  private def stitchPlans(rootLogicalPlan: LogicalPlan,
                          execPlans: Seq[ExecPlan],
                          queryContext: QueryContext)= {
    rootLogicalPlan match {
        case lp: LabelValues         => LabelValuesDistConcatExec(queryContext, InProcessPlanDispatcher,
                                        execPlans.sortWith((x, y) => !x.isInstanceOf[MetadataRemoteExec]))
        case lp: SeriesKeysByFilters => PartKeysDistConcatExec(queryContext, InProcessPlanDispatcher,
                                        execPlans.sortWith((x, y) => !x.isInstanceOf[MetadataRemoteExec]))
        case _                       => StitchRvsExec(queryContext, InProcessPlanDispatcher,
                                         execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
  }

  private def getLabelValuesUrlParams(lp: LabelValues) = Map("filter" -> lp.filters.map{f => f.column +
    f.filter.operatorString + f.filter.valuesStrings.head}.mkString(","),
    "labels" -> lp.labelNames.mkString(","))

  /**
    * Converts Route objects returned by FailureProvider to ExecPlan
    */
  private def routeExecPlanMapper(routes: Seq[Route], rootLogicalPlan: LogicalPlan,
                                  qContext: QueryContext, lookBackTime: Long): ExecPlan = {

    val offsetMs = LogicalPlanUtils.getOffsetMillis(rootLogicalPlan)
    val execPlans: Seq[ExecPlan] = routes.map { route =>
      route match {
        case route: LocalRoute => if (route.timeRange.isEmpty)
          localPlanner.materialize(rootLogicalPlan, qContext)
        else {
          val timeRange = route.asInstanceOf[LocalRoute].timeRange.get
          // Routes are created according to offset but logical plan should have time without offset.
          // Offset logic is handled in ExecPlan
          localPlanner.materialize(
            copyLogicalPlanWithUpdatedTimeRange(rootLogicalPlan, TimeRange(timeRange.startMs + offsetMs,
              timeRange.endMs + offsetMs)), qContext)
        }
        case route: RemoteRoute =>
          val timeRange = route.timeRange.get
          val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
          // Divide by 1000 to convert millis to seconds. PromQL params are in seconds.
          val promQlParams = PromQlQueryParams(queryParams.promQl,
            (timeRange.startMs + offsetMs) / 1000, queryParams.stepSecs, (timeRange.endMs + offsetMs) / 1000,
            queryParams.spread, processFailure = false)
          logger.debug("PromQlExec params:" + promQlParams)
          val httpEndpoint = remoteHttpEndpoint + queryParams.remoteQueryPath.getOrElse("")
          rootLogicalPlan match {
            case lp: LabelValues         => MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            getLabelValuesUrlParams(lp), qContext, InProcessPlanDispatcher,
                                            dsRef, promQlParams)
            case lp: SeriesKeysByFilters => val urlParams = Map("match[]" -> queryParams.promQl)
                                            MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                              urlParams, qContext, InProcessPlanDispatcher, dsRef, promQlParams)
            case _                       => PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            qContext, InProcessPlanDispatcher, dsRef, promQlParams)
          }

      }
    }

    if (execPlans.size == 1) execPlans.head
    else stitchPlans(rootLogicalPlan, execPlans, qContext)
  }

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    // lazy because we want to fetch failures only if needed
    lazy val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    lazy val periodicSeriesTime = getTimeFromLogicalPlan(logicalPlan)
    lazy val periodicSeriesTimeWithOffset = TimeRange(periodicSeriesTime.startMs - offsetMillis,
      periodicSeriesTime.endMs - offsetMillis)
    lazy val lookBackTime = getLookBackMillis(logicalPlan)
    // Time at which raw data would be retrieved which is used to get failures.
    // It should have time with offset and lookback as we need raw data at time including offset and lookback.
    lazy val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackTime,
      periodicSeriesTimeWithOffset.endMs)
    lazy val failures = failureProvider.getFailures(dsRef, queryTimeRange).sortBy(_.timeRange.startMs)

    val tsdbQueryParams = qContext.origQueryParams
    if (!logicalPlan.isRoutable ||
        !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
        (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
          && !tsdbQueryParams.asInstanceOf[PromQlQueryParams].processFailure) || // This is a query that was
                                                                                 // part of failure routing
        !hasSingleTimeRange(logicalPlan) || // Sub queries have different time ranges (unusual)
        failures.isEmpty) { // no failures in query time range
      localPlanner.materialize(logicalPlan, qContext)
    } else {
      val promQlQueryParams = tsdbQueryParams.asInstanceOf[PromQlQueryParams]
      val routes = if (promQlQueryParams.startSecs == promQlQueryParams.endSecs) { // Instant Query
        if (failures.forall(!_.isRemote)) {
          Seq(RemoteRoute(Some(TimeRange(periodicSeriesTimeWithOffset.startMs, periodicSeriesTimeWithOffset.endMs))))
        } else {
          Seq(LocalRoute(None))
        }
      } else {
        plan(failures, periodicSeriesTimeWithOffset, lookBackTime, promQlQueryParams.stepSecs * 1000)
      }
      logger.debug("Routes: " + routes)
      routeExecPlanMapper(routes, logicalPlan, qContext, lookBackTime)
    }
  }
}

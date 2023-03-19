package filodb.coordinator.queryplanner

import java.util.concurrent.ConcurrentHashMap

import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.jdk.CollectionConverters._

import com.typesafe.scalalogging.StrictLogging
import io.grpc.ManagedChannel

import filodb.core.DatasetRef
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.grpc.GrpcCommonUtils
import filodb.query.{LabelNames, LabelValues, LogicalPlan, SeriesKeysByFilters}
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
                              queryConfig: QueryConfig,
                              remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient,
                              channels: ConcurrentMap[String, ManagedChannel] =
                              new ConcurrentHashMap[String, ManagedChannel]().asScala)
  extends QueryPlanner with StrictLogging {

  import LogicalPlanUtils._
  import QueryFailureRoutingStrategy._
  import LogicalPlan._

  // HTTP endpoint is still mandatory as metadata queries still use it.
  val remoteHttpEndpoint: String = queryConfig.remoteHttpEndpoint
    .getOrElse(throw new IllegalArgumentException("remoteHttpEndpoint config needed"))

  val partitionName = queryConfig.partitionName
    .getOrElse(throw new IllegalArgumentException("partitionName config needed"))

  val remoteGrpcEndpoint: Option[String] = queryConfig.remoteGrpcEndpoint

  if(remoteGrpcEndpoint.isDefined)
    logger.info("Remote gRPC endpoint for HA configured to {}", remoteGrpcEndpoint.get)
  else
    logger.info("No remote gRPC endpoint for HA Planner configured")

  val remoteHttpTimeoutMs: Long = queryConfig.remoteHttpTimeoutMs.getOrElse(60000)

  val inProcessPlanDispatcher = InProcessPlanDispatcher(queryConfig)

  private def stitchPlans(rootLogicalPlan: LogicalPlan,
                          execPlans: Seq[ExecPlan],
                          queryContext: QueryContext)= {
    rootLogicalPlan match {
        case lp: LabelValues         => LabelValuesDistConcatExec(queryContext, inProcessPlanDispatcher,
                                        execPlans.sortWith((x, y) => !x.isInstanceOf[MetadataRemoteExec]))
        case lp: LabelNames          => LabelNamesDistConcatExec(queryContext, inProcessPlanDispatcher,
                                        execPlans.sortWith((x, y) => !x.isInstanceOf[MetadataRemoteExec]))
        case lp: SeriesKeysByFilters => PartKeysDistConcatExec(queryContext, inProcessPlanDispatcher,
                                        execPlans.sortWith((x, y) => !x.isInstanceOf[MetadataRemoteExec]))
        case _                       => StitchRvsExec(queryContext, inProcessPlanDispatcher,
                                         rvRangeFromPlan(rootLogicalPlan),
                                         execPlans.sortWith((x, y) => !x.isInstanceOf[PromQlRemoteExec]))
      // ^^ Stitch RemoteExec plan results with local using InProcessPlanDispatcher
      // Sort to move RemoteExec in end as it does not have schema
    }
  }

  //scalastyle:off method.length
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
            copyLogicalPlanWithUpdatedTimeRange(rootLogicalPlan, TimeRange(timeRange.startMs + offsetMs.max,
              timeRange.endMs + offsetMs.min)), qContext)
        }
        case route: RemoteRoute =>
          val timeRange = route.timeRange.get
          val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
          // Divide by 1000 to convert millis to seconds. PromQL params are in seconds.
          val promQlParams = PromQlQueryParams(queryParams.promQl,
            (timeRange.startMs + offsetMs.max) / 1000, queryParams.stepSecs, (timeRange.endMs + offsetMs.min) / 1000)
          val newQueryContext = qContext.copy(origQueryParams = promQlParams, plannerParams = qContext.plannerParams.
            copy(processFailure = false, processMultiPartition = false) )
          logger.debug("PromQlExec params:" + promQlParams)
          val httpEndpoint = remoteHttpEndpoint + queryParams.remoteQueryPath.getOrElse("")
          rootLogicalPlan match {
            case lp: LabelValues         => MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            PlannerUtil.getLabelValuesUrlParams(lp, queryParams), newQueryContext,
                                            inProcessPlanDispatcher, dsRef, remoteExecHttpClient)
            case lp: LabelNames         => MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            Map("match[]" -> queryParams.promQl), newQueryContext,
                                            inProcessPlanDispatcher, dsRef, remoteExecHttpClient)
            case lp: SeriesKeysByFilters => val urlParams = Map("match[]" -> queryParams.promQl)
                                            MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                              urlParams, newQueryContext, inProcessPlanDispatcher,
                                              dsRef, remoteExecHttpClient)
            case _                       =>
              if (remoteGrpcEndpoint.isDefined && !(queryConfig.grpcPartitionsDenyList.contains("*") ||
                queryConfig.grpcPartitionsDenyList.contains(partitionName.toLowerCase))) {
                val endpoint = remoteGrpcEndpoint.get
                val channel = channels.getOrElseUpdate(endpoint, GrpcCommonUtils.buildChannelFromEndpoint(endpoint))
                PromQLGrpcRemoteExec(channel, remoteHttpTimeoutMs, newQueryContext, inProcessPlanDispatcher,
                  dsRef)
              } else
                PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            newQueryContext, inProcessPlanDispatcher, dsRef, remoteExecHttpClient)
          }

      }
    }

    if (execPlans.size == 1) execPlans.head
    else stitchPlans(rootLogicalPlan, execPlans, qContext)
  }
  //scalastyle:on method.length

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {

    // lazy because we want to fetch failures only if needed
    lazy val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    lazy val periodicSeriesTime = getTimeFromLogicalPlan(logicalPlan)
    lazy val periodicSeriesTimeWithOffset = TimeRange(periodicSeriesTime.startMs - offsetMillis.max,
      periodicSeriesTime.endMs - offsetMillis.min)
    lazy val lookBackTime = getLookBackMillis(logicalPlan).max
    // Time at which raw data would be retrieved which is used to get failures.
    // It should have time with offset and lookback as we need raw data at time including offset and lookback.
    lazy val queryTimeRange = TimeRange(periodicSeriesTimeWithOffset.startMs - lookBackTime,
      periodicSeriesTimeWithOffset.endMs)
    lazy val failures = failureProvider.getFailures(dsRef, queryTimeRange).sortBy(_.timeRange.startMs)

    val tsdbQueryParams = qContext.origQueryParams
    if (!logicalPlan.isRoutable ||
        !tsdbQueryParams.isInstanceOf[PromQlQueryParams] || // We don't know the promql issued (unusual)
        (tsdbQueryParams.isInstanceOf[PromQlQueryParams]
          && !qContext.plannerParams.processFailure) || // This is a query that was
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

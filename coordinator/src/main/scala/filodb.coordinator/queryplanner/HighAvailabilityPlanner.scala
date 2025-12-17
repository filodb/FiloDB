package filodb.coordinator.queryplanner

import java.util.concurrent.ConcurrentHashMap

import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.jdk.CollectionConverters._

import com.typesafe.scalalogging.StrictLogging
import io.grpc.ManagedChannel

import filodb.coordinator.GrpcPlanDispatcher
import filodb.coordinator.ShardMapper
import filodb.core.DatasetRef
import filodb.core.metrics.FilodbMetrics
import filodb.core.query.{ActiveShardMapper, DownPartition, LegacyFailoverMode, PlannerParams, ShardLevelFailoverMode}
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.grpc.GrpcCommonUtils
import filodb.query.{LabelNames, LabelValues, LogicalPlan, SeriesKeysByFilters}
import filodb.query.exec._

object HighAvailabilityPlanner {
  final val FailoverCounterName = "single-cluster-failovers-materialized"
}
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
                              localShardMapperFunc: => ShardMapper,
                              failureProvider: FailureProvider,
                              queryConfig: QueryConfig,
                              remoteExecHttpClient: RemoteExecHttpClient = RemoteHttpClient.defaultClient,
                              channels: ConcurrentMap[String, ManagedChannel] =
                              new ConcurrentHashMap[String, ManagedChannel]().asScala,
                              buddyShardMapperProvider: Option[ActiveShardMapperProvider] = None,
                              workUnit: String,
                              buddyWorkUnit: String,
                              clusterName: String,
                              useShardLevelFailover : Boolean
                             )
  extends QueryPlanner with StrictLogging {

  import LogicalPlanUtils._
  import QueryFailureRoutingStrategy._
  import LogicalPlan._

  // legacy failover counter captures failovers when we send a PromQL to the buddy
  // cluster
  val legacyFailoverCounter = FilodbMetrics.counter(HighAvailabilityPlanner.FailoverCounterName,
                                                    Map("cluster" -> clusterName, "type" -> "legacy"))

  // full failover counter captures failovers when we materialize a plan locally and
  // send an entire plan to the buddy cluster for execution
  val fullFailoverCounter = FilodbMetrics.counter(HighAvailabilityPlanner.FailoverCounterName,
                                                  Map("cluster" -> clusterName, "type" -> "full"))

  // partial failover counter captures failovers when we materialize a plan locally and
  // send some parts of it for execution to the buddy cluster
  val partialFailoverCounter = FilodbMetrics.counter(HighAvailabilityPlanner.FailoverCounterName,
                                                     Map("cluster" -> clusterName, "type" -> "partial"))

  // HTTP endpoint is still mandatory as metadata queries still use it.
  val remoteHttpEndpoint: String = queryConfig.remoteHttpEndpoint
    .getOrElse(throw new IllegalArgumentException("remoteHttpEndpoint config needed"))

  val partitionName = queryConfig.partitionName
    .getOrElse(throw new IllegalArgumentException("partitionName config needed"))

  val plannerSelector: String = queryConfig.plannerSelector
    .getOrElse(throw new IllegalArgumentException("plannerSelector is mandatory"))

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
            copyLogicalPlanWithUpdatedSeconds(rootLogicalPlan,
              (timeRange.startMs + offsetMs.max) / 1000,
              (timeRange.endMs + offsetMs.min) / 1000),
            qContext)
        }
        case route: RemoteRoute =>
          legacyFailoverCounter.increment()
          val timeRange = route.timeRange.get
          val queryParams = qContext.origQueryParams.asInstanceOf[PromQlQueryParams]
          // rootLogicalPlan can be different from queryParams.promQl
          // because rootLogicalPlan may not include the transformer that will not sent to remote.
          // For instance, when promql = 1 - sum(foo), rootLogicalPlan = sum(foo).
          // Because the logic "1 - " is translated to a transformer that runs locally.
          // Divide by 1000 to convert millis to seconds. PromQL params are in seconds.
          val promQlParams = PromQlQueryParams(LogicalPlanParser.convertToQuery(rootLogicalPlan),
            (timeRange.startMs + offsetMs.max) / 1000, queryParams.stepSecs, (timeRange.endMs + offsetMs.min) / 1000)
          val newQueryContext = qContext.copy(origQueryParams = promQlParams, plannerParams = qContext.plannerParams.
            copy(processFailure = false, processMultiPartition = false) )
          logger.debug("PromQlExec params:" + promQlParams)
          val httpEndpoint = remoteHttpEndpoint + queryParams.remoteQueryPath.getOrElse("")
          rootLogicalPlan match {
            case lp: LabelValues         => MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            PlannerUtil.getLabelValuesUrlParams(lp, queryParams), newQueryContext,
                                            inProcessPlanDispatcher, dsRef, remoteExecHttpClient, queryConfig)
            case lp: LabelNames         => MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            Map("match[]" -> queryParams.promQl), newQueryContext,
                                            inProcessPlanDispatcher, dsRef, remoteExecHttpClient, queryConfig)
            case lp: SeriesKeysByFilters => val urlParams = Map("match[]" -> queryParams.promQl)
                                            MetadataRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                              urlParams, newQueryContext, inProcessPlanDispatcher,
                                              dsRef, remoteExecHttpClient, queryConfig)
            case _                       =>
              if (remoteGrpcEndpoint.isDefined && !(queryConfig.grpcPartitionsDenyList.contains("*") ||
                queryConfig.grpcPartitionsDenyList.contains(partitionName.toLowerCase))) {
                val endpoint = remoteGrpcEndpoint.get
                val channel = channels.getOrElseUpdate(endpoint, GrpcCommonUtils.buildChannelFromEndpoint(endpoint))
                PromQLGrpcRemoteExec(channel, remoteHttpTimeoutMs, newQueryContext, inProcessPlanDispatcher,
                  dsRef, plannerSelector)
              } else
                PromQlRemoteExec(httpEndpoint, remoteHttpTimeoutMs,
                                            newQueryContext, inProcessPlanDispatcher, dsRef, remoteExecHttpClient)
          }

      }
    }

    if (execPlans.size == 1) execPlans.head
    else stitchPlans(rootLogicalPlan, execPlans, qContext)
  }

  override def materialize(logicalPlan: LogicalPlan, qContext: QueryContext): ExecPlan = {
    val origQueryParams = qContext.origQueryParams

    val unroutableLogicalPlan = !logicalPlan.isRoutable
    // We don't know the promql issued (unusual)
    lazy val notPromQlQuery = !origQueryParams.isInstanceOf[PromQlQueryParams]
    // This is a query that was part of failure routing
    lazy val markedNotToBeFailedOver =
      origQueryParams.isInstanceOf[PromQlQueryParams] && !qContext.plannerParams.processFailure
    // TODO why would we ever care that there is only one time range
    // keeping it "as is" for now
    // Sub queries have different time ranges (unusual)
    lazy val hasMultipleTimeRanges = !hasSingleTimeRange(logicalPlan)
    // materializeLegacy uses getFailures call that mixes 2 issues:
    // 1) shards down and
    // 2) failures associated with a FiloDB cluster.
    // FailureTimeRange marks a FiloDB cluster partitions/work_units/dataset/cluster_type
    // having bad/non existent data, allowing to redirect traffic to the cluster that has data.
    // The issue is that if we have shards down getFailures() will return a FailureTimeRange
    // which conceptually is NOT correct. Supposed we have one shard down in us-east-xyz and one in us-west-zyx.
    // We still can return the results for as long as the down shards are NOT overlapping but if we get
    // two FailureTimeRanges from both of the work units, theoretically we should not respond to the query
    // at all since we know that both clusters have corrupted/invalid/non existent data.
    // Instead we should keep shards down and FailureTimeRanges information separate!
    // Keeping the existing logic in place for now but allow one to switch to the new logic.
    if (unroutableLogicalPlan || notPromQlQuery || markedNotToBeFailedOver || hasMultipleTimeRanges) {
      // we don't want to do any complicated shard level logic and instead
      // even want to turn it off completely to make sure that the logic stays
      // exactly the same
      val plannerParams = qContext.plannerParams.copy(
        failoverMode = LegacyFailoverMode
      )
      val q = qContext.copy(plannerParams = plannerParams)
      localPlanner.materialize(logicalPlan, q)
    } else if (useShardLevelFailover || qContext.plannerParams.failoverMode == ShardLevelFailoverMode) {
      // we need to populate planner params with the shard maps
      val localActiveShardMapper = getLocalActiveShardMapper(qContext.plannerParams)
      val remoteActiveShardMapper = getRemoteActiveShardMapper(qContext.plannerParams)
      val shardLevelFailoverIsNeeded =
        (!localActiveShardMapper.allShardsActive) && (!remoteActiveShardMapper.allShardsActive)
      if (shardLevelFailoverIsNeeded) {
        val plannerParams = qContext.plannerParams.copy(
          localShardMapper = Some(localActiveShardMapper),
          buddyShardMapper = Some(remoteActiveShardMapper)
        )
        val q = qContext.copy(plannerParams = plannerParams)
        materializeShardLevelFailover(logicalPlan, q)
      } else {
        materializeLegacy(logicalPlan, qContext)
      }
    } else {
      materializeLegacy(logicalPlan, qContext)
    }
  }
  //scalastyle:on method.length

  def materializeLegacy(logicalPlan: LogicalPlan, oqContext: QueryContext): ExecPlan = {
    // ensure that we do not use any features of shard level failover
    val plannerParams = oqContext.plannerParams.copy(
      failoverMode = LegacyFailoverMode
    )
    val qContext = oqContext.copy(plannerParams = plannerParams)
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
    val failures = failureProvider.getFailures(dsRef, queryTimeRange).sortBy(_.timeRange.startMs)
    val tsdbQueryParams = qContext.origQueryParams
    if (failures.isEmpty) { // no failures in query time range
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

  // Shard level failover handles two distinct cases:
  // CASE NUMBER_ONE
  // EVERYTHING is executed remotely on the buddy cluster
  // buddy remote cluster might still have shards down but we are not allowed to query local,
  // hence, we still create a plan to query remote. If remote cluster has shards down,
  // we should check if partial results are allowed, and if not FAIL immediately (without
  // sending the query to the remote system and letting the remote system to resolve the issue)
  // In that case remote shards should be used for both leaf plans and interim plans.
  // ===============================
  // | SingleClusterPlanner should:
  // |  a) have explicit map of remote shards (can only by in QueryContext, unless we want to change
  // |     QueryPlanner interface)
  // |  b) should understand that we do shard level failover.
  // |  c) should understand that local shards are not to be used, instead RemoteActorPlanDispatcher's
  // |     need to be created, that on the other side will get converted to ActorPlanDispatcher.
  // ===============================
  // CASE NUMBER TWO
  // We do mixed processing utilizing some remote and some local shards.
  // Interim processing (non-leaf plans) is done locally, shards not available locally are queried remotely
  // with push down of the transformers.
  // SingleClusterPlanner will:
  // 1) test if the query can be answered at all with the combination of local and remote
  // 2) for leaf plans that need data which is not available locally,
  //    create RemoteActorPlanDispatchers instead of ActorPlanDispatchers
  //    with the actor path that is provided by QueryContext with a remote shard map
  //    The plans need to be wrapped with GenericRemoteExec that is injected with GrpcRemoteDispatcher.
  //    RemoteActorPlanDispatcher's on the remote side would create a corresponding
  //    ActorPlanDispatcher's during the actual dispatch calls.
  // 2) have access to remote shard map
  // 3) interim processing (non leaf plans) is done by the local FiloDB nodes.
  //    Ideally, the data that can be processed remotely should be processed remotely. Imagine an
  //    unlikely case when there are about 50% of shards healthy locally and 50% remotely. If we
  //    do all the interim work locally, our local nodes would have to work twice as hard, potentially
  //    running out of memory. It's not trivial, however, to implement such planning and if we introduce
  //    stateless nodes in the future, it won't be needed anyway. If, however, we want to distribute
  //    the load as even as possible we could do use the following logic:
  //          A) if healthy local shard count > healthy remote shards, just always use local shards
  //          B) if healthy remote shard count > healthy local shards, just always use remote
  //             in this case, REMOTE side would call on local shards (this shows how inefficient it
  //             is to not be able to query shards directly from anywhere by a stateless query engine farm).
  //    Since we don't do the above we might run into issue, if number of local shards is small,
  //    it is especially bad if query has multi-level aggregation. If this becomes an issue we can
  //    introduce a check requiring at least 50% local shards running, otherwise give up.
  //    This might increase operational overhead (if we have a bunch of shards down it's better to
  //    failover to a partition where we have more shards up as we might fail the queries that
  //    theoretically could be resolved).
  def materializeShardLevelFailover(
    logicalPlan: LogicalPlan, qContext: QueryContext
  ) : ExecPlan = {
    val offsetMillis = LogicalPlanUtils.getOffsetMillis(logicalPlan)
    val periodicSeriesTime = getTimeFromLogicalPlan(logicalPlan)
    val periodicSeriesTimeWithOffset = TimeRange(periodicSeriesTime.startMs - offsetMillis.max,
      periodicSeriesTime.endMs - offsetMillis.min)
    val lookBackTime = getLookBackMillis(logicalPlan).max
    // Time at which raw data would be retrieved which is used to get failures.
    // It should have time with offset and lookback as we need raw data at time including offset and lookback.
    val queryTimeRange = TimeRange(
      periodicSeriesTimeWithOffset.startMs - lookBackTime,
      periodicSeriesTimeWithOffset.endMs
    )
    val failures = failureProvider.getMaintenancesAndDataIssues(dsRef, queryTimeRange).sortBy(_.timeRange.startMs)
    // if failures is an empty collection, next statement will be true
    val canQueryLocal = failures.forall(f => f.isRemote)
    val canQueryRemote = failures.forall(f => !f.isRemote)
    val cannotQueryLocal = !canQueryLocal
    val cannotQueryRemote = !canQueryRemote
    val pp = qContext.plannerParams
    val localActiveShardMapper = pp.localShardMapper.get
    val remoteActiveShardMapper = pp.buddyShardMapper.get
    if (cannotQueryLocal && cannotQueryRemote) {
      // the only way this might happen is because one of the partitions is in maintenance
      // and the other one has a mark saying that the partition has a gap
      throw new filodb.core.query.ServiceUnavailableException(
        "Time range of the query is marked as unavailable by the FailureProvider"
      )
    } else if (cannotQueryLocal) {
      materializeAllRemote(logicalPlan, qContext, localActiveShardMapper, remoteActiveShardMapper)
    } else if (cannotQueryRemote) {
      val plannerParams = qContext.plannerParams.copy(
        failoverMode = LegacyFailoverMode
      )
      val q = qContext.copy(plannerParams = plannerParams)
      localPlanner.materialize(logicalPlan, q)
    } else if (localActiveShardMapper.allShardsActive) {
      val plannerParams = qContext.plannerParams.copy(
        failoverMode = LegacyFailoverMode
      )
      val q = qContext.copy(plannerParams = plannerParams)
      localPlanner.materialize(logicalPlan, q)
    } else if (remoteActiveShardMapper.allShardsActive) {
      materializeAllRemote(logicalPlan, qContext, localActiveShardMapper, remoteActiveShardMapper)
    } else {
      materializeMixedLocalAndRemote(logicalPlan, qContext, localActiveShardMapper, remoteActiveShardMapper)
    }
  }

  def remoteShardsAreHealthy(): Boolean = {
    buddyShardMapperProvider.get.getActiveShardMapper().allShardsActive
  }

  def materializeMixedLocalAndRemote(
    logicalPlan: LogicalPlan, qContext : QueryContext,
    localActiveShardMapper: ActiveShardMapper,
    remoteActiveShardMapper: ActiveShardMapper
  ): ExecPlan = {
    partialFailoverCounter.increment()
    // it makes sense to do local planning if we have at least 50% of shards running
    // as the query might overload the few shards we have while doing second level aggregation
    // Generally, it would be better to ship the entire query to the cluster that has more shards up
    // and let that cluster pull missing shards data from its buddy cluster. We don't want to implement
    // this feature though because hopefully in the near future we will have a class of stateless nodes to which
    // we can always assign non leaf plans
    val activeLocalShards = localActiveShardMapper.activeShards.size
    if (activeLocalShards < (localActiveShardMapper.shards.length /2) ) {
      throw new filodb.core.query.ServiceUnavailableException(
        s"$activeLocalShards  is not enough to finish the query in $clusterName, work unit $workUnit"
      )
    }
    val timeout : Long = queryConfig.remoteHttpTimeoutMs.getOrElse(60000)
    val haPlannerParams = qContext.plannerParams.copy(
      failoverMode = filodb.core.query.ShardLevelFailoverMode,
      buddyGrpcTimeoutMs = Some(timeout),
      buddyGrpcEndpoint = Some(remoteGrpcEndpoint.get)
    )
    val context = qContext.copy(plannerParams = haPlannerParams)
    logger.info(context.getQueryLogLine("Using shard level failover"))
    val plan = localPlanner.materialize(logicalPlan, context);
    plan
  }

  def getLocalActiveShardMapper(plannerParams: PlannerParams): filodb.core.query.ActiveShardMapper = {
    val shardMapper = localShardMapperFunc
    val shardInfo = filodb.core.query.ShardInfo(false, "")
    val shardInfoArray = Array.fill(shardMapper.statuses.size)(shardInfo)
    val localStatuses = shardMapper.statuses
    for (i <- localStatuses.indices) {
      val isActive = if (localStatuses(i) == filodb.coordinator.ShardStatusActive) {
        true
      } else {
        false
      }
      val actorRef = shardMapper.coordForShard(i)
      val path = if (actorRef == akka.actor.ActorRef.noSender) {
        ""
      } else {
        actorRef.path.toString
      }
      shardInfoArray(i) = filodb.core.query.ShardInfo(isActive, path)
    }
    val localActiveShardMapper = ActiveShardMapper(shardInfoArray)
    markDownShards(localActiveShardMapper, plannerParams.downPartitions, partitionName, workUnit, clusterName)
    localActiveShardMapper
  }

  def getRemoteActiveShardMapper(plannerParams: PlannerParams): filodb.core.query.ActiveShardMapper = {
    val shardMapper = buddyShardMapperProvider.get.getActiveShardMapper()
    markDownShards(shardMapper, plannerParams.downPartitions, partitionName, buddyWorkUnit, clusterName)
    shardMapper
  }

  def markDownShards(
      asm: ActiveShardMapper,
      downPartitions: scala.collection.mutable.Set[DownPartition],
      partitionName: String, workunitName: String, clusterType: String
  ): Unit = {
    val downPartition = downPartitions.find(
      dp => dp.name == partitionName
    )
    //val clusterType = localPlanner.asInstanceOf[SingleClusterPlanner].cl
    @scala.annotation.unused val downWorkUnit = downPartition.foreach(dp =>
      dp.downWorkUnits.find(dwu => dwu.name == workunitName).foreach(
        wu => wu.downClusters.find(dc => dc.clusterType == clusterType).foreach(
          dc => dc.downShards.foreach(
            s => asm.shards(s) = asm.shards(s).copy(active = false)
          )
        )
      )
    )
  }

  def materializeAllRemote(
    logicalPlan: LogicalPlan,
    qContext: QueryContext,
    localActiveShardMapper: ActiveShardMapper, remoteActiveShardMapper: ActiveShardMapper
  ): GenericRemoteExec = {
    fullFailoverCounter.increment()
    val timeout: Long = queryConfig.remoteHttpTimeoutMs.getOrElse(60000)
    val plannerParams = qContext.plannerParams.copy(
      failoverMode = filodb.core.query.ShardLevelFailoverMode,
      buddyGrpcTimeoutMs = Some(timeout),
      buddyGrpcEndpoint = Some(remoteGrpcEndpoint.get)
    )
    val c = qContext.copy(plannerParams = plannerParams)
    val ep = localPlanner.materialize(logicalPlan, c)
    val dispatcher = GrpcPlanDispatcher(remoteGrpcEndpoint.get, timeout)
    GenericRemoteExec(dispatcher, ep)
  }



}

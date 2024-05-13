package filodb.coordinator.queryplanner

import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.stub.{MetadataUtils, StreamObserver}
import java.net.InetAddress
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{MulticastStrategy, Observable}
import monix.reactive.subjects.ConcurrentSubject
import scala.collection.concurrent.{Map => ConcurrentMap}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import filodb.core.{DatasetRef, QueryTimeoutException}
import filodb.core.query.{PromQlQueryParams, QueryConfig, QueryContext}
import filodb.core.store.ChunkSource
import filodb.grpc.{GrpcCommonUtils, GrpcMultiPartitionQueryService, RemoteExecGrpc}
import filodb.query.{LabelNames, LabelValues, LogicalPlan, QueryResponse, SeriesKeysByFilters, StreamQueryResponse}
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

  case class GrpcPlanDispatcher(endpoint: String) extends PlanDispatcher {

    val clusterName = InetAddress.getLocalHost().getHostName()

    val requestTimeoutMs: Long = queryConfig.remoteHttpTimeoutMs.getOrElse(60000)

    def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)(implicit sched: Scheduler):
    Task[QueryResponse] = {
      // "source" is unused (the param exists to support InProcessDispatcher).
      val queryTimeElapsed = System.currentTimeMillis() - plan.execPlan.queryContext.submitTime
      val remainingTime = plan.clientParams.deadlineMs - queryTimeElapsed

      // Don't send if time left is very small
      if (remainingTime < 1) {
        Task.raiseError(QueryTimeoutException(queryTimeElapsed, this.getClass.getName))
      } else {
        dispatchExecutionPlan(plan, remainingTime, sched)
      }

    }

    // Even though this is NOT a streaming call, under the hood we use a streaming GRPC call
    // PromQLGrpcServer.executePlan
    // This is done so to unify executePlan() method with executeStreaming() method of PromQLGrpcServer.
    // Method exec() of PromQLGrpcServer is not used by MultiPartition and HighAvailabilityPlanner's. One can see,
    // for example, that method fromProto() of GrpcMultiPartitionQueryService.Response is never used
    // in the code.
    // executeStreaming() internally converts a non streaming QueryResponse to a GRPC streaming response and it's
    // executeStreaming() that we use for MultiPartition and HighAvailabilityPlanner's.
    // There are two reasons to use streaming GRPC API in a non streaming Scala API:
    // 1) since we use execStreaming() as opposed to exec(), we want to continue utilize the same code path
    // 2) we probably do not want to maintain both codepaths, ie we should deprecate exec()
    // 3) both streaming and non streaming Scala API calls can use streaming GRPC calls.
    def dispatchExecutionPlan(plan: ExecPlanWithClientParams, remainingTime: Long, sched: Scheduler):
    Task[QueryResponse] = {
      val t = Timeout(FiniteDuration(remainingTime, TimeUnit.MILLISECONDS))
      // We only dispatch the child of ExecPlan, we expect the type of plan to be dispatched
      // by GrpcPlanDispatcher is GenericRemoteExec
      val genericRemoteExec = plan.execPlan.asInstanceOf[GenericRemoteExec]
      import filodb.coordinator.ProtoConverters._
      val protoPlan = genericRemoteExec.execPlan.toExecPlanContainerProto

      val channel = channels.getOrElseUpdate(endpoint, GrpcCommonUtils.buildChannelFromEndpoint(endpoint))
      val observableResponse: Observable[GrpcMultiPartitionQueryService.StreamingResponse] = {
        val subject =
          ConcurrentSubject[GrpcMultiPartitionQueryService.StreamingResponse](MulticastStrategy.Publish)(sched)
        subject.doOnSubscribe(
          Task.eval {
            val nonBlockingStub = RemoteExecGrpc.newStub(channel)
            val md = new Metadata();
            plan.execPlan.queryContext.traceInfo.foreach {
              case (key, value) => md.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value)
            }
            nonBlockingStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
              .withDeadlineAfter(requestTimeoutMs, TimeUnit.MILLISECONDS)
              .executePlan(
                protoPlan,
                new StreamObserver[GrpcMultiPartitionQueryService.StreamingResponse] {
                  override def onNext(value: GrpcMultiPartitionQueryService.StreamingResponse): Unit =
                    subject.onNext(value)

                  override def onError(t: java.lang.Throwable): Unit = subject.onError(t)

                  override def onCompleted(): Unit = subject.onComplete()
                }
              )
          }
        )
      }
      import filodb.query.ProtoConverters._
      val taskOfList: Task[List[GrpcMultiPartitionQueryService.StreamingResponse]] = observableResponse.toListL
      taskOfList.map(_.toIterator.toQueryResponse)
    }

    // Currently, we do not use streaming for dispatching execution plans withing FiloDB clusters, even though
    // we do have call dispatchStreamingExecPlan that utilize Akka streaming.
    // It's an experimental feature and taking into account that Akka is not to be used in the newer versions of FiloDB,
    // ActorPlanDispatcher's streaming feature is not to be used.
    // GrpcPlanDispatcher is to be used exclusively for HighAvailability planner, as such it is NOT used for calls
    // withing a FiloDB cluster but for calls between FiloDB clusters. To do so, it utilizes PromQlGrpcServer.
    // PromQLGrpcServer currently is mostly a wrapper on top of ActorPlanDispatcher and although it defines method
    // execStreaming
    // it does not use queryPlanner.dispatchStreamingExecPlan and instead makes a call to dispatchExecPlan
    // PromQLGrpcServer gets normal non streaming QueryResponse from the FiloDB cluster and then converts it to
    // a GRPC streaming response but this is not to be confused with Akka streaming that theoretically can be used
    // withing FiloDB cluster.
    // When we start using GRPC dispatcher within cluster (ie replace ActorPlanDispatcher) and start using streaming
    // for these GRPC dispatchers, we need to implement this method. Currently, it does not make sense to implement
    // streaming in this dispatcher.

    def dispatchStreaming
    (plan: ExecPlanWithClientParams, source: ChunkSource)
    (implicit sched: Scheduler): Observable[StreamQueryResponse] = {
      ???
    }

    def isLocalCall: Boolean = false
  }
}

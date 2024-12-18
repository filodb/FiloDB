package filodb.coordinator

import com.typesafe.scalalogging.StrictLogging
import io.grpc.Metadata
import io.grpc.stub.{MetadataUtils, StreamObserver}
import java.net.InetAddress
import java.util.concurrent.TimeUnit
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.{MulticastStrategy, Observable}
import monix.reactive.subjects.ConcurrentSubject
import scala.concurrent.duration.FiniteDuration

import filodb.core.QueryTimeoutException
import filodb.core.store.ChunkSource
import filodb.grpc.GrpcCommonUtils
import filodb.grpc.GrpcMultiPartitionQueryService
import filodb.grpc.GrpcMultiPartitionQueryService.RemoteExecPlan
import filodb.grpc.RemoteExecGrpc
import filodb.query.{QueryResponse, StreamQueryResponse}
import filodb.query.exec.{ExecPlanWithClientParams, GenericRemoteExec, PlanDispatcher}

object GrpcPlanDispatcher {

  import io.grpc.ManagedChannel
  import scala.collection.concurrent.TrieMap

  val channelMap = new TrieMap[String, ManagedChannel]
  Runtime.getRuntime.addShutdownHook(new Thread(() => channelMap.values.foreach(_.shutdown())))
}

case class GrpcPlanDispatcher(endpoint: String, requestTimeoutMs: Long) extends PlanDispatcher with StrictLogging {

  val clusterName = InetAddress.getLocalHost().getHostName()

  override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)(implicit sched: Scheduler):
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
  // 2) we probably do not want to maintain both code paths, ie we should deprecate exec()
  // 3) both streaming and non streaming Scala API calls can use streaming GRPC calls.
  def dispatchExecutionPlan(plan: ExecPlanWithClientParams, remainingTime: Long, sched: Scheduler):
  Task[QueryResponse] = {
    val t = akka.util.Timeout(FiniteDuration(remainingTime, TimeUnit.MILLISECONDS))
    // We only dispatch the child of ExecPlan, we expect the type of plan to be dispatched
    // by GrpcPlanDispatcher is GenericRemoteExec
    val genericRemoteExec = plan.execPlan.asInstanceOf[GenericRemoteExec]
    import filodb.coordinator.ProtoConverters._
    val protoPlan = genericRemoteExec.execPlan.toExecPlanContainerProto
    logger.debug(s"Query ${plan.execPlan.queryContext.queryId} proto plan size is  ${protoPlan.toByteArray.length}B")
    logger.debug(s"Query ${plan.execPlan.queryContext.queryId} exec plan ${genericRemoteExec.execPlan.printTree()}")
    val queryContextProto = genericRemoteExec.execPlan.queryContext.toProto
    val remoteExecPlan : RemoteExecPlan = RemoteExecPlan.newBuilder()
      .setExecPlan(protoPlan)
      .setQueryContext(queryContextProto)
      .build()
    val channel =
      GrpcPlanDispatcher.channelMap.getOrElseUpdate(endpoint, GrpcCommonUtils.buildChannelFromEndpoint(endpoint))
    val observableResponse: monix.reactive.Observable[GrpcMultiPartitionQueryService.StreamingResponse] = {
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
              remoteExecPlan,
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


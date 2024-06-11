package filodb.coordinator

import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

import filodb.core.store.ChunkSource
import filodb.query.{QueryResponse, StreamQueryResponse}
import filodb.query.exec.{ExecPlanWithClientParams, PlanDispatcher}

// RemoteActorPlanDispatcher is different from ActorPlanDispatcher because at the moment
// of its instantiation it cannot have a valid ActorRef since it's referring to a machine
// that is not part of the local FiloDB cluster.
// However, when the dispatch call happens actor ref is created and normal ActorPlanDispatcher
// is instantiated and utilized to fulfill dispatch.
case class RemoteActorPlanDispatcher(path: String, clusterName: String) extends PlanDispatcher {

  override def dispatch(
     plan: ExecPlanWithClientParams, source: ChunkSource
  )(implicit sched: Scheduler): Task[QueryResponse] = {
    val serialization = akka.serialization.SerializationExtension(ActorSystemHolder.system)
    val deserializedActorRef = serialization.system.provider.resolveActorRef(path)
    val dispatcher = ActorPlanDispatcher(
      deserializedActorRef, clusterName
    )
    dispatcher.dispatch(plan, source)
  }

  def dispatchStreaming
  (plan: ExecPlanWithClientParams, source: ChunkSource)
  (implicit sched: Scheduler): Observable[StreamQueryResponse] = ???

  def isLocalCall: Boolean = false
}


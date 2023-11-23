package filodb.coordinator

import akka.actor.Props
import monix.reactive.MulticastStrategy
import monix.reactive.subjects.ConcurrentSubject

import filodb.coordinator.ActorSystemHolder.system
import filodb.query.Query.qLogger
import filodb.query.StreamQueryResponse

object ResultActor {
  def props(subject: ConcurrentSubject[StreamQueryResponse, StreamQueryResponse]): Props =
    Props(classOf[ResultActor], subject)
  lazy val subject = ConcurrentSubject[StreamQueryResponse](MulticastStrategy.Publish)(QueryScheduler.queryScheduler)
  lazy val resultActor = system.actorOf(Props(new ResultActor(subject)))
}

class ResultActor(subject: ConcurrentSubject[StreamQueryResponse, StreamQueryResponse]) extends BaseActor {

  def receive: Receive = {
    case q: StreamQueryResponse =>
      try {
        subject.onNext(q)
        qLogger.debug(s"Got ${q.getClass.getSimpleName} for plan ${q.planId}")
      } catch {
        case e: Throwable =>
          qLogger.error(s"Exception when processing $q", e)
      }
    case msg =>
      qLogger.error(s"Unexpected message $msg in ResultActor from sender ${sender()}")
  }
}

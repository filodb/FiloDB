package filodb.coordinator

import scala.concurrent.duration._

import akka.actor.Props
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.reactive.subjects.ConcurrentSubject
import monix.reactive.{MulticastStrategy, Observable, Pipe}

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


object MyApp extends App with StrictLogging {

  import monix.execution.Scheduler.Implicits.global

  val obs1 = Observable.interval(1.second)
    .doOnSubscribe(Task.eval {
      println("subscribed")
    })
    .pipeThrough(Pipe.publish[Long])


  val first = obs1.dump("obs1").firstL

  val obs3 = Observable.fromTask(first.map { first =>
    obs1.dump("obs2")
  }).flatten

  obs3.dump("obs3").take(10).countL.runToFuture

  Thread.sleep(20000)
}
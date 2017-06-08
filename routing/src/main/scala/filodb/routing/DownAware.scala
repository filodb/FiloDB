/**
* Copyright (c) 2017 Apple, Inc. All rights reserved.
*/
package filodb.routing

import akka.actor.{ActorRef, PoisonPill}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}

trait DownAware extends KafkaActor {
  import akka.pattern.{gracefulStop, pipe}
  import context.dispatcher

  protected def timeout: FiniteDuration

  protected def gracefulShutdown(sender: ActorRef): Unit = {

    log.info("Starting graceful shutdown on node.")

    val future = try
      Future.sequence(context.children.map { child =>
        gracefulStop(self, timeout, PoisonPill)
      })
    catch {
      case e: akka.pattern.AskTimeoutException =>
        log.warning("Error shutting down all workers", e)
        Promise.failed(e).future
    }

    scala.concurrent.Await.result(future, timeout)
    future pipeTo sender
    ()
  }
}

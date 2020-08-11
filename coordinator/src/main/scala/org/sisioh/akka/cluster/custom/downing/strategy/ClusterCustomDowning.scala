package org.sisioh.akka.cluster.custom.downing.strategy

import akka.actor.{ ActorLogging, Address, Scheduler }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterDomainEvent
import scala.concurrent.duration._

trait ClusterCustomDowning extends ActorLogging { base: CustomAutoDownBase =>

  protected val cluster: Cluster = Cluster(context.system)

  override protected def selfAddress: Address = cluster.selfAddress

  override protected def scheduler: Scheduler = {
    if (context.system.scheduler.maxFrequency < 1.second / cluster.settings.SchedulerTickDuration) {
      log.warning(
        "CustomDowning does not use a cluster dedicated scheduler. " +
         "Cluster will use a dedicated scheduler if configured " +
        "with 'akka.scheduler.tick-duration' [{} ms] >  'akka.cluster.scheduler.tick-duration' [{} ms].",
        (1000 / context.system.scheduler.maxFrequency).toInt,
        cluster.settings.SchedulerTickDuration.toMillis
      )
    }
    context.system.scheduler
  }

  override def preStart(): Unit =
    cluster.subscribe(self, classOf[ClusterDomainEvent])

  override def postStop(): Unit =
    cluster.unsubscribe(self)

}

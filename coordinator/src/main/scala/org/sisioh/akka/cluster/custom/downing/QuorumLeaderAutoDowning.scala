/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing

import akka.actor.{ ActorSystem, Address, Props }
import akka.cluster.DowningProvider
import com.typesafe.config.Config
import org.sisioh.akka.cluster.custom.downing.strategy.ClusterCustomDowning
import org.sisioh.akka.cluster.custom.downing.strategy.quorumLeader.QuorumLeaderAutoDownBase

import scala.concurrent.Await
import scala.concurrent.duration._

final class QuorumLeaderAutoDowning(system: ActorSystem) extends DowningProvider {

  private val config: Config = system.settings.config

  override def downRemovalMargin: FiniteDuration = {
    val key = "custom-downing.down-removal-margin"
    config.getString(key) match {
      case "off" => Duration.Zero
      case _     => Duration(config.getDuration(key, MILLISECONDS), MILLISECONDS)
    }
  }

  override def downingActorProps: Option[Props] = {
    val stableAfter = system.settings.config.getDuration("custom-downing.stable-after").toMillis.millis
    val role = {
      val r = system.settings.config.getString("custom-downing.quorum-leader-auto-downing.role")
      if (r.isEmpty) None else Some(r)
    }
    val quorumSize = system.settings.config.getInt("custom-downing.quorum-leader-auto-downing.quorum-size")
    val downIfOutOfQuorum =
      system.settings.config.getBoolean("custom-downing.quorum-leader-auto-downing.down-if-out-of-quorum")
    val shutdownActorSystem =
      system.settings.config.getBoolean("custom-downing.quorum-leader-auto-downing.shutdown-actor-system-on-resolution")
    Some(QuorumLeaderAutoDown.props(role, quorumSize, downIfOutOfQuorum, shutdownActorSystem, stableAfter))
  }
}

private[downing] object QuorumLeaderAutoDown {

  def props(
      quorumRole: Option[String],
      quorumSize: Int,
      downIfOutOfQuorum: Boolean,
      shutdownActorSystem: Boolean,
      autoDownUnreachableAfter: FiniteDuration
  ): Props =
    Props(
      new QuorumLeaderAutoDown(quorumRole, quorumSize, downIfOutOfQuorum, shutdownActorSystem, autoDownUnreachableAfter)
    )

}

private[downing] class QuorumLeaderAutoDown(
    quorumRole: Option[String],
    quorumSize: Int,
    downIfOutOfQuorum: Boolean,
    shutdownActorSystem: Boolean,
    autoDownUnreachableAfter: FiniteDuration
) extends QuorumLeaderAutoDownBase(quorumRole, quorumSize, downIfOutOfQuorum, autoDownUnreachableAfter)
    with ClusterCustomDowning {

  override protected def down(node: Address): Unit = {
    log.info("Quorum leader is auto-downing unreachable node [{}]", node)
    cluster.down(node)
  }

  override protected def shutdownSelf(): Unit = {
    if (shutdownActorSystem) {
      Await.result(context.system.terminate(), 10.seconds)
    } else {
      throw new SplitBrainResolvedError("QuorumLeaderAutoDowning")
    }
  }
}

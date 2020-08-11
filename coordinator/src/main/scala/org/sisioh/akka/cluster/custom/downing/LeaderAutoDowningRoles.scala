/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing

import akka.actor.{ ActorSystem, Address, Props }
import akka.cluster.DowningProvider
import com.typesafe.config.Config
import org.sisioh.akka.cluster.custom.downing.strategy.ClusterCustomDowning
import org.sisioh.akka.cluster.custom.downing.strategy.leaderRoles.LeaderAutoDownRolesBase

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{ FiniteDuration, _ }

final class LeaderAutoDowningRoles(system: ActorSystem) extends DowningProvider {

  private val config: Config = system.settings.config

  override def downRemovalMargin: FiniteDuration = {
    val key = "custom-downing.down-removal-margin"
    config.getString(key) match {
      case "off" => Duration.Zero
      case _     => Duration(config.getDuration(key, MILLISECONDS), MILLISECONDS)
    }
  }

  override def downingActorProps: Option[Props] = {
    val stableAfter = config.getDuration("custom-downing.stable-after").toMillis.millis
    val roles       = config.getStringList("custom-downing.leader-auto-downing-roles.target-roles").asScala.toSet
    if (roles.isEmpty) None else Some(LeaderAutoDownRoles.props(roles, stableAfter))
  }
}

private[downing] object LeaderAutoDownRoles {

  def props(targetRoles: Set[String], autoDownUnreachableAfter: FiniteDuration): Props =
    Props(new LeaderAutoDownRoles(targetRoles, autoDownUnreachableAfter))

}

private[downing] class LeaderAutoDownRoles(targetRoles: Set[String], autoDownUnreachableAfter: FiniteDuration)
    extends LeaderAutoDownRolesBase(targetRoles, autoDownUnreachableAfter)
    with ClusterCustomDowning {

  override protected def down(node: Address): Unit = {
    log.info("Leader is auto-downing unreachable node [{}]", node)
    cluster.down(node)
  }
}

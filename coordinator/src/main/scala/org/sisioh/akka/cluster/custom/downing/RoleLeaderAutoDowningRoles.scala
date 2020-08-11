/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing

import akka.actor.{ ActorSystem, Address, Props }
import akka.cluster.DowningProvider
import com.typesafe.config.Config
import org.sisioh.akka.cluster.custom.downing.strategy.ClusterCustomDowning
import org.sisioh.akka.cluster.custom.downing.strategy.roleLeaderRoles.RoleLeaderAutoDownRolesBase

import scala.jdk.CollectionConverters._
import scala.concurrent.duration.{ FiniteDuration, _ }

final class RoleLeaderAutoDowningRoles(system: ActorSystem) extends DowningProvider {

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
    val leaderRole  = config.getString("custom-downing.role-leader-auto-downing-roles.leader-role")
    val roles       = config.getStringList("custom-downing.role-leader-auto-downing-roles.target-roles").asScala.toSet
    if (roles.isEmpty) None else Some(RoleLeaderAutoDownRoles.props(leaderRole, roles, stableAfter))
  }
}

private[downing] object RoleLeaderAutoDownRoles {

  def props(leaderRole: String, targetRoles: Set[String], autoDownUnreachableAfter: FiniteDuration): Props =
    Props(new RoleLeaderAutoDownRoles(leaderRole, targetRoles, autoDownUnreachableAfter))
}

private[downing] class RoleLeaderAutoDownRoles(
    leaderRole: String,
    targetRoles: Set[String],
    autoDownUnreachableAfter: FiniteDuration
) extends RoleLeaderAutoDownRolesBase(leaderRole, targetRoles, autoDownUnreachableAfter)
    with ClusterCustomDowning {

  override protected def down(node: Address): Unit = {
    log.info("RoleLeader is auto-downing unreachable node [{}]", node)
    cluster.down(node)
  }
}

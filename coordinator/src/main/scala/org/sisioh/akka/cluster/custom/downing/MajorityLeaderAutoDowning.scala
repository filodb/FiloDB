/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing

import akka.actor.{ ActorSystem, Address, Props }
import akka.cluster.DowningProvider
import com.typesafe.config.Config
import org.sisioh.akka.cluster.custom.downing.strategy.ClusterCustomDowning
import org.sisioh.akka.cluster.custom.downing.strategy.majorityLeader.MajorityLeaderAutoDownBase

import scala.concurrent.Await
import scala.concurrent.duration._

final class MajorityLeaderAutoDowning(system: ActorSystem) extends DowningProvider {

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
    val majorityMemberRole = {
      val r = config.getString("custom-downing.majority-leader-auto-downing.majority-member-role")
      if (r.isEmpty) None else Some(r)
    }
    val downIfInMinority = config.getBoolean("custom-downing.majority-leader-auto-downing.down-if-in-minority")
    val shutdownActorSystem =
      config.getBoolean("custom-downing.majority-leader-auto-downing.shutdown-actor-system-on-resolution")
    Some(MajorityLeaderAutoDown.props(majorityMemberRole, downIfInMinority, shutdownActorSystem, stableAfter))
  }

}

private[downing] object MajorityLeaderAutoDown {

  def props(
      majorityMemberRole: Option[String],
      downIfInMinority: Boolean,
      shutdownActorSystem: Boolean,
      autoDownUnreachableAfter: FiniteDuration
  ): Props =
    Props(
      new MajorityLeaderAutoDown(majorityMemberRole, downIfInMinority, shutdownActorSystem, autoDownUnreachableAfter)
    )
}

private[downing] class MajorityLeaderAutoDown(
    majorityMemberRole: Option[String],
    downIfInMinority: Boolean,
    shutdownActorSystem: Boolean,
    autoDownUnreachableAfter: FiniteDuration
) extends MajorityLeaderAutoDownBase(majorityMemberRole, downIfInMinority, autoDownUnreachableAfter)
    with ClusterCustomDowning {

  override protected def down(node: Address): Unit = {
    log.info("Majority is auto-downing unreachable node [{}]", node)
    cluster.down(node)
  }

  override protected def shutdownSelf(): Unit = {
    if (shutdownActorSystem) {
      Await.result(context.system.terminate(), 10.seconds)
    } else {
      throw new SplitBrainResolvedError("MajorityAutoDowning")
    }
  }
}

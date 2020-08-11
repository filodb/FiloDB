/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing.strategy.majorityLeader

import akka.actor.Address
import akka.cluster.MemberStatus.Down
import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.strategy.Members

import scala.concurrent.duration.FiniteDuration

abstract class MajorityLeaderAutoDownBase(
    majorityMemberRole: Option[String],
    downIfInMinority: Boolean,
    autoDownUnreachableAfter: FiniteDuration
) extends MajorityAwareCustomAutoDownBase(autoDownUnreachableAfter) {

  override protected def onLeaderChanged(leader: Option[Address]): Unit = {
    log.debug("onLeaderChanged: majorityMemberRole = {}, isLeader = {}", majorityMemberRole, isLeader)
    if (majorityMemberRole.isEmpty && isLeader) {
      log.debug("onLeaderChanged: down PendingUnreachableMembers")
      downPendingUnreachableMembers()
    }
  }

  override protected def onRoleLeaderChanged(role: String, leader: Option[Address]): Unit = {
    majorityMemberRole.foreach { r =>
      log.debug("onRoleLeaderChanged: r = {}, role = {}, isRoleLoaderOf(r) = {}", r, role, isRoleLeaderOf(r))
      if (r == role && isRoleLeaderOf(r)) {
        log.debug("onRoleLeaderChanged: down PendingUnreachableMembers")
        downPendingUnreachableMembers()
      }
    }
  }

  override protected def onMemberDowned(member: Member): Unit = {
    log.debug(
      "onMemberDowned: isMajority(majorityMemberRole) = {}, isLeaderOf(majorityMemberRole) = {}",
      isMajority(majorityMemberRole),
      isLeaderOf(majorityMemberRole)
    )
    if (isMajority(majorityMemberRole)) {
      if (isLeaderOf(majorityMemberRole)) {
        log.debug("onMemberDowned: down PendingUnreachableMembers")
        downPendingUnreachableMembers()
      }
    } else {
      log.debug("onMemberDowned: down({}) ", selfAddress)
      down(selfAddress)
    }
  }

  override protected def onMemberRemoved(member: Member, previousStatus: MemberStatus): Unit = {
    log.debug(
      "onMemberRemoved: isMajority(majorityMemberRole) = {}, isLeaderOf(majorityMemberRole) = {}",
      isMajority(majorityMemberRole),
      isLeaderOf(majorityMemberRole)
    )
    if (isMajority(majorityMemberRole)) {
      if (isLeaderOf(majorityMemberRole)) {
        log.debug("onMemberRemoved: down PendingUnreachableMembers")
        downPendingUnreachableMembers()
      }
    } else {
      log.debug("onMemberDowned: down({}) ", selfAddress)
      down(selfAddress)
    }
  }

  override protected def downOrAddPending(member: Member): Unit =
    if (isLeaderOf(majorityMemberRole)) {
      log.debug("downOrAddPending: down({})", member.address)
      down(member.address)
      replaceMember(member.copy(Down))
    } else
      addPendingUnreachableMember(member)

  override protected def downOrAddPendingAll(members: Members): Unit = {
    if (isMajorityAfterDown(members, majorityMemberRole)) {
      members.foreach(downOrAddPending)
    } else if (downIfInMinority) {
      shutdownSelf()
    }
  }
}

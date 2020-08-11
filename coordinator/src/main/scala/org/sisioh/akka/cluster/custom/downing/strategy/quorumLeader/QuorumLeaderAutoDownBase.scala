/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing.strategy.quorumLeader

import akka.actor.Address
import akka.cluster.MemberStatus.Down
import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.strategy.Members

import scala.concurrent.duration.FiniteDuration

abstract class QuorumLeaderAutoDownBase(
    quorumRole: Option[String],
    quorumSize: Int,
    downIfOutOfQuorum: Boolean,
    autoDownUnreachableAfter: FiniteDuration
) extends QuorumAwareCustomAutoDownBase(quorumSize, autoDownUnreachableAfter) {

  override protected def onLeaderChanged(leader: Option[Address]): Unit = {
    log.debug("onLeaderChanged: quorumRole = {}, isLeader = {}", quorumRole, isLeader)
    if (quorumRole.isEmpty && isLeader) {
      log.debug("onLeaderChanged: down PendingUnreachableMembers")
      downPendingUnreachableMembers()
    }
  }

  override protected def onRoleLeaderChanged(role: String, leader: Option[Address]): Unit = {
    quorumRole.foreach { r =>
      log.debug("onRoleLeaderChanged: r = {}, role = {}, isRoleLoaderOf(r) = {}", r, role, isRoleLeaderOf(r))
      if (r == role && isRoleLeaderOf(r)) {
        log.debug("onRoleLeaderChanged: down PendingUnreachableMembers")
        downPendingUnreachableMembers()
      }
    }
  }

  override protected def onMemberDowned(member: Member): Unit = {
    log.debug(
      "onMemberDowned: isQuorumMet(quorumRole) = {}, isLeaderOf(quorumRole) = {}",
      isQuorumMet(quorumRole),
      isLeaderOf(quorumRole)
    )
    if (isQuorumMet(quorumRole)) {
      if (isLeaderOf(quorumRole)) {
        log.debug("onMemberDowned: down PendingUnreachableMembers")
        downPendingUnreachableMembers()
      }
    } else {
      log.debug("onMemberRemoved: down({})", selfAddress)
      down(selfAddress)
    }
  }

  override protected def onMemberRemoved(member: Member, previousStatus: MemberStatus): Unit = {
    log.debug(
      "onMemberDowned: isQuorumMet(quorumRole) = {}, isLeaderOf(quorumRole) = {}",
      isQuorumMet(quorumRole),
      isLeaderOf(quorumRole)
    )
    if (isQuorumMet(quorumRole)) {
      if (isLeaderOf(quorumRole)) {
        log.debug("onMemberRemoved: down PendingUnreachableMembers")
        downPendingUnreachableMembers()
      }
    } else {
      log.debug("onMemberRemoved: down({})", selfAddress)
      down(selfAddress)
    }
  }

  override protected def downOrAddPending(member: Member): Unit = {
    if (isLeaderOf(quorumRole)) {
      down(member.address)
      replaceMember(member.copy(Down))
    } else {
      addPendingUnreachableMember(member)
    }
  }

  override protected def downOrAddPendingAll(members: Members): Unit = {
    if (isQuorumMetAfterDown(members, quorumRole)) {
      members.foreach(downOrAddPending)
    } else if (downIfOutOfQuorum) {
      shutdownSelf()
    }
  }
}

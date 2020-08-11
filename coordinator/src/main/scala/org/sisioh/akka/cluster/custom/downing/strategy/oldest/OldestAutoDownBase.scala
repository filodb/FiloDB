/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing.strategy.oldest

import akka.cluster.MemberStatus.Down
import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.strategy.Members

import scala.concurrent.duration.FiniteDuration

abstract class OldestAutoDownBase(
    oldestMemberRole: Option[String],
    downIfAlone: Boolean,
    autoDownUnreachableAfter: FiniteDuration
) extends OldestAwareCustomAutoDownBase(autoDownUnreachableAfter) {

  override protected def onMemberDowned(member: Member): Unit = {
    if (isOldestOf(oldestMemberRole, member))
      downPendingUnreachableMembers()
  }

  override protected def onMemberRemoved(member: Member, previousStatus: MemberStatus): Unit = {
    if (isOldestOf(oldestMemberRole))
      downPendingUnreachableMembers()
  }

  override protected def downOrAddPending(member: Member): Unit = {
    if (isOldestOf(oldestMemberRole)) {
      down(member.address)
    } else
      addPendingUnreachableMember(member)
  }

  private def downOnSecondary(member: Member): Unit = {
    if (isSecondaryOldest(oldestMemberRole)) {
      down(member.address)
      replaceMember(member.copy(Down))
    }
  }

  override protected def downOrAddPendingAll(members: Members): Unit = {
    val oldest = oldestMember(oldestMemberRole)
    if (downIfAlone && isOldestAlone(oldestMemberRole)) {
      if (isOldestOf(oldestMemberRole))
        shutdownSelf()
      else if (isSecondaryOldest(oldestMemberRole))
        members.foreach(downOnSecondary)
      else
        members.foreach(downOrAddPending)
    } else {
      if (oldest.fold(true)(members.contains))
        shutdownSelf()
      else
        members.foreach(downOrAddPending)
    }
  }

  protected def downAloneOldest(member: Member): Unit = {
    val oldest = oldestMember(oldestMemberRole)
    if (isOldestOf(oldestMemberRole)) {
      shutdownSelf()
    } else if (isSecondaryOldest(oldestMemberRole) && oldest.contains(member)) {
      oldest.foreach { m =>
        down(m.address)
        replaceMember(m.copy(Down))
      }
    } else
      addPendingUnreachableMember(member)
  }
}

/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing.strategy.oldest

import akka.cluster.ClusterEvent._
import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.SplitBrainResolver
import org.sisioh.akka.cluster.custom.downing.strategy.CustomAutoDownBase

import scala.concurrent.duration.FiniteDuration

abstract class OldestAwareCustomAutoDownBase(autoDownUnreachableAfter: FiniteDuration)
    extends CustomAutoDownBase(autoDownUnreachableAfter)
    with SplitBrainResolver {

  private var membersByAge: SortedMembersByOldest = SortedMembersByOldest.empty

  override protected def receiveEvent: Receive = {
    case MemberUp(m) =>
      log.info("{} is up", m)
      replaceMember(m)
    case UnreachableMember(m) =>
      log.info("{} is unreachable", m)
      replaceMember(m)
      addUnreachableMember(m)
    case ReachableMember(m) =>
      log.info("{} is reachable", m)
      replaceMember(m)
      removeUnreachableMember(m)
    case MemberLeft(m) =>
      log.info("{} is left the cluster", m)
      replaceMember(m)
    case MemberExited(m) =>
      log.info("{} exited the cluster", m)
      replaceMember(m)
    case MemberDowned(m) =>
      log.info("{} was downed", m)
      replaceMember(m)
      onMemberDowned(m)
    case MemberRemoved(m, prev) =>
      log.info("{} was removed from the cluster", m)
      removeUnreachableMember(m)
      removeMember(m)
      onMemberRemoved(m, prev)
  }

  override protected def initialize(state: CurrentClusterState): Unit = {
    log.debug("initialize: {}", state)
    membersByAge = SortedMembersByOldest(state.members)
  }

  protected def replaceMember(member: Member): Unit = {
    membersByAge = membersByAge.replace(member)
  }

  protected def removeMember(member: Member): Unit = {
    membersByAge -= member
  }

  protected def isOldest: Boolean = {
    membersByAge.isOldest(selfAddress)
  }

  protected def isOldestWithout(member: Member): Boolean = {
    membersByAge.isOldestWithout(selfAddress, member)
  }

  protected def isOldestOf(role: Option[String]): Boolean = {
    membersByAge.isOldestOf(selfAddress, role)
  }

  protected def isOldestOf(role: Option[String], without: Member): Boolean = {
    membersByAge.isOldestOf(selfAddress, role)
  }

  protected def isOldestAlone(role: Option[String]): Boolean = {
    membersByAge.isOldestAlone(selfAddress, role, isOK, isKO)
  }

  protected def isSecondaryOldest(role: Option[String]): Boolean = {
    membersByAge.isSecondaryOldest(selfAddress, role)
  }

  protected def oldestMember(role: Option[String]): Option[Member] =
    membersByAge.oldestMember(role)

  private def isOK(member: Member) = {
    (member.status == MemberStatus.Up ||
    member.status == MemberStatus.Leaving) &&
    (!pendingUnreachableMembers.contains(member) &&
    !unstableUnreachableMembers.contains(member))
  }

  private def isKO(member: Member): Boolean = !isOK(member)
}

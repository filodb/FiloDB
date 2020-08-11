/**
  * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
  *
  * 2016- Modified by Yusuke Yasuda
  * 2019- Modified by Junichi Kato
  * The original source code can be found here.
  * https://github.com/akka/akka/blob/master/akka-cluster/src/main/scala/akka/cluster/AutoDown.scala
  */
package org.sisioh.akka.cluster.custom.downing.strategy.majorityLeader

import akka.cluster.ClusterEvent._
import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.SplitBrainResolver
import org.sisioh.akka.cluster.custom.downing.strategy.{ CustomAutoDownBase, Members }

import scala.concurrent.duration.FiniteDuration

abstract class MajorityAwareCustomAutoDownBase(autoDownUnreachableAfter: FiniteDuration)
    extends CustomAutoDownBase(autoDownUnreachableAfter)
    with SplitBrainResolver {

  private var leader: Boolean                           = false
  private var roleLeader: Map[String, Boolean]          = Map.empty
  private var membersByAddress: SortedMembersByMajority = SortedMembersByMajority.empty

  override protected def receiveEvent: Receive = {
    case LeaderChanged(leaderOption) =>
      leader = leaderOption.contains(selfAddress)
      if (isLeader) {
        log.info("This node is the new Leader")
      }
      onLeaderChanged(leaderOption)
    case RoleLeaderChanged(role, leaderOption) =>
      roleLeader += (role -> leaderOption.contains(selfAddress))
      if (isRoleLeaderOf(role)) {
        log.info("This node is the new role leader for role {}", role)
      }
      onRoleLeaderChanged(role, leaderOption)
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
      log.info("{} left the cluster", m)
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

  protected def isLeader: Boolean = leader

  protected def isRoleLeaderOf(role: String): Boolean = roleLeader.getOrElse(role, false)

  override protected def initialize(state: CurrentClusterState): Unit = {
    leader = state.leader.contains(selfAddress)
    roleLeader = state.roleLeaderMap.mapValues(_.exists(_ == selfAddress)).toMap
    membersByAddress = SortedMembersByMajority(state.members)
  }

  protected def replaceMember(member: Member): Unit = {
    membersByAddress = membersByAddress.replace(member)
  }

  protected def removeMember(member: Member): Unit = {
    membersByAddress -= member
  }

  protected def isLeaderOf(majorityRole: Option[String]): Boolean = majorityRole.fold(isLeader)(isRoleLeaderOf)

  protected def majorityMemberOf(role: Option[String]): SortedMembersByMajority = {
    membersByAddress.majorityMemberOf(role)
  }

  protected def isMajority(role: Option[String]): Boolean = {
    membersByAddress.isMajority(role, isOK)
  }

  protected def isMajorityAfterDown(members: Members, role: Option[String]): Boolean = {
    membersByAddress.isMajorityAfterDown(members, role, isOK)
  }

  private def isOK(member: Member) = {
    (member.status == MemberStatus.Up ||
    member.status == MemberStatus.Leaving) &&
    (!pendingUnreachableMembers.contains(member) &&
    !unstableUnreachableMembers.contains(member))
  }

  private def isKO(member: Member): Boolean = !isOK(member)
}

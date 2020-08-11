/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing.strategy.quorumLeader

import akka.cluster.ClusterEvent._
import akka.cluster.Member
import org.sisioh.akka.cluster.custom.downing.SplitBrainResolver
import org.sisioh.akka.cluster.custom.downing.strategy.{ CustomAutoDownBase, Members }

import scala.concurrent.duration.FiniteDuration

abstract class QuorumAwareCustomAutoDownBase(quorumSize: Int, autoDownUnreachableAfter: FiniteDuration)
    extends CustomAutoDownBase(autoDownUnreachableAfter)
    with SplitBrainResolver {

  private var leader: Boolean                  = false
  private var roleLeader: Map[String, Boolean] = Map.empty

  private var membersByAge: SortedMembersByQuorum = SortedMembersByQuorum.empty

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
      log.info("{} exited from the cluster", m)
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
    log.debug("initialize: {}", state)
    leader = state.leader.contains(selfAddress)
    roleLeader = state.roleLeaderMap.mapValues(_.exists(_ == selfAddress)).toMap
    membersByAge = SortedMembersByQuorum(state.members)
  }

  protected def replaceMember(member: Member): Unit = {
    membersByAge = membersByAge.replace(member)
  }

  protected def removeMember(member: Member): Unit = {
    membersByAge -= member
  }

  protected def isLeaderOf(quorumRole: Option[String]): Boolean = quorumRole.fold(isLeader)(isRoleLeaderOf)

  private def noContainsPendingUnreachableMembers(m: Member): Boolean = !pendingUnreachableMembers.contains(m)

  protected def isQuorumMet(role: Option[String]): Boolean = {
    membersByAge.isQuorumMet(noContainsPendingUnreachableMembers, quorumSize, role)
  }

  protected def isQuorumMetAfterDown(members: Members, role: Option[String]): Boolean = {
    membersByAge.isQuorumMetAfterDown(noContainsPendingUnreachableMembers, quorumSize, members, role)
  }

  protected def isUnreachableStable: Boolean = scheduledUnreachableMembers.isEmpty

}

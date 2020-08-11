/**
  * Copyright (C) 2016- Yuske Yasuda
  * Copyright (C) 2019- SISIOH Project
  */
package org.sisioh.akka.cluster.custom.downing.strategy.leaderRoles

import akka.cluster.ClusterEvent._
import org.sisioh.akka.cluster.custom.downing.strategy.CustomAutoDownBase

import scala.concurrent.duration.FiniteDuration

abstract class LeaderAwareCustomAutoDownBase(autoDownUnreachableAfter: FiniteDuration)
    extends CustomAutoDownBase(autoDownUnreachableAfter) {

  private var leader: Boolean = false

  protected def isLeader: Boolean = leader

  override protected def receiveEvent: Receive = {
    case LeaderChanged(leaderOption) =>
      leader = leaderOption.contains(selfAddress)
      if (isLeader) {
        log.info("This node is the new Leader")
      }
      onLeaderChanged(leaderOption)
    case UnreachableMember(m) =>
      log.info("{} is unreachable", m)
      addUnreachableMember(m)
    case ReachableMember(m) =>
      log.info("{} is reachable", m)
      removeUnreachableMember(m)
    case MemberDowned(m) =>
      log.info("{} was downed", m)
      onMemberDowned(m)
    case MemberRemoved(m, s) =>
      log.info("{} was removed from the cluster", m)
      removeUnreachableMember(m)
      onMemberRemoved(m, s)
  }

  override protected def initialize(state: CurrentClusterState): Unit = {
    log.debug("initialize: {}", state)
    leader = state.leader.contains(selfAddress)
  }
}

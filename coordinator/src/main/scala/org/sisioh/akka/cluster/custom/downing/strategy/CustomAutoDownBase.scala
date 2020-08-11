/**
  * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
  *
  * 2016- Modified by Yusuke Yasuda
  * 2019- Modified by Junichi Kato
  * The original source code can be found here.
  * https://github.com/akka/akka/blob/master/akka-cluster/src/main/scala/akka/cluster/AutoDown.scala
  */
package org.sisioh.akka.cluster.custom.downing.strategy

import akka.actor.{ Actor, Address, Scheduler }
import akka.cluster.ClusterEvent._
import akka.cluster.MemberStatus.{ Down, Exiting }
import akka.cluster._
import akka.event.LoggingAdapter

import scala.concurrent.duration.{ Duration, FiniteDuration }

object CustomDowning {
  private[downing] case class UnreachableTimeout(member: Member)
  private[downing] val skipMemberStatus: Set[MemberStatus] = Set[MemberStatus](Down, Exiting)
}

abstract class CustomAutoDownBase(autoDownUnreachableAfter: FiniteDuration) extends Actor {
  autoDownUnreachableAfter.isFinite
  import CustomDowning._

  def log: LoggingAdapter

  protected def selfAddress: Address

  protected def down(node: Address): Unit

  protected def downOrAddPending(member: Member): Unit

  protected def downOrAddPendingAll(members: Members): Unit

  protected def scheduler: Scheduler

  import context.dispatcher

  private var _scheduledUnreachable: MemberCancellables = MemberCancellables.empty
  private var _pendingUnreachableMembers: Members       = Members.empty
  private var _unstableUnreachableMembers: Members      = Members.empty

  protected def scheduledUnreachableMembers: MemberCancellables = _scheduledUnreachable
  protected def pendingUnreachableMembers: Members              = _pendingUnreachableMembers
  protected def unstableUnreachableMembers: Members             = _unstableUnreachableMembers

  override def postStop(): Unit = {
    _scheduledUnreachable.cancelAll()
    super.postStop()
  }

  override def receive: Receive = receiveEvent orElse predefinedReceiveEvent

  protected def receiveEvent: Receive

  private def predefinedReceiveEvent: Receive = {
    case state: CurrentClusterState =>
      initialize(state)
      state.unreachable foreach addUnreachableMember
    case UnreachableTimeout(member) =>
      if (_scheduledUnreachable contains member) {
        _scheduledUnreachable -= member
        if (_scheduledUnreachable.isEmpty) {
          _unstableUnreachableMembers += member
          downOrAddPendingAll(_unstableUnreachableMembers)
          _unstableUnreachableMembers = Members.empty
        } else {
          _unstableUnreachableMembers += member
        }
      }
    case _: ClusterDomainEvent =>
  }

  protected def initialize(state: CurrentClusterState): Unit = {}

  protected def onMemberDowned(member: Member): Unit = {}

  protected def onMemberRemoved(member: Member, previousStatus: MemberStatus): Unit = {}

  protected def onLeaderChanged(leader: Option[Address]): Unit = {}

  protected def onRoleLeaderChanged(role: String, leader: Option[Address]): Unit = {}

  protected def addUnreachableMember(member: Member): Unit =
    if (!skipMemberStatus(member.status) && !_scheduledUnreachable.contains(member)) {
      if (autoDownUnreachableAfter == Duration.Zero)
        downOrAddPending(member)
      else {
        val task = scheduler.scheduleOnce(autoDownUnreachableAfter, self, UnreachableTimeout(member))
        _scheduledUnreachable += (member -> task)
      }
    }

  protected def removeUnreachableMember(member: Member): Unit = {
    _scheduledUnreachable.cancel(member)
    _scheduledUnreachable -= member
    _pendingUnreachableMembers -= member
    _unstableUnreachableMembers -= member
  }

  protected def addPendingUnreachableMember(member: Member): Unit = _pendingUnreachableMembers += member

  protected def downPendingUnreachableMembers(): Unit = {
    val (head, tail) = _pendingUnreachableMembers.splitHeadAndTail
    head.foreach { member =>
      log.debug("downPendingUnreachableMembers: down({})", member.address)
      down(member.address)
    }
    _pendingUnreachableMembers = tail
  }
}

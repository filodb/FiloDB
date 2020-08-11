package org.sisioh.akka.cluster.custom.downing.strategy.oldest

import akka.actor.Address
import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.strategy.SortedMembers

import scala.collection.immutable

class SortedMembersByOldest(values: immutable.SortedSet[Member]) extends SortedMembers(values, Member.ageOrdering) {
  override type This = SortedMembersByOldest

  override protected def createInstance(values: immutable.SortedSet[Member]): SortedMembersByOldest =
    new SortedMembersByOldest(values)

  private def isAllIntermediateMemberRemoved(member: Member): Boolean = {
    val isUnsafe = filterNot(_ == member).exists { m =>
      m.status == MemberStatus.Down || m.status == MemberStatus.Exiting
    }
    !isUnsafe
  }

  private def isAllIntermediateMemberRemoved: Boolean = {
    val isUnsafe = exists { m =>
      m.status == MemberStatus.Down || m.status == MemberStatus.Exiting
    }
    !isUnsafe
  }

  private def targetMembers(role: Option[String]): immutable.SortedSet[Member] = {
    role.fold(values)(r => values.filter(_.hasRole(r)))
  }

  private def isOldestUnsafe(selfAddress: Address, role: Option[String]): Boolean = {
    targetMembers(role).headOption.map(_.address).contains(selfAddress)
  }

  def isOldest(selfAddress: Address): Boolean = {
    isAllIntermediateMemberRemoved && isOldestUnsafe(selfAddress, None)
  }

  def isOldestWithout(selfAddress: Address, member: Member): Boolean = {
    isAllIntermediateMemberRemoved(member) && isOldestUnsafe(selfAddress, None)
  }

  def isOldestOf(selfAddress: Address, role: Option[String]): Boolean = {
    isAllIntermediateMemberRemoved && isOldestUnsafe(selfAddress, role)
  }

  def isOldestOf(selfAddress: Address, role: Option[String], without: Member): Boolean = {
    isAllIntermediateMemberRemoved(without) && isOldestUnsafe(selfAddress, role)
  }

  def isSecondaryOldest(selfAddress: Address, role: Option[String]): Boolean = {
    val tm = targetMembers(role)
    if (tm.size >= 2) {
      tm.slice(1, 2).head.address == selfAddress
    } else false
  }

  def oldestMember(role: Option[String]): Option[Member] =
    targetMembers(role).headOption

  def isOldestAlone(
      selfAddress: Address,
      role: Option[String],
      isOK: Member => Boolean,
      isKO: Member => Boolean
  ): Boolean = {
    val tm = targetMembers(role)
    if (tm.isEmpty || tm.size == 1) true
    else {
      val oldest = tm.head
      val rest   = tm.tail
      if (isOldestUnsafe(selfAddress, role)) {
        isOK(oldest) && rest.forall(isKO)
      } else {
        isKO(oldest) && rest.forall(isOK)
      }
    }
  }

}

object SortedMembersByOldest {

  lazy val empty: SortedMembersByOldest = new SortedMembersByOldest(immutable.SortedSet.empty(Member.ageOrdering))

  def apply(values: immutable.SortedSet[Member]): SortedMembersByOldest = {
    empty union new SortedMembersByOldest(values.filterNot { m =>
      m.status == MemberStatus.Removed
    })
  }

}

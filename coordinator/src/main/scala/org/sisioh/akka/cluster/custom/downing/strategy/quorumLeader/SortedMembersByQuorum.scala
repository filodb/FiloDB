package org.sisioh.akka.cluster.custom.downing.strategy.quorumLeader

import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.strategy.{ Members, SortedMembers }

import scala.collection.immutable

class SortedMembersByQuorum(values: immutable.SortedSet[Member]) extends SortedMembers(values, Member.ageOrdering) {
  override type This = SortedMembersByQuorum
  override protected def createInstance(values: immutable.SortedSet[Member]): SortedMembersByQuorum =
    new SortedMembersByQuorum(values)

  protected def targetMember(p: Member => Boolean): SortedMembersByQuorum = filter { m =>
    (m.status == MemberStatus.Up || m.status == MemberStatus.Leaving) && p(m)
  }

  private def quorumMemberOf(p: Member => Boolean, role: Option[String]): SortedMembersByQuorum = {
    val ms = targetMember(p)
    role.fold(ms)(r => ms.filter(_.hasRole(r)))
  }

  def isQuorumMet(p: Member => Boolean, quorumSize: Int, role: Option[String]): Boolean = {
    val ms = quorumMemberOf(p, role)
    ms.size >= quorumSize
  }

  def isQuorumMetAfterDown(p: Member => Boolean, quorumSize: Int, members: Members, role: Option[String]): Boolean = {
    val minus =
      if (role.isEmpty) members.size
      else {
        val r = role.get
        members.count(_.hasRole(r))
      }
    val ms = quorumMemberOf(p, role)
    ms.size - minus >= quorumSize
  }

}

object SortedMembersByQuorum {

  lazy val empty: SortedMembersByQuorum = new SortedMembersByQuorum(immutable.SortedSet.empty(Member.ageOrdering))

  def apply(values: immutable.SortedSet[Member]): SortedMembersByQuorum = {
    empty union new SortedMembersByQuorum(values.filterNot { m =>
      m.status == MemberStatus.Removed
    })
  }
}

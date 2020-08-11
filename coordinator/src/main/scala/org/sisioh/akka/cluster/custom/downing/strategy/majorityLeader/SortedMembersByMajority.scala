package org.sisioh.akka.cluster.custom.downing.strategy.majorityLeader

import akka.cluster.{ Member, MemberStatus }
import org.sisioh.akka.cluster.custom.downing.strategy.{ Members, SortedMembers }

import scala.collection.immutable

class SortedMembersByMajority(values: immutable.SortedSet[Member]) extends SortedMembers(values, Member.ordering) {
  override type This = SortedMembersByMajority
  override protected def createInstance(values: immutable.SortedSet[Member]): SortedMembersByMajority =
    new SortedMembersByMajority(values)

  def majorityMemberOf(role: Option[String]): SortedMembersByMajority = {
    createInstance(role.fold(values)(r => values.filter(_.hasRole(r))))
  }

  def isMajority(role: Option[String], isOK: Member => Boolean): Boolean = {
    val ms        = majorityMemberOf(role)
    val okMembers = ms filter isOK
    val koMembers = ms -- okMembers

    val isEqual = okMembers.size == koMembers.size
    okMembers.size > koMembers.size || isEqual && ms.headOption.forall(okMembers.contains)
  }

  def isMajorityAfterDown(members: Members, role: Option[String], isOK: Member => Boolean): Boolean = {
    val minus =
      if (role.isEmpty) members
      else {
        val r = role.get
        members.filter(_.hasRole(r))
      }
    val ms        = majorityMemberOf(role)
    val okMembers = (ms filter isOK) -- minus
    val koMembers = ms -- okMembers

    val isEqual = okMembers.size == koMembers.size
    okMembers.size > koMembers.size || isEqual && ms.headOption.forall(okMembers.contains)
  }
}

object SortedMembersByMajority {

  lazy val empty: SortedMembersByMajority = new SortedMembersByMajority(immutable.SortedSet.empty(Member.ordering))

  def apply(values: immutable.SortedSet[Member]): SortedMembersByMajority = {
    empty union new SortedMembersByMajority(values.filterNot { m =>
      m.status == MemberStatus.Removed
    })
  }

}

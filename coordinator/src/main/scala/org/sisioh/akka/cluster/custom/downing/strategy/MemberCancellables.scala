package org.sisioh.akka.cluster.custom.downing.strategy

import akka.actor.Cancellable
import akka.cluster.Member

class MemberCancellables(values: Map[Member, Cancellable]) {

  def add(member: Member, cancellable: Cancellable): MemberCancellables =
    new MemberCancellables(values + (member -> cancellable))
  def remove(member: Member): MemberCancellables           = new MemberCancellables(values - member)
  def +(values: (Member, Cancellable)): MemberCancellables = add(values._1, values._2)
  def -(member: Member): MemberCancellables                = remove(member)
  def isEmpty: Boolean                                     = values.isEmpty
  def contains(member: Member): Boolean                    = values.contains(member)
  def cancel(member: Member): Unit                         = values.get(member).foreach(_.cancel())
  def cancelAll(): Unit                                    = values.values.foreach(_.cancel())
}

object MemberCancellables {

  val empty: MemberCancellables = new MemberCancellables(Map.empty)

  def apply(values: Map[Member, Cancellable]) = new MemberCancellables(values)

}

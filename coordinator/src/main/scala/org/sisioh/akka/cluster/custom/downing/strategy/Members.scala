package org.sisioh.akka.cluster.custom.downing.strategy

import akka.cluster.Member

class Members(values: Set[Member]) {
  def add(member: Member): Members    = new Members(values + member)
  def remove(member: Member): Members = new Members(values - member)

  def +(member: Member): Members = add(member)
  def -(member: Member): Members = remove(member)

  def size: Int                        = values.size
  def clear: Members                   = new Members(Set.empty)
  def count(p: Member => Boolean): Int = values.count(p)

  def filter(p: Member => Boolean): Members = new Members(values.filter(p))
  def contains(member: Member): Boolean     = values.contains(member)
  def foreach(f: Member => Unit): Unit      = values.foreach(f)

  def splitHeadAndTail: (Members, Members) = {
    val (head, tail) = values.splitAt(1)
    (new Members(head), new Members(tail))
  }

  def toSet: Set[Member] = values
}

object Members {

  lazy val empty: Members = new Members(Set.empty)

  def apply(values: Member*): Members = new Members(values.toSet)

}

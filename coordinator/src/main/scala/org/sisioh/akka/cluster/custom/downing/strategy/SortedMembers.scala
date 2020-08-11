package org.sisioh.akka.cluster.custom.downing.strategy

import akka.cluster.Member

import scala.collection.immutable

abstract class SortedMembers(values: immutable.SortedSet[Member], ordering: Ordering[Member]) {
  type This <: SortedMembers

  protected def createInstance(values: immutable.SortedSet[Member]): This

  def add(member: Member): This = createInstance(values + member)

  def remove(member: Member): This = createInstance(values - member)

  def +(member: Member): This = add(member)

  def -(member: Member): This = remove(member)

  protected def --(members: This): This    = createInstance(values -- members.toSortedSet)
  protected def --(members: Members): This = createInstance(values -- members.toSet)

  def replace(member: Member): This = synchronized {
    remove(member).add(member).asInstanceOf[This]
  }

  protected def size: Int = values.size

  protected def clear: This = createInstance(immutable.SortedSet.empty(ordering))

  protected def count(p: Member => Boolean): Int = values.count(p)

  protected def filter(p: Member => Boolean): This = createInstance(values.filter(p))

  protected def filterNot(p: Member => Boolean): This = createInstance(values.filterNot(p))

  protected def contains(member: Member): Boolean = values.contains(member)

  protected def exists(p: Member => Boolean): Boolean = values.exists(p)

  protected def headOption: Option[Member] = values.headOption

  protected def foreach(f: Member => Unit): Unit = values.foreach(f)

  protected def isEmpty: Boolean = values.isEmpty

  protected def union(members: This): This =
    createInstance(values.union(members.toSortedSet))

  protected def toSortedSet: immutable.SortedSet[Member] = values

}

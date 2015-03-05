package filodb.core.cassandra

import akka.actor.Props

import filodb.core.CommandThrottlingActor

/**
 * The DataWriterActor regulates all future/async operations on Cassandra
 * data table writes.  It allows a fixed number of outstanding
 * futures.
 */
object DataWriterActor {
  val DefaultMaxOutstandingFutures = 128

  // Use this to create the class. Actors cannot be directly instantiated
  def props(maxOutstandingFutures: Int = DefaultMaxOutstandingFutures): Props =
    Props(classOf[CommandThrottlingActor], DataTable.commandMapper, maxOutstandingFutures)
}

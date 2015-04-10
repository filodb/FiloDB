package filodb.core.cassandra

import akka.actor.Props

import filodb.core.CommandThrottlingActor

/**
 * The MetadataActor regulates all future/async operations on Cassandra
 * datasets, partitions, columns tables.  It allows a fixed number of outstanding
 * futures.
 */
object MetadataActor {
  val DefaultMaxOutstandingFutures = 32

  val metadataMapper: CommandThrottlingActor.Mapper =
    DatasetTableOps.commandMapper orElse
    ColumnTable.commandMapper

  // Use this to create the class. Actors cannot be directly instantiated
  def props(maxOutstandingFutures: Int = DefaultMaxOutstandingFutures): Props =
    Props(classOf[CommandThrottlingActor], metadataMapper, maxOutstandingFutures)
}

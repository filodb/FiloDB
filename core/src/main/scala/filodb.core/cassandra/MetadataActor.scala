package filodb.core.cassandra

import akka.actor.{Actor, Props}
import scala.concurrent.Future

import filodb.core.messages.{Command, Response, NoSuchCommand}

/**
 * The MetadataActor regulates all future/async operations on Cassandra
 * datasets, partitions, columns tables.  It allows a fixed number of outstanding
 * futures.
 */
object MetadataActor {
  import com.websudos.phantom.Implicits._

  val DefaultMaxOutstandingFutures = 32

  // Returned when the # of outstanding futures is too high
  case object TooManyOutstandingFutures
  case object FutureCompleted

  // Use this to create the class. Actors cannot be directly instantiated
  def props(maxOutstandingFutures: Int = DefaultMaxOutstandingFutures): Props =
    Props(classOf[MetadataActor], maxOutstandingFutures)

  val DefaultMapper: PartialFunction[Command, Future[Response]] = {
    case x => Future { NoSuchCommand }
  }
}

/**
 * For now, just rejects commands if the # of outstanding futures is too high.
 * In the future, support cancelling individual futures?
 *
 * Note to those new to Actors:
 * 1. You cannot directly instantiate them, nor directly access even public variables.
 *    All access is via messages.
 * 2. No need to synchronize any vars, because actor code only runs on one thread at a time
 *
 * TODO: send metrics to a metrics collector actor
 */
class MetadataActor(maxOutstandingFutures: Int) extends Actor {
  import MetadataActor._
  import context.dispatcher   // for future callbacks

  var outstandingFutures = 0

  // Chain all the partial functions together into one big mapper
  val commandMapper = DatasetTableOps.commandMapper orElse
                      ColumnTable.commandMapper orElse
                      DefaultMapper

  def receive = {
    case FutureCompleted       => if (outstandingFutures > 0) outstandingFutures -= 1
    case c: Command            =>
      if (outstandingFutures >= maxOutstandingFutures) {
        // TODO: log/send rejection metrics
        sender ! TooManyOutstandingFutures
      } else {
        outstandingFutures += 1
        val originator = sender   // sender is a function, don't call in the callback
        commandMapper(c).onSuccess { case response =>
          self ! FutureCompleted
          originator ! response
        }
      }
  }
}
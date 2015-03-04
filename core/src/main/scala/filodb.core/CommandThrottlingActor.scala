package filodb.core

import akka.actor.Actor
import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.concurrent.Future

import filodb.core.messages.{Command, Response, NoSuchCommand}

object CommandThrottlingActor {
  // Returned when the # of outstanding futures is too high
  case object TooManyOutstandingFutures
  case object FutureCompleted

  type Mapper = PartialFunction[Command, Future[Response]]
}

/**
 * A class designed to take Commands, deliver Responses, and do so while throttling futures
 * preventing a huge backlog of them.
 * For now, just rejects commands if the # of outstanding futures is too high.
 * In the future, support cancelling individual futures?
 *
 * @constructor
 * @param mapper a PartialFunction from Commmand to Future[Response]
 * @param maxFutures the maximum number of command futures allowed to be outstanding
 *
 * Note to those new to Actors:
 * 1. You cannot directly instantiate them, nor directly access even public variables.
 *    All access is via messages.
 * 2. No need to synchronize any vars, because actor code only runs on one thread at a time
 *
 * TODO: send metrics to a metrics collector actor
 */
class CommandThrottlingActor(mapper: CommandThrottlingActor.Mapper,
                             maxFutures: Int) extends BaseActor {
  import CommandThrottlingActor._
  import context.dispatcher   // for future callbacks

  val mapperWithDefault = mapper orElse ({
    case x: Any => Future { NoSuchCommand }
  }: Mapper)
  var outstandingFutures = 0

  def receive: Receive = {
    case FutureCompleted       => if (outstandingFutures > 0) outstandingFutures -= 1
    case c: Command            =>
      if (outstandingFutures >= maxFutures) {
        // TODO: log/send rejection metrics
        logger.debug(s"$outstandingFutures futures outstanding, rejecting request")
        sender ! TooManyOutstandingFutures
      } else {
        outstandingFutures += 1
        val originator = sender   // sender is a function, don't call in the callback
        mapperWithDefault(c).onSuccess { case response: Response =>
          self ! FutureCompleted
          originator ! response
        }
      }
  }
}
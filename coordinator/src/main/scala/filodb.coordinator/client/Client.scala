package filodb.coordinator.client

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

import filodb.core._

object Client {
  implicit val context = scala.concurrent.ExecutionContext.Implicits.global

  def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 30 seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

  def actorAsk[B](actor: ActorRef, msg: Any,
                  askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B = {
    implicit val timeout = Timeout(askTimeout)
    parse(actor ? msg, askTimeout)(f)
  }

  def standardResponse[B](partial: PartialFunction[Any, B]): PartialFunction[Any, B] =
    (partial orElse {
      case other: ErrorResponse => throw ClientException(other)
      case other: Any =>           throw new RuntimeException(s"Unexpected response message: $other")
    })
}

case class ClientException(error: ErrorResponse) extends Exception(error.toString)

trait ClientBase {
  def nodeCoordinator: ActorRef

  /**
   * Convenience standard function for sending a message to NodeCoordinatorActor and parsing responses.
   * @param msg the message to send
   * @param askTimeout timeout for expecting a response
   * @param f the partialFunction for processing responses. Does not need to deal with ErrorResponses,
   *        as that will automatically be handled by the fallback function defined in standardResponse -
   *        unless it is desired to override that
   */
  def askCoordinator[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B =
    Client.actorAsk(nodeCoordinator, msg, askTimeout)(Client.standardResponse(f))
}

/**
 * Standard client for FiloDB coordinator actor, which takes reference to a single NodeCoordinator
 */
class Client(val nodeCoordinator: ActorRef) extends IngestionOps with DatasetOps
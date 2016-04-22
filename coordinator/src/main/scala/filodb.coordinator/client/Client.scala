package filodb.coordinator.client

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.slf4j.StrictLogging
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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

  def actorsAsk[B](actors: Seq[ActorRef], msg: Any,
                   askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): Seq[B] = {
    implicit val timeout = Timeout(askTimeout)
    val fut = Future.sequence(actors.map(_ ? msg))
    Await.result(fut, askTimeout).map(f)
  }
}

case class ClientException(error: ErrorResponse) extends Exception(error.toString)

trait ClientBase {
  /**
   * Convenience standard function for sending a message to one NodeCoordinator and parsing responses.
   * (Which one depends on the specific client)
   * @param msg the message to send
   * @param askTimeout timeout for expecting a response
   * @param f the partialFunction for processing responses. Does not need to deal with ErrorResponses,
   *        as that will automatically be handled by the fallback function defined in standardResponse -
   *        unless it is desired to override that
   */
  def askCoordinator[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B

  /**
   * Sends a message to ALL the coordinators, parsing the responses and returning a sequence
   */
  def askAllCoordinators[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]):
    Seq[B]
}

/**
 * Standard client for a local FiloDB coordinator actor, which takes reference to a single NodeCoordinator
 * For example, this would be used by the CLI.
 */
class LocalClient(val nodeCoordinator: ActorRef) extends IngestionOps with DatasetOps {
  def askCoordinator[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B =
    Client.actorAsk(nodeCoordinator, msg, askTimeout)(Client.standardResponse(f))

  def askAllCoordinators[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]):
    Seq[B] = Seq(askCoordinator(msg, askTimeout)(f))
}

/**
 * A client for connecting to a cluster of NodeCoordinators all having a specific role.
 */
class ClusterClient(system: ActorSystem, role: Option[String]) extends IngestionOps with DatasetOps
with StrictLogging {
  import akka.cluster.routing.ClusterRouterGroup
  import akka.cluster.routing.ClusterRouterGroupSettings
  import akka.routing._

  val coordRouter = system.actorOf(
    // ClusterRouterGroup(ConsistentHashingGroup(Nil), ClusterRouterGroupSettings(
    ClusterRouterGroup(RandomGroup(Nil), ClusterRouterGroupSettings(
      totalInstances = 100, routeesPaths = List("/user/coordinator"),
      allowLocalRoutees = true, useRole = role)).props(),
    name = "coordinatorRouter")

  private def getActorRefs(r: Routee): ActorRef = r match {
    case ActorRefRoutee(ref) => ref
    case ActorSelectionRoutee(selection) =>
      logger.debug(s"Got ActorSelectionRoutee with selection $selection")
      Await.result(selection.resolveOne(10 seconds), 11 seconds)
  }

  /**
   * Uses the router to consistently hash messages
   */
  def askCoordinator[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B =
    Client.actorAsk(coordRouter, msg, askTimeout)(Client.standardResponse(f))

  def askAllCoordinators[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]):
    Seq[B] = {
    implicit val timeout = Timeout(askTimeout)
    val routees = Await.result(coordRouter ? GetRoutees, askTimeout).asInstanceOf[Routees]
    val refs = routees.routees.map(getActorRefs)
    logger.debug(s"Sending message $msg to refs $refs, addresses ${refs.map(_.path.address)}...")
    Client.actorsAsk(refs, msg, askTimeout)(Client.standardResponse(f))
  }
}
package filodb.coordinator.client

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import akka.actor.{ActorRef, ActorSystem, Address}
import akka.pattern.ask
import akka.util.Timeout
import monix.execution.Scheduler

import filodb.coordinator.ActorName
import filodb.core._

object Client {
  implicit val context: Scheduler = GlobalScheduler.globalImplicitScheduler

  def parse[T, B](cmd: => Future[T], awaitTimeout: FiniteDuration = 30 seconds)(func: T => B): B = {
    func(Await.result(cmd, awaitTimeout))
  }

  /**
   * Synchronous ask of an actor, parsing the result with a PartialFunction
   */
  def actorAsk[B](actor: ActorRef, msg: Any,
                  askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B = {
    implicit val timeout: Timeout = Timeout(askTimeout)
    parse(actor ? msg, askTimeout)(f)
  }

  def asyncAsk(actor: ActorRef, msg: Any, askTimeout: FiniteDuration = 30 seconds): Future[Any] = {
    implicit val timeout: Timeout = Timeout(askTimeout)
    actor ? msg
  }

  def asyncTypedAsk[T: ClassTag](actor: ActorRef, msg: Any, askTimeout: FiniteDuration = 30 seconds): Future[T] = {
    implicit val timeout: Timeout = Timeout(askTimeout)
    (actor ? msg).mapTo[T]
  }

  def standardResponse[B](partial: PartialFunction[Any, B]): PartialFunction[Any, B] =
    (partial orElse {
      case other: ErrorResponse => throw ClientException(other)
      case other: Any =>           throw new RuntimeException(s"Unexpected response message: $other")
    })

  def actorsAsk[B](actors: Seq[ActorRef], msg: Any,
                   askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): Seq[B] = {
    implicit val timeout: Timeout = Timeout(askTimeout)
    val fut = Future.sequence(actors.map(_ ? msg))
    Await.result(fut, askTimeout).map(f)
  }

  /**
   * Creates a LocalClient that remotely connects to a standalone FiloDB node NodeCoordinator.
   * @param host the full host string (without port) or IP address where the FiloDB standalone node resides
   * @param port the Akka port number for remote connectivity
   * @param system the ActorSystem to connect to
   */
  def standaloneClient(system: ActorSystem,
                       v2ClusterEnabled: Boolean,
                       host: String,
                       port: Int = 2552,
                       askTimeout: FiniteDuration = 30 seconds): LocalClient = {
    val addr = Address("akka.tcp", "filo-standalone", host, port)
    val refFuture = system.actorSelection(ActorName.nodeCoordinatorPath(addr, v2ClusterEnabled))
                          .resolveOne(askTimeout)
    val ref = Await.result(refFuture, askTimeout)
    new LocalClient(ref)
  }

  /**
   * @param hostPort host:port of the standalone node
   * @param system the ActorSystem to connect to
   * @param askTimeout timeout for expecting a response
   * @return a LocalClient that remotely connects to a standalone FiloDB node NodeCoordinator
   */
  def standaloneClientV2(hostPort: String,
                         system: ActorSystem,
                         askTimeout: FiniteDuration = 10 seconds): LocalClient = {
    val refFuture = system.actorSelection(ActorName.nodeCoordinatorPathClusterV2(hostPort)).resolveOne(askTimeout)
    val ref = Await.result(refFuture, askTimeout)
    new LocalClient(ref)
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

}

trait AllClientOps extends QueryOps with ClusterOps

/**
 * Standard client for a local FiloDB coordinator actor, which takes reference to a single NodeCoordinator
 * For example, this would be used by the CLI.
 */
class LocalClient(val nodeCoordinator: ActorRef) extends AllClientOps {
  def askCoordinator[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B =
    Client.actorAsk(nodeCoordinator, msg, askTimeout)(Client.standardResponse(f))

  // Always get the cluster actor ref anew.  Cluster actor may move around the cluster!
  def clusterActor: Option[ActorRef] =
    askCoordinator(MiscCommands.GetClusterActor) { case x: Option[ActorRef] @unchecked => x }
}
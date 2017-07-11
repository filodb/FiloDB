package filodb.coordinator.client

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.StrictLogging
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

import filodb.core._
import filodb.coordinator.NodeClusterActor

object Client {
  implicit val context = monix.execution.Scheduler.Implicits.global

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

  /**
   * Sends a message to ALL coordinators without waiting for a response
   */
  def sendAllIngestors(msg: Any): Unit
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

  def sendAllIngestors(msg: Any): Unit = { nodeCoordinator ! msg }
}

/**
 * A client for connecting to a cluster of NodeCoordinators.
 * @param nodeClusterActor ActorRef to an instance of NodeClusterActor
 * @param ingestionRole the role of the cluster members doing the ingestion
 * @param metadataRole the role of the cluster member handling metadata updates
 */
class ClusterClient(nodeClusterActor: ActorRef,
                    ingestionRole: String,
                    metadataRole: String) extends IngestionOps with DatasetOps
with StrictLogging {
  import NodeClusterActor._

  def askCoordinator[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]): B =
    Client.actorAsk(nodeClusterActor, ForwardToOne(metadataRole, msg), askTimeout)(
                    Client.standardResponse(f))

  def askAllCoordinators[B](msg: Any, askTimeout: FiniteDuration = 30 seconds)(f: PartialFunction[Any, B]):
    Seq[B] = {
    implicit val timeout = Timeout(askTimeout)
    val coords: Set[ActorRef] = Await.result(nodeClusterActor ? GetRefs(ingestionRole), askTimeout) match {
      case refs: Set[ActorRef] @unchecked => refs
      case NoSuchRole          => throw ClientException(NoSuchRole)
    }
    logger.debug(s"Sending message $msg to coords $coords, addresses ${coords.map(_.path.address)}...")
    Client.actorsAsk(coords.toSeq, msg, askTimeout)(Client.standardResponse(f))
  }

  def sendAllIngestors(msg: Any): Unit = nodeClusterActor ! ForwardToAll(ingestionRole, msg)
}
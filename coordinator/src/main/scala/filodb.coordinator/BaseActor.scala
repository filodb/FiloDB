package filodb.coordinator

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef

trait BaseActor extends Actor with StrictLogging {
  logger.info(s"Starting class ${this.getClass.getName}, actor $self with path ${self.path}")

  override def preStart(): Unit = {
    logger.debug("In preStart()")
  }

  override def postStop(): Unit = {
    logger.info("Shutting down.")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"preRestart: Thrown while processing $message in ${self.path.toSerializationFormat}", reason)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    logger.error(s"postRestart: Thrown while processing in ${self.path.toSerializationFormat}", reason)
    super.postRestart(reason)
  }

  /** Returns true if actor refs `a` and b are the same actor and incarnation.
    * This test of identity is preferable to `actor1 == actor2` because it
    * additionally leverages the unique ID of the actors. In this way we
    * test for different incarnations of actors with same path:
    * address, path and name elements.
    */
  protected def isSame(a: ActorRef, b: ActorRef): Boolean = (a compareTo b) == 0

}

/** Leverages the [[akka.actor.ActorContext.children]] dataset structure
  * managed by Akka for an actor creating other actors. Uses a naming
  * convention allowing avoidance of an additional dataset holding
  * created actors.
  */
private[coordinator] trait NamingAwareBaseActor extends BaseActor {

  /** Returns true if this is the Singleton node cluster actor. */
  def isCluster(actor: ActorRef): Boolean =
    actor.path.name startsWith ActorName.ClusterSingletonManagerName

  /** Returns true if this is any incarnation of a node coordinator actor. */
  def isCoordinator(actor: ActorRef): Boolean =
    actor.path.name == ActorName.CoordinatorName

  /** Returns the one child actor, for the given `DatasetRef`, of the
    * given behavior (e.g. ingestion, query), if exists.
    */
  def childFor(dataset: DatasetRef, prefix: String): Option[ActorRef] =
    context.child(s"$prefix-$dataset")

  /** Returns all children for all `DatasetRef`s of the given behavior,
    * e.g. get all Ingester workers, get all Query workers to broadcast
    * an event to them.
    */
  def childrenForType(prefix: String): Iterable[ActorRef] =
    context.children filter (_.path.name startsWith prefix)
}

package filodb.coordinator

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging

trait BaseActor extends Actor with StrictLogging {
  logger.info(s"Starting class ${this.getClass.getName}, actor $self with path ${self.path}")

  override def postStop(): Unit = {
    logger.info("Shutting down.")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"Error while processing $message", reason)
    super.preRestart(reason, message)
  }
}

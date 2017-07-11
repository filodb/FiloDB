package filodb.coordinator

import akka.actor.Actor
import com.typesafe.scalalogging.StrictLogging

abstract class BaseActor extends Actor with StrictLogging {
  logger.info(s"Starting class ${this.getClass.getName}, actor $self with path ${self.path}")
}

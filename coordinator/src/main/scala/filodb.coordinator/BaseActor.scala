package filodb.coordinator

import akka.actor.Actor
import com.typesafe.scalalogging.slf4j.StrictLogging

abstract class BaseActor extends Actor with StrictLogging {
  logger.info("Starting class " + this.getClass.getName)
}

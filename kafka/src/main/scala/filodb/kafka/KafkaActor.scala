package filodb.kafka

import scala.concurrent.duration.Duration

import akka.actor.Actor
import akka.actor.{ActorInitializationException, ActorKilledException, OneForOneStrategy, SupervisorStrategy}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException

private[filodb] trait KafkaActor extends Actor with StrictLogging {

  import akka.actor.SupervisorStrategy._

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = Duration.Inf, loggingEnabled = true) {
      case _: ActorInitializationException => Stop
      case _: ActorKilledException         => Stop
      case _: ConfigException              => Stop
      case _: KafkaException               => Escalate
      case _: Exception                    => Escalate
    }

  override def postStop(): Unit = {
    logger.info("Shutting down.")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"Error while processing $message", reason)
    super.preRestart(reason, message)
  }
}

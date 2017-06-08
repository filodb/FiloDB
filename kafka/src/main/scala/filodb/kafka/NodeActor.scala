package filodb.kafka

import scala.concurrent.duration.Duration

import akka.actor.{Actor, ActorLogging}
import akka.actor.{ActorInitializationException, ActorKilledException, OneForOneStrategy, SupervisorStrategy}
import org.apache.kafka.common.KafkaException

private[filodb] trait NodeActor extends Actor with ActorLogging {

  import akka.actor.SupervisorStrategy._

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = Duration.Inf, loggingEnabled = true) (defaultDecider)

  override def postStop(): Unit = {
    log.info("Shutting down.")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "Error - '{}' while processing {}", reason, message)
    super.preRestart(reason, message)
  }
}

trait KafkaActor extends NodeActor {
  import akka.actor.SupervisorStrategy._

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = Duration.Inf, loggingEnabled = true) {
      case _: ActorInitializationException    => Stop
      case _: ActorKilledException            => Stop
      case _: KafkaException                  => Escalate
      case _: Exception                       => Escalate
    }
}

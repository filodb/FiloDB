package filodb.kafka

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{ExtendedActorSystem, Extension}
import com.typesafe.scalalogging.slf4j.StrictLogging

/** The base Kafka extension, created by the ActorSystem. */
private[kafka] abstract class Kafka(val system: ExtendedActorSystem)
  extends Extension with StrictLogging {

  system.registerOnTermination(shutdown())

  protected def settings: KafkaSettings

  protected val _isTerminated = new AtomicBoolean(false)

  /** INTERNAL API.*/
  protected def shutdown(): Unit

  /** INTERNAL API.
    * If self-created actor system, manage lifecycle internally.
    */
  protected def internalShutdown(): Unit = {
    if (_isTerminated.compareAndSet(false, true)) {
      logger.info("Starting shutdown.")
      system.shutdown()
      system.awaitTermination(settings.GracefulStopTimeout)
    }
  }
}

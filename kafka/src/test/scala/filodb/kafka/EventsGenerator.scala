package filodb.kafka

import akka.actor.ActorSystem
import akka.event.Logging

import scala.util.Random

/** Simulates what would be an emitter in a distributed system generating events
  * via http or other protocol to microservices in clusters.
  *
  * Generates device events from regions and time zones.
  */
final class EventsGenerator(system: ActorSystem,
                            router: KafkaExtension,
                            partitionNum: Int,
                            eventsPerSecond: Int) {

  private val random = new Random()

  private val logger = Logging.getLogger(system, this)

  /** Generates n-events per second for multiple time zones, that are
    * published to the gateway (e.g. some microservice via some protocol)
    * which publishes to Kafka.
    */
  def generate(): Unit = {
    val key = 1

    for {
      eps  <- 0 until eventsPerSecond
      part <- 0 until partitionNum
    } router.publish(part, random.nextLong.toString.getBytes)
  }
}

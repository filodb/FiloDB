package filodb.routing

import akka.actor.ActorSystem
import akka.event.Logging
import com.typesafe.config.ConfigFactory
import filodb.kafka.ProducerSettings
import org.joda.time.{DateTime, DateTimeZone}

/** Simulates what would be an emitter in a distributed system generating events
  * via http or other protocol to microservices in clusters.
  *
  * Generates device events from regions and time zones.
  */
private[data] final class EventsGenerator(system: ActorSystem,
                                          settings: NodeSettings,
                                          gateway: NodeRouter) {

  private val settings = new ProducerSettings()

  private val logger = Logging.getLogger(system, this)

  val config = ConfigFactory.load()

  val EventsPerSecondAndPartition = settings.getInt("tasks.events-per-second-and-partition")

  /** Generates n-events per second for multiple time zones, that are
    * published to the gateway (e.g. some microservice via some protocol)
    * which publishes to Kafka.
    */
  @SuppressWarnings(Array("org.wartremover.warts.NoNeedForMonad"))
  def generate(): Unit = {
    val userTime = new DateTime(DateTimeZone.UTC)
    /* 50k events per second, 20 regions */

    for {
      n <- 0 until EventsPerSecondAndPartition
      (region, zone) <- settings.Locations
      location = Location(region, zone)
      deviceTime = userTime.withZone(location.timezone).plusMillis(1)
      event = DeviceEvent(region, deviceTime.getMillis, location)
    } gateway.publish(event)
  }
}

package filodb.coordinator

import akka.actor.ActorSystem
import com.typesafe.config.Config

/**
 * Provides singleton access to the actor system.
 * Should be called exactly once at JVM startup.
 */
object ActorSystemHolder {
  var system: ActorSystem = _

  def createActorSystem(name: String, config: Config): ActorSystem = {
    system = ActorSystem("filo-standalone", config)
    system
  }

}

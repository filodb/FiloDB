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

  /**
   *  This is to be used in unit tests.
   *  Allows a unit test to create its own ActorSystem
   *  in case if one does not exist and terminate it later.
   */
  def terminateActorSystem() : Unit = {
    system.terminate()
    // scalastyle:off null
    system = null
  }

}

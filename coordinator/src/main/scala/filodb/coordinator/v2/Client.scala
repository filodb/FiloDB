package filodb.coordinator.v2

import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

import filodb.core.DatasetRef

object Client extends App {

  val overrides =
    """
      |akka {
      |   actor {
      |     provider = remote
      |   }
      |   remote {
      |      enabled-transports = ["akka.remote.netty.tcp"]
      |      netty.tcp {
      |        hostname = "127.0.0.1"
      |        port = 3552
      |      }
      |   }
      |}
      |""".stripMargin

  val config = ConfigFactory.parseString(overrides).withFallback(ConfigFactory.load)
  val system = ActorSystem("FiloDB", config)

  implicit val ec = system.dispatcher
  val selection = system.actorSelection("akka.tcp://FiloDB@127.0.0.1:2552/user/NodeCoordinatorActor")
    .resolveOne(10.seconds).foreach { ref =>
    ref ! GetShardMapScatter(DatasetRef("prometheus"))
    println("Sent message")
  }
}


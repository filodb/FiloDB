package filodb.coordinator.v2

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Server extends App {

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
      |        port = 2552
      |      }
      |   }
      |}
      |""".stripMargin

  val config = ConfigFactory.parseString(overrides).withFallback(ConfigFactory.load)
  val system = ActorSystem("FiloDB", config)

  val worker = system.actorOf(Props[Server], "remote-worker")

  println(s"Worker actor path is ${worker.path}")

}

class Server extends Actor {
  def receive: Receive = {
    case msg: String =>
      println(s"Worker received message: $msg")
  }
}
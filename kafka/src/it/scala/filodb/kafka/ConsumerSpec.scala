package filodb.kafka

import scala.util.Random

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.apache.kafka.streams.KafkaStreams
import org.scalatest.{MustMatchers, WordSpecLike}

class ConsumerSpec extends TestKit(ActorSystem("test")) with BaseSpec with MustMatchers with WordSpecLike {

  private val random = new Random()
  private val expected = 10000
  private val eventsPerSecond = expected
  private val partitionNum = 100
  private val router = KafkaExtension(system) // publish events to consume

  override def beforeAll(): Unit = {
    val provisioned = router.provision(Map.empty[String,AnyRef].asJMap)
    router.isConnected must be(true)
  }

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "KafkaStream" must {
    "stream from kafka to specific filodb nodes" in {
      KafkaStream.start(config: Map[String,AnyRef])
      generate()

      Thread.sleep(100000)
    }
  }

  private def generate(): Unit = {
    val key = 1

    for {
      eps <- 0 until eventsPerSecond
      part <- 0 until partitionNum
    } router.publish(part.toLong, s"new-event-${System.nanoTime}".getBytes)
  }
}

package filodb.kafka

import akka.actor.ActorSystem
import akka.testkit.TestKit
import org.scalatest.{MustMatchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class PublisherSpec extends TestKit(ActorSystem()) with BaseSpec with MustMatchers with WordSpecLike {

  val userProducerConfig = Map.empty[String, AnyRef] // has defaults for local

  val router = KafkaExtension(system)

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A publisher" must {
    "provision resources" in {
      val provisioned = router.provision(userProducerConfig.asJMap)
      router.isConnected must be(true)
    }
    "publish to kafka" in {
      val expected = 10000
      val eventsPerSecond = expected
      val partitionNum = 100

      /* simulate event stream */
      val generator = new EventsGenerator(system, router, partitionNum, eventsPerSecond)
      generator.generate()

      val topic = router.kafkaSettings.IngestionTopic
      def status = Await.result(router.getPublishStatus(topic), 10.seconds)
      while(status.totalSends < expected) {
        Thread.sleep(2000)
      }
      status.totalSends must be >= (100L)
    }
  }
}


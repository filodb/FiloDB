package filodb.routing

import akka.actor.{ActorSystem, Cancellable}
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import filodb.kafka.PublisherSettings
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

abstract class KafkaEventsGeneratorSpec extends TestKit(ActorSystem("test"))
  with WordSpecLike with MustMatchers with BeforeAndAfterAll with Eventually  {

  protected val settings = new NodeSettings(ConfigFactory.load())

  protected val kafkaSettings = new PublisherSettings()

  protected val microservice = NodeRouter(system)

  protected val generator = new EventsGenerator(system, settings, microservice)

  protected var task: Option[Cancellable] = None

  override def afterAll(): Unit = {
    task.map(_.cancel())
    microservice.shutdown()
    TestKit.shutdownActorSystem(system)

    //eventually(timeout(2.seconds)) (system.isTerminated)
  }x
}

class KafkaPartitionSpec extends KafkaEventsGeneratorSpec {

  import settings._
  import kafkaSettings._

  "KafkaPartition" must {
    "publish and get status" in {
      eventually(microservice.isRunning)

      task = Some(system.scheduler.schedule(Duration.Zero, PublishInterval)(generator.generate()))

      val status = Await.result(microservice.getPublishStatus(IngestionTopic), 8000.millis)
      println("********** status ***********")
      status.success.nonEmpty must be(true)
      status.failure.count must be(0)
      println("********** status ***********")

    }
  }
}
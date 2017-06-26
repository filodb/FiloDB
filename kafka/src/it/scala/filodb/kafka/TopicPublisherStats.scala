package filodb.kafka

import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef, Props}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

final class TopicPublisherStats(settings: SinkSettings, topic: String) extends KafkaActor {

  import Sink._
  import settings._
  import context.dispatcher

  private var success: Map[Int, SuccessCount] = Map.empty

  private var failure: FailureCount = FailureCount()

  private val task = context.system.scheduler.schedule(Duration.Zero, StatusLogInterval) (accumulate())

  /** Optionally publish failures to subscribers, enabling more immediate
    * response to failures, and capture for cumulative analysis and query later. */
  private val failureChannel =
    if (producerConfig.nonEmpty) Some(new KafkaProducer[String, PublishFailure](producerConfig.asProps))
    else None

  override def postStop(): Unit = {
    task.cancel()
    super.postStop()
  }

  def receive: Actor.Receive = {
    case e: PublishSuccess => onSuccess(e)
    case e: PublishFailure => onFailure(e)
    case GetStatus         => onStatus(sender())
  }

  private def onSuccess(e: PublishSuccess): Unit =
    success.get(e.partition) match {
      case Some(existing) =>
        success = success - e.partition
        success += e.partition -> existing.copy(count = existing.count + 1)//.add(partition, sentOffset)
      case _ =>
        success += e.partition -> SuccessCount(e.partition, 0)
    }

  private def onFailure(e: PublishFailure): Unit = {
    failure = failure.increment(e.reason)
    failureChannel foreach (_.send(new ProducerRecord(FailureTopic, e)))
  }

  private def onStatus(origin: ActorRef): Unit =
    sender() ! PublishContextStatus(FailureTopic, success, failure)

  private def accumulate(): Unit = success match {
    case s if s.isEmpty && failure.nonEmpty =>
      logger.error(s"Topic[failures=${failure.failures.size}: brokers may not be up yet or producer unable to connect.")

    case s if s.nonEmpty =>
      val successCount = success.values.map(_.count).sum
      logger.debug(s"Totals [topic=$topic sends=$successCount failures=${failure.failures}]")

      success foreach { case (partition, s) =>
        logger.debug(s" => partition=$partition sends=${s.count}")
      }
    case _ =>
  }
}

private[filodb] object TopicPublisherStats {
  def props(settings: SinkSettings, topic: String): Props =
    Props(new TopicPublisherStats(settings, topic))
}
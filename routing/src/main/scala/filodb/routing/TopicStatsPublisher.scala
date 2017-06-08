package filodb.routing

import akka.actor.{Actor, ActorRef, Props}

import scala.collection.immutable
import scala.concurrent.duration.{Duration, _}

final class TopicStatsPublisher(topic: String, taskLogInterval: FiniteDuration) extends KafkaActor {

  import ReadProtocol._
  import TopicStatsPublisher._
  import WriteProtocol._
  import context.dispatcher

  private var success: Map[Int, SuccessCount] = Map.empty

  private var failure: FailureCount = FailureCount(0L, 0L)

  private val task = context.system.scheduler.schedule(Duration.Zero, taskLogInterval) (status())

  override def postStop(): Unit = {
    task.cancel()
    super.postStop()
  }

  def receive: Actor.Receive = {
    case PublishSuccessStatus(_, _, partition, offset) =>
      onSuccess(partition, offset)
    case PublishFailureStatus(time, _, _) =>
      onFailure(time)
    case GetPublishStatus =>
      status(sender())
  }

  private def status(origin: ActorRef): Unit = {
    origin ! TopicPartitionPublishStatus(success, failure)
  }

  private def seen(partition: Int, offset: Long): Boolean =
    success.values.exists { _.seen(partition, offset) }

  private def onSuccess(partition: Int, sentOffset: Long): Unit = {
    if (seen(partition, sentOffset)) {
      log.error(s"Duplicate offset sent={} for topic={} partition {} not allowed.",
        sentOffset, topic, partition)
      context.parent ! DuplicateOffset(UtcTime.now, topic, partition, sentOffset)
    } else {
      success.get(partition) match {
        case Some(existing) =>
          success = success - partition
          success += partition -> existing.add(partition, sentOffset)
        case _ =>
          success += partition -> SuccessCount(partition, immutable.SortedSet(sentOffset))
      }
    }
  }

  private def onFailure(time: Long): Unit = {
    failure = failure.copy(timestamp = time, count = failure.count + 1)
  }

  private def status(): Unit = {
    if (success.isEmpty && failure.nonEmpty)
      log.error("Topic[failures={}: brokers may not be up yet or producer unable to connect.", failure.count)
    else {
      // if (success.nonEmpty) context.parent ! ClusterProtocol.ConnectTimeout

      val successCount = success.values.map(_.offsets.size).sum
      log.info("Totals [topic={} sends={} failures={}]", topic, successCount, failure.count)

      success foreach { case (partition, s) =>
        log.info(s"  partition={} sends={} offset-range={} to {}",
          partition, s.offsets.size, s.offsets.min, s.offsets.max)
      }
    }
  }
}

object TopicStatsPublisher {

  def props(topic: String, taskLogInterval: FiniteDuration): Props =
    Props(new TopicStatsPublisher(topic, taskLogInterval))

  private[filodb] final case class SuccessCount(partition: Int, offsets: immutable.SortedSet[Long]) {
    import filodb.implicits._

    def seen(part: Int, offset: Long): Boolean =
      part === partition && offsets.contains(offset)

    def add(part: Int, offset: Long): SuccessCount =
      if (seen(part, offset)) this else copy(offsets = offsets + offset)
  }

  private[filodb] final case class FailureCount(timestamp: Long, count: Long) {

    def nonEmpty: Boolean = count > 0
  }
}

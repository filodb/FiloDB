package filodb.kafka

import scala.concurrent.duration.{Duration, _}

import akka.actor.{Actor, Props}

final class TopicStatsPublisher(topic: String, taskLogInterval: FiniteDuration) extends KafkaActor {

  import Protocol._, TopicStatsPublisher._
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
      log.info("\nReturning request success {}, failure {}", success, failure)
      sender() ! TopicPartitionPublishStatus(success, failure)
  }

 /* TODO after hackathon, had to change things too quickly to update this
   private def seen(partition: Int, offset: Long): Boolean =
    success.values.exists { _.seen(partition, offset) }*/

  private def onSuccess(partition: Int, sentOffset: Long): Unit = {
    success.get(partition) match {
      case Some(existing) =>
        val pre = success.size
        success = success - partition
        val post = success.size
        require(post < pre)
        success += partition -> existing.copy(count = existing.count + 1)//.add(partition, sentOffset)
        require(pre == success.size)
      case _ =>
        success += partition -> SuccessCount(partition, 0)
    }
  }

  private def onFailure(time: Long): Unit = {
    failure = failure.copy(timestamp = time, count = failure.count + 1)
  }

  private def status(): Unit = success match {
    case s if s.isEmpty && failure.nonEmpty =>
      log.error("Topic[failures={}: brokers may not be up yet or producer unable to connect.", failure.count)

    case s if s.nonEmpty =>
      val successCount = success.values.map(_.count).sum
      log.info("Totals [topic={} sends={} failures={}]", topic, successCount, failure.count)

      success foreach { case (partition, s) =>
        log.info(s" => partition={} sends={}", partition, s.count)// /*offset-range={} to {}*/
      }
    case _ =>
  }
}

private[filodb] object TopicStatsPublisher {

  def props(topic: String, taskLogInterval: FiniteDuration): Props =
    Props(new TopicStatsPublisher(topic, taskLogInterval))

  final case class SuccessCount(partition: Int, count: Long) {

   /* TODO fix after hackathon, had to change things too quickly to fix
   def seen(part: Int, offset: Long): Boolean =
      part === partition && offsetRange.contains(offset)

    def add(part: Int, offset: Long): SuccessCount =
      if (seen(part, offset)) this else copy(offsetRange = offsetRange.)//offsets + offset)*/
  }

  final case class FailureCount(timestamp: Long, count: Long) {

    def nonEmpty: Boolean = count > 0
  }
}

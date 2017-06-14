package filodb.kafka

import akka.actor.ActorRef

/* Quick wip not final. */
// scalastyle:off
private[filodb] object Protocol {

  @SerialVersionUID(1L)
  trait ClusterEvent extends Serializable

  trait NodeAck

  @SerialVersionUID(1L)
  sealed trait NodeTask extends ClusterEvent

  sealed trait ClusterCommand extends ClusterEvent

  sealed trait Lifecycle extends NodeTask

  sealed trait TaskSuccess extends NodeTask

  sealed trait TaskFailure extends NodeTask

  case object Initialized extends Lifecycle

  case object GracefulShutdown extends Lifecycle

  final case object IsInitialized extends TaskSuccess

  final case object Connected extends TaskSuccess

  final case object ChildConnected extends TaskSuccess

  final case object Disconnected extends TaskSuccess

  final case object ConnectTimeout extends TaskFailure

  @SerialVersionUID(1L)
  final case class Provision(config: Map[String,AnyRef]) extends ClusterCommand

  @SerialVersionUID(1L)
  final case class Provisioned(sender: ActorRef) extends TaskSuccess

  @SerialVersionUID(1L)
  final case class CreatePublisher(topic: String, config: Map[String,AnyRef]) extends ClusterCommand

  @SerialVersionUID(1L)
  final case class PublisherCreated(topic: String) extends ClusterCommand

  final case class WorkerFailure(eventTime: Long, topic: String, e: Throwable, ctx: String) extends TaskFailure

  /* Write */
  @SerialVersionUID(1L)
  trait WriteEvent extends ClusterEvent {
    //def identity: ID
  }

  sealed trait PublishTask extends WriteEvent

  final case class FailureContext(ctx: TaskFailure) extends PublishTask

  final case class Publish(key: Long, value: Array[Byte], start: Long) extends PublishTask

  final case class PublishTo(userTopic: String,
                             key: Int,
                             value: Array[Byte]) extends PublishTask

  sealed trait PublishTaskAck extends NodeAck

  final case class PublishSuccessAck(topic: String,
                                     partition: Int,
                                     offset: Long
                                    ) extends PublishTaskAck with TaskSuccess

  final case class PublishFailureAck(eventTime: Long,
                                     topic: String,
                                     context: Throwable,
                                     key: Option[AnyRef]
                                    ) extends PublishTaskAck with TaskFailure

  sealed trait PublishSuccess extends TaskSuccess

  sealed trait PublishFailure extends TaskFailure

  final case class PublishSuccessStatus(eventTime: Long,
                                        topic: String,
                                        partition: Int,
                                        sentOffset: Long) extends PublishSuccess

  final case class PublishFailureStatus(eventTime: Long,
                                        topic: String,
                                        reason: Exception) extends PublishFailure

  final case class NoRouteExists(eventTime: Long, bytes: Array[Byte]) extends PublishFailure

  /* Read */

  @SerialVersionUID(1L)
  sealed trait ClusterQuery extends Serializable

  import TopicStatsPublisher.{FailureCount, SuccessCount}

  final case class GetPublishStatus(topic: String) extends ClusterQuery

  final case class TopicPartitionPublishStatus(success: Map[Int, SuccessCount], failure: FailureCount) {

    def totalSends: Long = success.map(_._2.count).sum
  }

}
// scalastyle:on

import org.joda.time.{DateTime, DateTimeZone}

object UtcTime {

  def now: Long = new DateTime(DateTimeZone.UTC).getMillis
}

package filodb.routing

import org.joda.time.{DateTime, DateTimeZone}

/* Quick wip not remotely final. */
private[filodb] object PlatformProtocol {

  @SerialVersionUID(1L)
  trait PlatformEvent extends Serializable

  trait ClusterEvent extends PlatformEvent

  trait ClusterAck

  @SerialVersionUID(1L)
  sealed trait ClusterTask extends ClusterEvent

  sealed trait Lifecycle extends ClusterTask

  case object Initialized extends Lifecycle

  case object GracefulShutdown extends Lifecycle

  sealed trait TaskSuccess extends ClusterTask

  sealed trait TaskFailure extends ClusterTask

  final case object IsInitialized extends TaskSuccess

  final case object Connected extends TaskSuccess

  final case object ChildConnected extends TaskSuccess

  final case object Disconnected extends TaskSuccess

  final case object ConnectTimeout extends TaskFailure

  final case class WorkerFailure(eventTime: Long, topic: String, e: Throwable, ctx: String) extends TaskFailure

}

/* Quick wip not remotely final. */
object WriteProtocol {

  import PlatformProtocol._

  /** temporary placeholder for the actual event details */
  trait WriteEvent extends PlatformEvent {
    def identity: String
  }

  /** temporary placeholder for the actual metadata and event details */
  final case class RawEvent(identity: String) extends WriteEvent

  sealed trait PublishTask extends ClusterTask

  final case class Publish(key: AnyRef, value: AnyRef) extends PublishTask

  sealed trait PublishTaskAck extends ClusterAck

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

  final case class NoRouteExists(eventTime: Long,
                                 eventType: Class[WriteEvent]) extends PublishFailure

  final case class DuplicateOffset(eventTime: Long,
                                   topic: String,
                                   partition: Int,
                                   sentOffset: Long) extends PublishFailure

}

/* Quick wip not remotely final. */
object ReadProtocol {

  @SerialVersionUID(1L)
  sealed trait ClusterQuery extends Serializable

  sealed trait NodeQuery extends Serializable

  import TopicStatsPublisher.{FailureCount, SuccessCount}

  final case class GetNodeStatus(topic: String) extends ClusterQuery

  final case class GetPublishStatus(topic: String) extends ClusterQuery

  final case class TopicPartitionPublishStatus(success: Map[Int, SuccessCount], failure: FailureCount)

}

object UtcTime {

  def now: Long = new DateTime(DateTimeZone.UTC).getMillis
}
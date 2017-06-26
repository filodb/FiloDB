package filodb.kafka

/** INTERNAL API. */
private[filodb] object Protocol {

  @SerialVersionUID(1L)
  trait TaskContext extends Serializable

  trait TaskSuccess extends TaskContext

  trait TaskFailure extends TaskContext

  @SerialVersionUID(1L)
  sealed trait TaskCommand extends Serializable
  trait Write extends TaskCommand
  trait TaskQuery extends TaskCommand

  @SerialVersionUID(1L)
  trait LifecycleCommand extends TaskCommand

  @SerialVersionUID(1L)
  sealed trait LifecycleTask extends Serializable

  sealed trait Lifecycle extends TaskContext

  final case object Provision extends LifecycleCommand

  final case object Provisioned extends TaskSuccess

  final case object GetConnected extends TaskSuccess

  final case class Connected(topic: String) extends TaskSuccess

  final case class NotConnected(topic: String) extends TaskFailure

  final case object ConnectTimeout extends TaskFailure

  case object GracefulShutdown extends LifecycleCommand

}

object UtcTime {
  import org.joda.time.{DateTime, DateTimeZone}

  def now: Long = new DateTime(DateTimeZone.UTC).getMillis
}

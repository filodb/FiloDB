package filodb.kafka

/** Write to FiloDB via Kafka: lifecycle, commands, tasks, failures, acks. */
object Sink {
  import Protocol._

  @SerialVersionUID(1L)
  sealed trait PublishContext extends TaskContext  //def identity: ID

  final case class Publish[K,V](key: K, value: V) extends Write

  final case class PublishAndAck[K,V](key: K, value: V) extends Write

  final case class PublishSuccess(eventTime: Long,
                                  topic: String,
                                  partition: Int,
                                  offset: Long) extends PublishContext with TaskSuccess

  final case class PublishFailure(eventTime: Long,
                                  topic: String,
                                  reason: Exception,
                                  key: Option[Any]) extends PublishContext with TaskFailure

  /** To know counts per partition. */
  final case class SuccessCount(partition: Int, count: Long) extends Serializable

  /** Failure context per publishes to a topic: counts per error type. */
  final case class FailureCount(failures: Map[String, Int] = Map.empty) extends Serializable {

    def increment(e: Exception): FailureCount = {
      val key = e.getClass.getName
      val count = failures.get(key).map(_ + 1).getOrElse(1)
      copy(failures = failures.updated(key, count))
    }

    def nonEmpty: Boolean = failures.nonEmpty
  }


  final case class GetStatus(topic: String) extends TaskQuery

  final case class PublishContextStatus(topic: String,
                                        success: Map[Int, SuccessCount],
                                        failure: FailureCount) extends PublishContext {

    def totalSends: Long = success.map(_._2.count).sum
  }
}
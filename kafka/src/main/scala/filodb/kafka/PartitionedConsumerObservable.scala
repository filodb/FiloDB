package filodb.kafka

import java.lang.{Long => JLong}

import scala.collection.JavaConverters._
import scala.concurrent.{blocking, Future}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import com.typesafe.scalalogging.StrictLogging
import monix.eval.{Callback, Task}
import monix.execution.{Ack, Cancelable}
import monix.execution.Ack.{Continue, Stop}
import monix.kafka.KafkaConsumerConfig
import monix.kafka.config.ObservableCommitType
import monix.reactive.{Observable, Observer}
import monix.reactive.observers.Subscriber
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

/** Exposes an `Observable` that consumes a Kafka stream by means of a Kafka
  * Consumer client.
  *
  * FiloDB with Kafka employs manual offset control of consumer commits
  * for offset management. Because `monix.kafka.KafkaConsumerObservable`
  * does not allow Kafka's manual offset control, only automatic, and is
  * final class that can not be extended we have to temporarily add a minor
  * implementation difference. If a user configures Kafka's `enable.auto.commit`
  * to false, `monix.observable.commit.order` defaults to committing after:
  * (config.observableCommitOrder.isAfter) regardless of Kafka's manual or
  * automatic offset configuration.
  *
  * {{{
  *   KafkaConsumerObservable {
  *     private def feedTask(out: Subscriber[ConsumerRecord[K,V]]): Task[Unit] = {
  *       val shouldCommitAfter = !config.enableAutoCommit && config.observableCommitOrder.isAfter
  *       if (shouldCommitAfter) consumerCommit(consumer)
  *    }
  *  }
  * }}}
  *
  * In this case, Monix-Kafka's `shouldCommitAfter` is true and it commits offsets
  * in a private function. We will contribute a configurable patch then remove this.
  * Until then:
  *
  *   "The consumer application need not use Kafka's built-in offset storage,
  *   it can store offsets in a store of its own choosing."
  */
private[filodb] class PartitionedConsumerObservable[K, V] private
(config: KafkaConsumerConfig,
 consumer: Task[KafkaConsumer[K, V]])
  extends Observable[ConsumerRecord[K, V]] {

  override def unsafeSubscribeFn(out: Subscriber[ConsumerRecord[K, V]]): Cancelable = {
    import out.scheduler

    feedTask(out).runAsync(new Callback[Unit] {
      def onSuccess(value: Unit): Unit =
        out.onComplete()
      def onError(ex: Throwable): Unit =
        out.onError(ex)
    })
  }

  // scalastyle:off
  private def feedTask(out: Subscriber[ConsumerRecord[K,V]]): Task[Unit] = {
    // Caching value to save CPU cycles
    val pollTimeoutMillis = config.fetchMaxWaitTime.toMillis

    // temporary fix until patch submitted: start
    // Boolean value indicating that we should trigger a commit before downstream ack
    val shouldCommitBefore = config.enableAutoCommit && config.observableCommitOrder.isBefore
    // Boolean value indicating that we should trigger a commit after downstream ack
    val shouldCommitAfter = config.enableAutoCommit && config.observableCommitOrder.isAfter
    // temporary fix until patch submitted: end

    def consumerCommit(consumer: KafkaConsumer[K,V]): Unit =
      config.observableCommitType match {
        case ObservableCommitType.Sync =>
          blocking(consumer.commitSync())
        case ObservableCommitType.Async =>
          blocking(consumer.commitAsync())
      }

    /* Returns a task that continuously polls the `KafkaConsumer` for
     * new messages and feeds the given subscriber.
     *
     * Creates an asynchronous boundary on every poll.
     */
    def runLoop(consumer: KafkaConsumer[K,V]): Task[Unit] = {
      // Creates a task that polls the source, then feeds the downstream
      // subscriber, returning the resulting acknowledgement
      val ackTask: Task[Ack] = Task.unsafeCreate { (context, cb) =>
        implicit val s = context.scheduler

        // Forced asynchronous boundary (on the I/O scheduler)
        s.executeAsync { () =>
          context.frameRef.reset()

          val ackFuture =
            try consumer.synchronized {
              if (context.connection.isCanceled) Stop else {
                val next = blocking(consumer.poll(pollTimeoutMillis))
                if (shouldCommitBefore) consumerCommit(consumer)
                // Feeding the observer happens on the Subscriber's scheduler
                // if any asynchronous boundaries happen
                Observer.feed(out, next.asScala)(out.scheduler)
              }
            } catch {
              case NonFatal(ex) =>
                Future.failed(ex)
            }

          ackFuture.syncOnComplete {
            case Success(ack) =>
              // The `streamError` flag protects against contract violations
              // (i.e. onSuccess/onError should happen only once).
              // Not really required, but we don't want to depend on the
              // scheduler implementation.
              var streamErrors = true
              try consumer.synchronized {
                // In case the task has been cancelled, there's no point
                // in continuing to do anything else
                if (context.connection.isCanceled) {
                  streamErrors = false
                  cb.asyncOnSuccess(Stop)
                } else {
                  if (shouldCommitAfter) consumerCommit(consumer)
                  streamErrors = false
                  cb.asyncOnSuccess(ack)
                }
              } catch {
                case NonFatal(ex) =>
                  if (streamErrors) cb.asyncOnError(ex)
                  else s.reportFailure(ex)
              }

            case Failure(ex) =>
              cb.asyncOnError(ex)
          }
        }
      }

      ackTask.flatMap {
        case Stop => Task.unit
        case Continue => runLoop(consumer)
      }
    }

    /* Returns a `Task` that triggers the closing of the
     * Kafka Consumer connection.
     */
    def cancelTask(consumer: KafkaConsumer[K,V]): Task[Unit] = {
      // Forced asynchronous boundary
      val cancelTask = Task {
        consumer.synchronized(blocking(consumer.close()))
      }

      // By applying memoization, we are turning this
      // into an idempotent action, such that we are
      // guaranteed that consumer.close() happens
      // at most once
      cancelTask.memoize
    }

    Task.unsafeCreate { (context, cb) =>
      implicit val s = context.scheduler
      val feedTask = consumer.flatMap { c =>
        // Skipping all available messages on all partitions
        if (config.observableSeekToEndOnStart) c.seekToEnd(Nil.asJavaCollection)
        // A task to execute on both cancellation and normal termination
        val onCancel = cancelTask(c)
        // We really need an easier way of adding
        // cancelable stuff to a task!
        context.connection.push(Cancelable(() => onCancel.runAsync(s)))
        runLoop(c).doOnFinish(_ => onCancel)
      }

      Task.unsafeStartNow(feedTask, context, cb)
    }
  }
  // scalastyle:on
}

object PartitionedConsumerObservable extends StrictLogging {
  import collection.JavaConverters._

  /** Creates a `KafkaConsumerObservable` instance.
    *
    * @param sourceConfig the `SourceConfig` needed for initializing the consumer
    * @param topicPartition the Kafka ingestion topic-partition(s) to assign the new consumer to
    * @param offset Some(longOffset) to seek to a certain offset when the consumer starts
    */
  def create(sourceConfig: SourceConfig,
             topicPartition: TopicPartition,
             offset: Option[Long]): PartitionedConsumerObservable[JLong, Any] = {

    val consumer = createConsumer(sourceConfig, topicPartition, offset)
    val cfg = consumerConfig(sourceConfig)
    require(!cfg.enableAutoCommit, "'enable.auto.commit' must be false.")

    // TODO uncomment after patch release and remove class PartitionedConsumerObservable
    // KafkaConsumerObservable[JLong, Any](cfg, consumer)
    new PartitionedConsumerObservable[JLong, Any](cfg, consumer)
  }

  private[filodb] def createConsumer(sourceConfig: SourceConfig,
                                     topicPartition: TopicPartition,
                                     offset: Option[Long]): Task[KafkaConsumer[JLong, Any]] =
    Task {
      val props = sourceConfig.asProps
      if (sourceConfig.LogConfig) logger.info(s"Consumer properties: $props")

      blocking {
        val consumer = new KafkaConsumer(props)
        consumer.assign(List(topicPartition).asJava)
        offset.foreach { off => consumer.seek(topicPartition, off) }
        consumer.asInstanceOf[KafkaConsumer[JLong, Any]]
      }
    }

  private[filodb] def consumerConfig(sourceConfig: SourceConfig) =
    KafkaConsumerConfig(sourceConfig.asConfig)

}

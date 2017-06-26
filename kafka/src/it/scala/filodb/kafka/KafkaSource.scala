package filodb.kafka

import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.collection.JavaConverters._

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider, PoisonPill, Props}
import akka.util.Timeout
import com.typesafe.config.Config

import filodb.core.memstore.MemStore
import filodb.coordinator.RowSourceFactory
import filodb.core.metadata.RichProjection

/** Experimental and in progress: providing an API that hides the underlying
  * implementation (initially Kafka streams for very high load users. Eventually
  * will support others via user configuration. Consumes events from the given source.
  * Shuts itself down gracefully on ActorSystem.shutdown/terminate.
  *
  * {{{
  *   val source = KafkaSource(system)
  *   val props = source.factory(projection, memStore)
  * }}}
  * or
  * {{{
  *   val yourOwnActor: ActorRef = subscriber
  *   KafkaSource(system).to(yourOwnActor, memStore, projection)
  * }}}
  */
object KafkaSource extends ExtensionId[KafkaSource] with ExtensionIdProvider {
  override def lookup: ExtensionId[_ <: Extension] = KafkaSource
  override def createExtension(system: ExtendedActorSystem): KafkaSource = new KafkaSource(system)
  override def get(system: ActorSystem): KafkaSource = super.get(system)
}

private[kafka] final class KafkaSource(override val system: ExtendedActorSystem) extends Kafka(system) {

  override val settings = new SourceSettings(system.settings.config)

  /** For graceful shutdown of streams and no message loss during shutdown. */
  private val sinks = new ConcurrentHashMap[String, ActorRef]

  /** Creates an event source receiving a stream of events from a configured channel
    * which deserializes and converts from user configurations and hands the streamed
    * events to the provided subscriber `sink`.
    *
    * @param sink       an `akka.actor.ActorRef` to receive the stream of deserialized
    *                   and converted `Seq[filodb.core.memstore.IngestRecord]`
    * @param projection the projection describing the dataset of the stream
    * @param memStore   the memstore to use
    */
  def to(sink: ActorRef, projection: RichProjection, memStore: MemStore): Unit = {
    val source = system.actorOf(Props(new KStreamsSource(settings, memStore, projection, sink))) //.withMailbox("akka.dispatch.BoundedMailbox")
    sinks.put(sink.path.name, source)
  }

  /** The coordinator API is changing, need for this may go away and this removed. */
  def factory(projection: RichProjection, memStore: MemStore): RowSourceFactory = new RowSourceFactory {
    override def create(config: Config, projection: RichProjection, memStore: MemStore): Props =
      Props(new KStreamsRowSource(settings, memStore, projection))
  }

  /** INTERNAL API.
    * Attempts to shut the streams down gracefully to not loose data.
    */
  override protected def shutdown(): Unit =
    if (_isTerminated.compareAndSet(false, true)) {
      import akka.pattern.gracefulStop
      import scala.concurrent.Await
      import system.dispatcher

      implicit val timeout: Timeout = settings.GracefulStopTimeout
      logger.info("Shutting down streams.")
      val futures = sinks.asScala.values.map(gracefulStop(_, settings.GracefulStopTimeout, PoisonPill))
      val status = Await.result(Future.sequence(futures), timeout.duration)
      logger.info(s"Shutdown of streams completed.")
    }
}

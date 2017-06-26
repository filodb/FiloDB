package filodb.kafka

import java.lang.{Long => JLong}

import scala.collection.JavaConverters._

import akka.actor.{Actor, ActorRef}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, ProcessorSupplier}

import filodb.core.memstore.{IngestRecord, MemStore}
import filodb.core.metadata.RichProjection

/** Experimental: template for easy usage of a Kafka Stream by
  * 1. creating a kafka stream with a
  *    a. configured serializer and deserializer type
  *    b. configured ingestion type converter
  * 2. exposing the current Kafka offset of the record being processed in the stream
  * 3. providing a direct channel from the stream to the implementation's sink
  *    via the `KStreamActor.behavior` implementation
  * 4. managing the lifecycle of the stream
  *
  * todo look at PartitionGrouper
  */
abstract class KStreamActor[V](settings: SourceSettings,
                               projection: RichProjection) extends Actor with StrictLogging {

  def to(records: Seq[IngestRecord]): Unit

  protected val converter = RecordConverter(settings.RecordConverterClass)

  protected val streamsConfig = new StreamsConfig(settings.streamsConfig.asJava)

  protected val builder = new KStreamBuilder()

  protected val stream = builder.stream[JLong, V](settings.IngestionTopic)

  stream.process(new ProcessorSupplier[JLong, V] {
    override def get: Processor[JLong, V] = new AbstractProcessor[V] {
      override def process(key: JLong, value: V): Unit =
        to(converter.convert(projection, value.asInstanceOf[AnyRef], this.ctx.offset))
      }})

  protected val streams = new KafkaStreams(builder, streamsConfig)
  streams.start()

  override def postStop(): Unit = {
    streams.close()
    super.postStop()
  }
}

/** Experimental An actor to fit into the RowSource design while
  * exposing the current Kafka offset of the record being processed.
  * Sends directly from KStream to subscriber. Needs:
  * props.withMailbox("akka.dispatch.BoundedMailbox") setup.
  *
  * INTERNAL API.
  */
private[filodb] final class KStreamsSource[V](settings: SourceSettings,
                                              memStore: MemStore,
                                              projection: RichProjection,
                                              subscriber: ActorRef
                                             ) extends KStreamActor[V](settings, projection) {

  override def to(records: Seq[IngestRecord]): Unit = records.foreach(subscriber ! _)

  override def receive: Actor.Receive = Actor.emptyBehavior
}

/** Experimental: An actor to fit into the RowSource design which is changing.
  * INTERNAL API.
  */
private[filodb] final class KStreamsRowSource[V](settings: SourceSettings,
                                                 val memStore: MemStore,
                                                 val projection: RichProjection
                                                ) extends KStreamActor[V](settings, projection) with QueueRowSource {

  override def to(events: Seq[IngestRecord]): Unit = events.foreach(queue.put)

  override def postStop(): Unit = {
    queue.clear()
    super.postStop()
  }
}

/** Experimental. Exposes the offset of the currently-processing record.
  * INTERNAL API.
  */
private[filodb] trait AbstractProcessor[V] extends Processor[JLong, V] {
  protected var ctx: ProcessorContext = _
  override def process(key: JLong, value: V): Unit
  override def init(context: ProcessorContext): Unit = this.ctx = context
  override def punctuate(timestamp: Long): Unit = {}
  override def close(): Unit = {}
}

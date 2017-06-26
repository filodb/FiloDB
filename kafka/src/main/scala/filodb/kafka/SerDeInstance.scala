package filodb.kafka

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.StreamsConfig

import filodb.coordinator.ConfigurableInstance

object SerDeInstance extends ConfigurableInstance {

  def create(cfg: Map[String, AnyRef]): Serde[_] =
    custom.getOrElse(primatives(cfg))

  /** Attempts to create custom Serde from reflection. Falls back
    * to attempting to create a built-in Kafka Serde for primitives.
    */
  private def custom: Try[Serde[_]] = {
    val ser = createInstance[Serializer[_ >: Any]](Class.forName(
      Loaded.producer(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).toString))
    val des = createInstance[Deserializer[_ >: Any]](Class.forName(
      Loaded.consumer(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).toString))

    Try(Serdes.serdeFrom(ser.get, des.get))
  }

  /** Attempts to create built-in primitive Serde's like LongSerde, etc from reflection. */
  private def primatives(cfg: Map[String, AnyRef]): Serde[_] = {
    val fqcn = cfg(StreamsConfig.VALUE_SERDE_CLASS_CONFIG)

    Try(fqcn match {
      case fqcn: Class[_] => Serdes.serdeFrom(fqcn)
      case fqcn: String   => Serdes.serdeFrom(createClass[Serde[_]](fqcn).get)
    }).recover { case NonFatal(e) =>
      logger.error(s"Unable to instantiate Serde from $fqcn", e)
      throw e
    }.getOrElse(throw new IllegalArgumentException(
      s"Unable to instantiate custom value Serde from config [$fqcn]."))
  }
}

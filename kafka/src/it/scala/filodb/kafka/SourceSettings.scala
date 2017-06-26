package filodb.kafka

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.StreamsConfig

final class SourceSettings(conf: Config) extends KafkaSettings(conf) {
  def this() = this(ConfigFactory.load)

  def consumerConfig: Map[String, AnyRef] =
    Loaded.consumer ++ Map(
      ConsumerConfig.GROUP_ID_CONFIG -> createSelfId,
      ConsumerConfig.CLIENT_ID_CONFIG -> createSelfId,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[LongDeserializer].getName)

  def streamsConfig: Map[String, AnyRef] = {
    val normalize = (prefix: String, k: String, v: AnyRef) => k.replace(prefix + ".", "") -> v

    val streams = Loaded.kafka.collect {
      case (k,v) if k.startsWith("streams.")=>
        normalize("streams", k, v)
      case (k,v) if k.startsWith("producer." + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) =>
        normalize("producer", k, v)
    } ++ consumerConfig.filterNot(_._1.startsWith("enable.auto.commit"))

    streams ++ Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> createSelfId,
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
      StreamsConfig.KEY_SERDE_CLASS_CONFIG ->  Serdes.Long().getClass)
  }

  /* Optional Kafka Streams usage. Call a processor's punctuate() method every N time units (e.g. 1000 ms) */
  val StreamProcessInterval: FiniteDuration = Duration(kafka.getDuration(
    "streams.process.interval.ms", TimeUnit.MILLISECONDS), MILLISECONDS)

}

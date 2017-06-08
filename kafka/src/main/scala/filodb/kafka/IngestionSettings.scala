package filodb.kafka

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig

final class IngestionSettings(conf: Config) extends KafkaSettings(conf) {

  def this() = this(ConfigFactory.load())

  /* Call a processor's punctuate() method every N time units (e.g. 1000 ms) */
  val StreamProcessInterval: FiniteDuration = Duration(filo.getDuration(
    "kafka.streams.process.interval.ms", TimeUnit.MILLISECONDS), MILLISECONDS)

  val EnableAutoCommit: Boolean = filo.getBoolean("kafka.consumer.enable.auto.commit")

  def consumerConfig: Map[String, AnyRef] = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> selfAddressId,
    ConsumerConfig.CLIENT_ID_CONFIG -> selfAddressId,
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> filo.getString("kafka.consumer.key.deserializer"),
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> filo.getString("kafka.consumer.value.deserializer"),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> filo.getString("kafka.consumer.auto.offset.reset"),
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> new java.lang.Boolean(EnableAutoCommit))

  def streamsConfig: StreamsConfig = {
    val c = Map(
      //    ConsumerConfig.GROUP_ID_CONFIG -> selfAddressId,
      StreamsConfig.APPLICATION_ID_CONFIG -> selfAddressId,
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
      StreamsConfig.KEY_SERDE_CLASS_CONFIG -> filo.getString("kafka.streams.key.serde"),
      StreamsConfig.VALUE_SERDE_CLASS_CONFIG -> filo.getString("kafka.streams.value.serde"),
      StreamsConfig.RECEIVE_BUFFER_CONFIG -> filo.getInt("kafka.streams.receive.buffer.bytes"),
      StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG -> filo.getInt("kafka.streams.buffered.records.per.partition"),
      StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> filo.getLong("kafka.streams.commit.interval.ms"),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> EnableAutoCommit)
    new StreamsConfig(c.asJava)
  }

}

package filodb.kafka

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.collection.JavaConverters._
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsConfig

final class IngestionSettings(conf: Config) extends KafkaSettings(conf) {

  /* Call a processor's punctuate() method every N time units (e.g. 1000 ms) */
  val StreamProcessInterval: FiniteDuration = Duration(filo.getDuration(
    "kafka.streams.process.interval.ms", TimeUnit.MILLISECONDS), MILLISECONDS)

  def consumerConfig: Map[String, AnyRef] = Map(
    ConsumerConfig.GROUP_ID_CONFIG -> selfAddressId,
    ConsumerConfig.CLIENT_ID_CONFIG -> selfAddressId,
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> filo.getString("kafka.consumer.key.deserializer"),
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> filo.getString("kafka.consumer.value.deserializer"),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> filo.getString("kafka.consumer.auto.offset.reset"),
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> new java.lang.Boolean(filo.getBoolean("kafka.consumer.enable.auto.commit")))

  def streamsConfig(serdeV: Serde[_]): Map[String, Any] = {
    Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> selfAddressId,
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
      StreamsConfig.KEY_SERDE_CLASS_CONFIG ->  Serdes.Long().getClass.getName,
      StreamsConfig.VALUE_SERDE_CLASS_CONFIG -> serdeV.getClass.getName,
      StreamsConfig.POLL_MS_CONFIG -> filo.getLong("kafka.streams.poll.ms"),
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> filo.getInt("kafka.streams.max.poll.records"),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> filo.getString("kafka.consumer.auto.offset.reset"))
      //ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> filo.getInt("kafka.streams.session.timeout.ms"),
      //StreamsConfig.RECEIVE_BUFFER_CONFIG -> filo.getInt("kafka.streams.receive.buffer.bytes"),
      //StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG -> filo.getInt("kafka.streams.buffered.records.per.partition"),
      //StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> filo.getLong("kafka.streams.commit.interval.ms"),

  }

  def createStreamsConfig(serdeV: Serde[_], additional: Map[String, Any]): StreamsConfig = {
    new StreamsConfig((additional ++ streamsConfig(serdeV)).asJava)
  }
}

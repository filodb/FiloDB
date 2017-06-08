package filodb.kafka

import java.net.InetAddress

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.CommonClientConfigs

// WIP
final class ConsumerSettings extends KafkaSettings {

  private val selfAddress = InetAddress.getLocalHost.getHostAddress

  val ConsumerKeyDeserializerClass: Class[_] =
    createClass(filo.getString("kafka.client.consumer.key.deserializer"),
      classOf[StringDeserializer].getCanonicalName)

  val ConsumerValueDeserializerClass: Class[_] =
    createClass(filo.getString("kafka.client.consumer.value.deserializer"),
      classOf[StringDeserializer].getCanonicalName)

  val ConsumerAutoOffsetReset: String = filo.getString("kafka.client.consumer.auto.offset.reset")

  val ConsumerEnableAutoCommit: Boolean = filo.getBoolean("kafka.client.consumer.enable.auto.commit")

  val streamsConfig: Map[String, java.lang.Object] = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> selfAddress,
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
      StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> "1000",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")

  def consumerConfig: Map[String, AnyRef] = Map(
    ConsumerConfig.CLIENT_ID_CONFIG -> selfAddress,
    CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> ConsumerKeyDeserializerClass,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> ConsumerValueDeserializerClass,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> ConsumerAutoOffsetReset,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> new java.lang.Boolean(ConsumerEnableAutoCommit))

}

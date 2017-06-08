package filodb.kafka

import java.net.InetAddress
import java.util.{Properties => JProperties}

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

// WIP
final class PublisherSettings extends KafkaSettings {
  import filodb.implicits._

  val ProducerKeySerializerClass: Class[_] =
    createClass(filo.getString("kafka.client.producer.key.serializer"),
      classOf[StringSerializer].getCanonicalName)

  val ProducerValueSerializerClass: Class[_] =
    createClass(filo.getString("kafka.client.producer.value.serializer"),
      classOf[StringSerializer].getCanonicalName)

  def producerConfig: JProperties = {
    val c: Map[String, AnyRef] = secureConfig ++ Map(
      CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> BootstrapServers,
      ProducerConfig.MAX_BLOCK_MS_CONFIG -> "80000",
      ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "80000",
      ProducerConfig.ACKS_CONFIG -> "1", // ack=1 from partition leader only, test:"all" slowest, safest
      ProducerConfig.CLIENT_ID_CONFIG -> InetAddress.getLocalHost.getHostAddress,
      ProducerConfig.BUFFER_MEMORY_CONFIG -> "67108864",
      ProducerConfig.BATCH_SIZE_CONFIG -> "8196",
      ProducerConfig.RETRIES_CONFIG -> "3",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> ProducerKeySerializerClass,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> ProducerValueSerializerClass)

    c.asProps
  }
}

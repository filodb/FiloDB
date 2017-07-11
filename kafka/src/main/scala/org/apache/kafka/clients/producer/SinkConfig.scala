package org.apache.kafka.clients.producer

import scala.collection.JavaConverters._

import org.apache.kafka.common.serialization.LongSerializer

import filodb.kafka.MergeableConfig

/** Ensures the configurations not explicitly overwritten by the user
  * will always be the current Kafka defaults, based on the version
  * of the Kafka client indicated in the build.
  *
  * INTERNAL API.
  *
  * @param bootstrapServers the kafka cluster host:port list to use
  *
  * @param clientId the client Id for the consumer instance
  *
  * @param provided the user-provided configurations
  */
final class SinkConfig(bootstrapServers: String,
                       clientId: String,
                       provided: Map[String, AnyRef]
                      ) extends MergeableConfig(provided) {

  override def kafkaConfig: Map[String, AnyRef] = {
    import ProducerConfig._
    val producer = filter("producer")

    require(producer.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).isDefined,
      s"'${ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG}' must be defined.")

    val config = producer ++ Map(
      BOOTSTRAP_SERVERS_CONFIG -> producer.getOrElse(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
      CLIENT_ID_CONFIG -> producer.getOrElse(CLIENT_ID_CONFIG, clientId),
      KEY_SERIALIZER_CLASS_CONFIG -> classOf[LongSerializer].getName)

    new ProducerConfig(config.asJava).values
      .asScala.toMap.map(valueTyped).map {
      case (key, value) => key -> config.getOrElse(key, value)
    }
  }
}

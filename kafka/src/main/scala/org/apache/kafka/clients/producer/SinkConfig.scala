package org.apache.kafka.clients.producer

import scala.collection.JavaConverters._

import filodb.kafka.MergeableConfig

/** Ensures the configurations not explicitly overwritten by the user
  * will always be the current Kafka defaults, based on the version
  * of the Kafka client indicated in the build.
  *
  * INTERNAL API.
  *
  * @param bootstrapServers the kafka cluster host:port list to use
  * @param clientId the client Id for the producer instance
  * @param provided the user-provided configurations
  */
final class SinkConfig(bootstrapServers: String,
                       clientId: String,
                       provided: Map[String, AnyRef]
                      ) extends MergeableConfig(bootstrapServers, clientId, provided, "producer") {

  override def kafkaConfig: Map[String, AnyRef] = {
    import ProducerConfig._

    require(provided.get(VALUE_SERIALIZER_CLASS_CONFIG).isDefined,
      s"'$VALUE_SERIALIZER_CLASS_CONFIG' must be defined.")

    val config = commonConfig ++ provided

    new ProducerConfig(config.asJava).values
      .asScala.toMap.map(valueTyped).map {
      case (key, value) => key -> config.getOrElse(key, value)
    }
  }
}

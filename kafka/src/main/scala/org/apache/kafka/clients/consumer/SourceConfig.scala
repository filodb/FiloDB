package org.apache.kafka.clients.consumer

import scala.collection.JavaConverters._

import filodb.kafka.MergeableConfig

/** Ensures the configurations not explicitly overwritten by the user
  * will always be the current Kafka defaults, based on the version
  * of the Kafka client indicated in the build.
  *
  * INTERNAL API.
  *
  * @param bootstrapServers the kafka cluster host:port list to use
  * @param clientId         the client Id for the consumer instance
  * @param provided         the user-provided configurations
  */
final class SourceConfig(bootstrapServers: String,
                         clientId: String,
                         provided: Map[String, AnyRef]
                        ) extends MergeableConfig(bootstrapServers, clientId, provided, "consumer") {

  override def kafkaConfig: Map[String, AnyRef] = {
    import ConsumerConfig._

    require(provided.get(VALUE_DESERIALIZER_CLASS_CONFIG).isDefined,
      "'value.deserializer' must be configured.")
    require(provided.get(AUTO_OFFSET_RESET_CONFIG).isDefined,
      "'auto.offset.reset' must be configured.")

    val config = commonConfig ++ provided ++ Map(
      GROUP_ID_CONFIG -> s"$clientId-${System.nanoTime}")

    new ConsumerConfig(config.asJava).values
      .asScala.toMap.map(valueTyped).map {
      case (key, value) => key -> config.getOrElse(key, value)
    }
  }
}

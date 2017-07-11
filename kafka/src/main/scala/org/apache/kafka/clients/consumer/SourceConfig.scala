package org.apache.kafka.clients.consumer

import scala.collection.JavaConverters._

import org.apache.kafka.common.serialization.LongDeserializer

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
final class SourceConfig(bootstrapServers: String,
                         clientId: String,
                         provided: Map[String, AnyRef]
                        ) extends MergeableConfig(provided) {

  override def kafkaConfig: Map[String, AnyRef] = {
    import ConsumerConfig._

    val consumer = filter("consumer")
    require(consumer.get(VALUE_DESERIALIZER_CLASS_CONFIG).isDefined,
      "'value.deserializer' must be defined.")
    require(consumer.get(AUTO_OFFSET_RESET_CONFIG).isDefined,
      "'auto.offset.reset' must be defined.")
    require(consumer.get(GROUP_ID_CONFIG).isEmpty,
      "'group.id' must be empty.")

    val config = consumer ++ Map(
      BOOTSTRAP_SERVERS_CONFIG ->
        consumer.getOrElse(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
      CLIENT_ID_CONFIG ->
        consumer.getOrElse(CLIENT_ID_CONFIG, clientId),
      KEY_DESERIALIZER_CLASS_CONFIG -> consumer.getOrElse(KEY_DESERIALIZER_CLASS_CONFIG,
        classOf[LongDeserializer].getName))

    new ConsumerConfig(config.asJava).values
      .asScala.toMap.map(valueTyped).map {
      case (key, value) => key -> config.getOrElse(key, value)
    }
  }
}

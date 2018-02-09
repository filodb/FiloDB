package filodb.timeseries

import java.lang.{Long => JLong}

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable}
import monix.kafka.config.AutoOffsetReset

/**
 * To run the test consumer, pass the source config path as the first arg
 */
object TestTimeseriesConsumer extends StrictLogging {
  implicit val io = Scheduler.io("kafka-consumer")

  def main(args: Array[String]): Unit = {
    val sourceConfig = ConfigFactory.parseFile(new java.io.File(args(0)))
    val topicName = sourceConfig.getString("sourceconfig.filo-topic-name")

    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = sourceConfig.getString("sourceconfig.bootstrap.servers").split(',').toList,
      groupId = "timeseries-source-consumer",
      autoOffsetReset = AutoOffsetReset.Latest
    )
    val consumer = KafkaConsumerObservable[JLong, String](consumerCfg, List(topicName))

    logger.info(s"Started consuming messages from topic $topicName")
    val task = consumer.map { record =>
      logger.info(s"Got this message from kafka partition ${record.partition} ==>  ${record.value}")
    }.subscribe()

    Thread.sleep(Long.MaxValue)
  }


}

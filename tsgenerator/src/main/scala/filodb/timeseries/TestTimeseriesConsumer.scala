package filodb.timeseries

import java.lang.{Long => JLong}

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.kafka.config.AutoOffsetReset
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable}

object TestTimeseriesConsumer extends StrictLogging {


  implicit val io = Scheduler.io("kafka-consumer")

  def main(args: Array[String]): Unit = {

    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = List(TestTimeseriesProducer.kafkaServer),
      groupId = "timeseries-source-consumer",
      autoOffsetReset = AutoOffsetReset.Latest
    )
    val consumer = KafkaConsumerObservable[JLong, String](consumerCfg, List(TestTimeseriesProducer.topicName))

    logger.info(s"Started consuming messages from topic ${TestTimeseriesProducer.topicName}")
    val task = consumer.map { record =>
      logger.info(s"Got this message from kafka partition ${record.partition} ==>  ${record.value}")
    }.subscribe()

    Thread.sleep(Long.MaxValue)
  }


}

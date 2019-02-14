package filodb.kafka

import java.lang.{Long => JLong}

import scala.collection.JavaConverters._
import scala.concurrent.Future

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.serialization.{ByteArraySerializer, LongSerializer}

import filodb.core.{Response, Success}
import filodb.core.downsample.DownsamplePublisher

class KafkaDownsamplePublisher(downsampleConfig: Config) extends DownsamplePublisher with StrictLogging {

  private val kafkaConfig = propsFromConfig(downsampleConfig.getConfig("publisher-config.kafka"))

  private val topics: Map[Int, String] = downsampleConfig.getConfig("publisher-config.topics")
    .entrySet().asScala.map { e => e.getKey.toInt -> e.getValue.unwrapped().toString }.toMap

  private var producer: KafkaProducer[JLong, Array[Byte]] = _

  override def publish(shardNum: Int, resolution: Int, records: Seq[Array[Byte]]): Future[Response] = {
    topics.get(resolution) match {
      case Some(topic) =>
        records.foreach { bytes =>
          val rec = new ProducerRecord[JLong, Array[Byte]](topic, shardNum, shardNum.toLong: JLong,
            bytes)
          producer.send(rec)
        }
        Future.successful(Success)
      case None =>
        Future.failed(new IllegalArgumentException(s"Unregistered resolution $resolution"))
    }
  }

  def propsFromConfig(config: Config): Map[String, Object] = {

    val map = config.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    }).toMap

    // immutable properties to be overwritten
    map ++ Map( "value.serializer" -> classOf[ByteArraySerializer],
                "key.serializer" -> classOf[LongSerializer],
                "partitioner.class" -> classOf[DefaultPartitioner])
  }

  override def start(): Unit = {
    logger.info(s"Starting Kafka Downsampling Publisher. Will be publishing to $topics with config: $kafkaConfig")
    producer =  new KafkaProducer(kafkaConfig.asJava)
  }

  override def stop(): Unit = {
    logger.info("Stopping Kafka Downsampling Publisher")
    producer.close()
  }
}

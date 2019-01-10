package filodb.kafka

import java.lang.{Long => JLong}

import scala.concurrent.Future

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.kafka.{KafkaProducerConfig, KafkaProducerSink}
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord

import filodb.core.{DataDropped, Response, Success}
import filodb.core.downsample.DownsamplePublisher

class KafkaDownsamplePublisher(sourceConf: Config, topics: Map[Int, String])
                              (implicit io: Scheduler) extends DownsamplePublisher with StrictLogging {

  val producerCfg = KafkaProducerConfig.default.copy(
    bootstrapServers = sourceConf.getString("sourceconfig.bootstrap.servers").split(',').toList
  )

  private val producer = KafkaProducerSink[JLong, Array[Byte]](producerCfg, io)

  override def publish(shardNum: Int, resolution: Int, records: Seq[Array[Byte]]): Future[Response] = {
    topics.get(resolution) match {
      case Some(topic) =>
        // TODO with this there is one connection per publish... ugh
        // We could place records in a queue and return sooner but this is susceptible to data loss without ack.
        // Need to implement sending back ack reliably for checkpoint to happen at caller,
        // without closing connections.
        Observable.now(records).map { recs =>
          recs.map(bytes => new ProducerRecord[JLong, Array[Byte]](topic, shardNum, shardNum.toLong: JLong, bytes))
        }.consumeWith(producer).map(_ => Success).runAsync
      case None =>
        logger.error(s"Got request to publish at unregistered resolution $resolution")
        Future(DataDropped)
    }
  }
}

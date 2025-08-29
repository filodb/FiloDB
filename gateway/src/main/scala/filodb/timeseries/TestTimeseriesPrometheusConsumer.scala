package filodb.timeseries

import java.lang.{Long => JLong}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task.deferFuture
import monix.execution.Scheduler
import monix.execution.exceptions.UpstreamTimeoutException
import monix.kafka.{KafkaConsumerConfig, KafkaConsumerObservable}
import monix.kafka.config.AutoOffsetReset
import monix.reactive.Observable
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Deserializer, LongDeserializer}
import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException

import filodb.core.binaryrecord2.RecordContainer
import filodb.core.metadata.Schema
import filodb.core.metadata.Schemas.{gauge, promCounter}
import filodb.kafka.RecordContainerDeserializer
import filodb.memory.format.RowReader

object TestTimeseriesPrometheusConsumer extends StrictLogging {

  private class ConsumerOptions(args: Seq[String]) extends ScallopConf(args) {
    val sourceConfigPath = trailArg[String](descr = "Path to source config, eg conf/timeseries-dev-source.conf")
    val genGaugeData = toggle(noshort = true, descrYes = "Consume Prometheus gauge-schema test data")
    val genCounterData = toggle(noshort = true, descrYes = "Consume Prometheus counter-schema test data")

    override def onError(e: Throwable): Unit = e match {
      // Intercept and ignore only the "Unknown option" error
      case ScallopException(message) if message.startsWith("Unknown option") =>
      // For all other errors, fall back to the default behavior
      case other => super.onError(other)
    }

    verify()
  }

  private case class PrometheusMetric(metric: String, labels: Map[String, String], value: Double)

  implicit val system: ActorSystem = ActorSystem("KafkaToPrometheus")
  implicit val io: Scheduler = Scheduler.io("kafka-consumer")
  implicit val keyDeserializer: Deserializer[JLong] = new LongDeserializer().asInstanceOf[Deserializer[JLong]]
  implicit val valueDeserializer: Deserializer[RecordContainer] = new RecordContainerDeserializer

  def main(args: Array[String]): Unit = {
    val opts = new ConsumerOptions(args)

    val schema = opts match {
      case o if o.genCounterData.getOrElse(false) => promCounter
      case o if o.genGaugeData.getOrElse(false) => gauge
      case _ => gauge // Default to gauge
    }

    logger.info(s"Configured to use schema: '$schema'")

    val sourceConfig = ConfigFactory.parseFile(new java.io.File(opts.sourceConfigPath()))
    val topicName = sourceConfig.getString("sourceconfig.filo-topic-name")

    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = sourceConfig.getString("sourceconfig.bootstrap.servers").split(',').toList,
      groupId = "timeseries-prometheus-consumer",
      autoOffsetReset = AutoOffsetReset.Latest,
      properties = Map(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[LongDeserializer].getName,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[RecordContainerDeserializer].getName
      )
    )

    val consumer = KafkaConsumerObservable[JLong, RecordContainer](consumerCfg, List(topicName))

    logger.info(s"Started consuming messages from topic '$topicName'")

    consumer.concatMap { record =>
        val container = record.value()
        val promMetrics = recordContainerToPrometheusMetric(container, schema)
        Observable.fromIterable(promMetrics)
      }
      .bufferTumbling(1000)
      .map(mapPrometheusMetricsToStrings)
      .mapEval(batch => deferFuture(pushToPrometheus(batch)))
      .timeoutOnSlowUpstream(10.seconds)
      .foreachL(res => logger.info(s"Batch processing completed with result : $res"))
      .runAsync { result =>
        // we want to finish the consumer process after we read all data and ingest to prometheus
        result match {
          case Right(_) =>
          case Left(_: UpstreamTimeoutException) =>
            logger.info(s"Consumer completed successfully after timeout")
          case Left(e) =>
            logger.error(s"Consumer failed with exception $e")
        }
        sys.exit(0)
      }
  }

  private def mapPrometheusMetricsToStrings(metrics: Seq[PrometheusMetric]) = {
    val deduplicated = metrics
      .groupBy(metric => (metric.metric, metric.labels))
      .map { case (_, metrics) => metrics.last }
    deduplicated.map { metric =>
      val labelsStr = metric.labels.map { case (k, v) => s"""$k="$v"""" }.mkString(",")
      s"""${metric.metric}{$labelsStr} ${metric.value}"""
    }.toSeq
  }

  // supports only for gauge/counter
  private def recordContainerToPrometheusMetric(container: RecordContainer, schema: Schema): Seq[PrometheusMetric] = {
    val iterator = container.iterate(schema.ingestionSchema)
    val valueColIdx = schema.ingestionSchema.columns.indexWhere(_.name == "value")
    val metricNameIdx = schema.ingestionSchema.columns.indexWhere(_.name == "_metric_")
    val tagsIdx = schema.ingestionSchema.columns.indexWhere(_.name == "tags")
    iterator.map { row: RowReader =>
      val value = row.getDouble(valueColIdx)
      val metricName = row.getString(metricNameIdx)
      val tagsMap = row.getAny(tagsIdx).asInstanceOf[Map[String, String]]
      PrometheusMetric(metricName, tagsMap, value)
    }.toSeq
  }

  private def pushToPrometheus(batch: Seq[String]): Future[HttpResponse] = {
    val entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, batch.mkString("", "\n", "\n"))
    val request = HttpRequest(method = HttpMethods.POST, uri = "http://localhost:31094/metrics/job/prometheus",
      entity = entity)
    Http().singleRequest(request)
  }
}

package filodb.timeseries

import java.lang.{Long => JLong}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.util.ByteString
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
import org.xerial.snappy.Snappy

import filodb.core.binaryrecord2.{BinaryRecordRowReader, RecordContainer, RecordSchema}
import filodb.core.metadata.{Schema, Schemas}
import filodb.core.metadata.Schemas.global
import filodb.gateway.remote.remote_storage.{LabelPair, Sample, TimeSeries, WriteRequest}
import filodb.kafka.RecordContainerDeserializer
import filodb.memory.BinaryRegionConsumer

object TestTimeseriesPrometheusConsumer extends StrictLogging {

  private case class PrometheusMetric(metric: String, labels: Map[String, String], value: Double, timestamp: Long)

  implicit val system: ActorSystem = ActorSystem("KafkaToPrometheus")
  implicit val io: Scheduler = Scheduler.io("kafka-consumer")
  implicit val keyDeserializer: Deserializer[JLong] = new LongDeserializer().asInstanceOf[Deserializer[JLong]]
  implicit val valueDeserializer: Deserializer[RecordContainer] = new RecordContainerDeserializer

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      logger.error("A path to a source config file must be provided as an argument.")
      sys.exit(1)
    }
    val sourceConfigPath = args(0)
    logger.info(s"Using source config file: $sourceConfigPath")


    val sourceConfig = ConfigFactory.parseFile(new java.io.File(sourceConfigPath)).resolve()
    val topicName = sourceConfig.getString("sourceconfig.filo-topic-name")

    val consumerCfg = KafkaConsumerConfig.default.copy(
      bootstrapServers = sourceConfig.getStringList("sourceconfig.bootstrap.servers").asScala.toList,
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
        val promMetrics = recordContainerToPrometheusMetric(container)
        Observable.fromIterable(promMetrics)
      }
      .bufferTumbling(1000)
      .map(prometheusMetricToTimeSeries)
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


  private def prometheusMetricToTimeSeries(metrics: Seq[PrometheusMetric]): Seq[TimeSeries] = {
    metrics.groupBy(m => (m.metric, m.labels))
      .map { case ((metricName, tags), groupedMetrics) =>
        val labels = (tags + ("__name__" -> metricName))
          .map { case (k, v) => LabelPair(Some(k), Some(v)) }
          .toVector
        val samples = groupedMetrics.map(m => Sample(Some(m.value), Some(m.timestamp)))
        TimeSeries(labels, samples)
      }.toSeq
  }

  private def processHistogramRecord(reader: BinaryRecordRowReader, schema: Schema,
                                     metricNameIdx: Int, tagsIdx: Int, tsColIdx: Int): Seq[PrometheusMetric] = {
    val sumColIdx = schema.ingestionSchema.columns.indexWhere(_.name == "sum")
    val countColIdx = schema.ingestionSchema.columns.indexWhere(_.name == "count")
    val histColIdx = schema.ingestionSchema.columns.indexWhere(_.name == "h")

    if (sumColIdx < 0 || countColIdx < 0 || histColIdx < 0) {
      logger.error(s"Histogram schema ${schema.name} is missing one of sum, count, or h columns")
      Seq.empty
    } else {
      val timestamp = reader.getLong(tsColIdx)
      val baseMetricName = reader.getString(metricNameIdx)
      val tagsMap = reader.getAny(tagsIdx).asInstanceOf[Map[String, String]]

      val sumValue = reader.getDouble(sumColIdx)
      val countValue = reader.getDouble(countColIdx)
      val hist = reader.getHistogram(histColIdx)

      val sumMetric = PrometheusMetric(s"${baseMetricName}_sum", tagsMap, sumValue, timestamp)
      val countMetric = PrometheusMetric(s"${baseMetricName}_count", tagsMap, countValue, timestamp)

      val bucketMetrics = (0 until hist.numBuckets).map { i =>
        val le = if (hist.bucketTop(i).isPosInfinity) "+Inf" else hist.bucketTop(i).toString
        val bucketTags = tagsMap + ("le" -> le)
        PrometheusMetric(s"${baseMetricName}_bucket", bucketTags, hist.bucketValue(i), timestamp)
      }
      Seq(sumMetric, countMetric) ++ bucketMetrics
    }
  }

  private def processRecord(reader: BinaryRecordRowReader, schema: Schema): Seq[PrometheusMetric] = {
    val metricNameIdx = schema.ingestionSchema.columns.indexWhere(_.name == "_metric_")
    val tagsIdx = schema.ingestionSchema.columns.indexWhere(_.name == "tags")
    val tsColIdx = schema.ingestionSchema.columns.indexWhere(_.name == "timestamp")

    schema match {
      case s if s.name.contains("histogram") => // Generic check for any histogram schema
        processHistogramRecord(reader, schema, metricNameIdx, tagsIdx, tsColIdx)

      case Schemas.gauge | Schemas.promCounter =>
        val valueColName = if (schema == Schemas.gauge) "value" else "count"
        val valueColIdx = schema.ingestionSchema.columns.indexWhere(_.name == valueColName)
        if (valueColIdx < 0) {
          logger.error(s"Schema ${schema.name} is missing required value column '$valueColName'")
          Seq.empty
        } else {
          val timestamp = reader.getLong(tsColIdx)
          val value = reader.getDouble(valueColIdx)
          val metricName = reader.getString(metricNameIdx)
          val tagsMap = reader.getAny(tagsIdx).asInstanceOf[Map[String, String]]
          Seq(PrometheusMetric(metricName, tagsMap, value, timestamp))
        }

      case _ =>
        logger.warn(s"Unsupported schema type: ${schema.name}. Skipping record.")
        Seq.empty
    }
  }

  private def recordContainerToPrometheusMetric(container: RecordContainer): Seq[PrometheusMetric] = {
    val metrics = scala.collection.mutable.Buffer[PrometheusMetric]()

    container.consumeRecords(new BinaryRegionConsumer {
      def onNext(base: Any, offset: Long): Unit = {
        val schemaId = RecordSchema.schemaID(base, offset)
        val schemaName = Schemas.global.schemaName(schemaId)
        val schema = global.schemas(schemaName)

        val reader = new BinaryRecordRowReader(schema.ingestionSchema, base)
        reader.recordOffset = offset

        metrics ++= processRecord(reader, schema)
      }
    })

    metrics
  }

  private def pushToPrometheus(batch: Seq[TimeSeries]): Future[HttpResponse] = {
    val writeRequest = WriteRequest(batch)
    val serialized = writeRequest.toByteArray
    val compressed = Snappy.compress(serialized)

    val entity = HttpEntity(ContentType.parse("application/x-protobuf").right.get, ByteString(compressed))

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = "http://localhost:31093/api/v1/write",
      entity = entity,
      headers = List(
        RawHeader("Content-Encoding", "snappy"),
        RawHeader("X-Prometheus-Remote-Write-Version", "0.1.0")
      )
    )

    logger.info(s"Sending batch of ${batch.size} metrics to Prometheus remote write...")
    Http().singleRequest(request)
  }
}

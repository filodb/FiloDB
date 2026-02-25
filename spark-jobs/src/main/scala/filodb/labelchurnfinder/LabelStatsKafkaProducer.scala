package filodb.labelchurnfinder

import java.time.Instant
import java.util.Properties

import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
 * Distributed Kafka producer for publishing label statistics from Spark executors.
 *
 * Uses Spark's distributed processing to:
 * 1. Group labels by workspace on executors (distributed groupBy)
 * 2. Create one Kafka producer per Spark partition
 * 3. Publish messages in parallel from all executors
 *
 */
class LabelStatsKafkaProducer(config: Config) extends StrictLogging {

  import LabelChurnFinder._

  private[labelchurnfinder] val kafkaConfig = config.getConfig("labelchurnfinder.kafka")
  private[labelchurnfinder] val topic = kafkaConfig.getString("topic")
  private[labelchurnfinder] val bootstrapServers = kafkaConfig.getString("bootstrap-servers")
  private[labelchurnfinder] val mosaicPartition = config.getString("partition")

  logger.info(s"Initialized distributed Kafka producer: topic=$topic," +
    s" bootstrapServers=$bootstrapServers, partition=$mosaicPartition")

  /**
   * Publishes label statistics from a DataFrame using distributed processing.
   *
   * Uses Spark's distributed groupBy and foreachPartition to:
   * 1. Group labels by workspace on executors (distributed)
   * 2. Create one Kafka producer per partition
   * 3. Publish messages in parallel from all executors
   *
   * @param df DataFrame with columns: ws, nsGroup, label, ats1h, ats3d, ats7d,
   *           labelSketch1h, labelSketch3d, labelSketch7d
   */
  def publishLabelStats(df: DataFrame): Unit = {
    val jobTimestamp = Instant.now()

    // Broadcast config to executors (read-only, serializable)
    val broadcastTopic = df.sparkSession.sparkContext.broadcast(topic)
    val broadcastBootstrapServers = df.sparkSession.sparkContext.broadcast(bootstrapServers)
    val broadcastPartition = df.sparkSession.sparkContext.broadcast(mosaicPartition)

    logger.info(s"Starting distributed publishing to Kafka topic '$topic'")

    val groupedByWorkspace = groupLabelsByWorkspace(df)
    processPartitions(groupedByWorkspace, broadcastTopic, broadcastBootstrapServers, broadcastPartition, jobTimestamp)

    logger.info(s"Distributed publishing complete for topic '$topic'")
  }

  /**
   * Groups labels by workspace using Spark's distributed groupBy.
   * Keeps data on executors and distributes workspaces across partitions.
   */
  private[labelchurnfinder] def groupLabelsByWorkspace(df: DataFrame): DataFrame = {
    df.groupBy(WsCol, NsGroupCol)
      .agg(
        collect_list(
          struct(
            col(LabelCol),
            col(Ats1hWithLabelCol),
            col(Ats3dWithLabelCol),
            col(Ats7dWithLabelCol),
            col(LabelSketch1hCol),
            col(LabelSketch3dCol),
            col(LabelSketch7dCol)
          )
        ).alias("labels")
      )
  }

  /**
   * Processes each Spark partition in parallel on executors.
   * Creates one Kafka producer per partition.
   */
  private def processPartitions(
    groupedData: DataFrame,
    broadcastTopic: org.apache.spark.broadcast.Broadcast[String],
    broadcastBootstrapServers: org.apache.spark.broadcast.Broadcast[String],
    broadcastPartition: org.apache.spark.broadcast.Broadcast[String],
    jobTimestamp: Instant
  ): Unit = {
    groupedData.foreachPartition { (partition: Iterator[Row]) =>
      val localProducer = LabelStatsKafkaProducer.createKafkaProducer(broadcastBootstrapServers.value)

      try {
        partition.foreach { row =>
          LabelStatsKafkaProducer.publishRow(row, localProducer,
            broadcastTopic.value, broadcastPartition.value, jobTimestamp)
        }
        localProducer.flush()
      } finally {
        localProducer.close()
      }
    }
  }
}

/**
 * Companion object containing stateless helper methods for Kafka publishing.
 * These methods are called from Spark executor closures and must not capture
 * any references to the outer LabelStatsKafkaProducer instance.
 */
object LabelStatsKafkaProducer {

  import LabelChurnFinder._
  import LabelStatisticsMessage._

  // Executor-side logger (transient to avoid serialization)
  @transient lazy val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /**
   * Creates a Kafka producer with configuration optimized for periodic batch jobs.
   *
   * Configuration rationale:
   * - Message size: 30-80KB compressed (workspace + all labels + HLL sketches)
   * - Workload: Periodic Spark batch job (not real-time streaming)
   * - Concurrency: N producers (one per Spark partition)
   * - Priority: Reliability and throughput over latency
   *
   * Key settings:
   * - 128KB batch size: Fits 1-3 workspace messages per batch
   * - 200ms linger: Batch multiple workspaces (no real-time requirement)
   * - gzip compression: 65-75% ratio for large JSON messages
   * - Idempotence: Prevents duplicates on retry
   * - 16MB buffer: Conservative for N concurrent producers on executors
   */
  private def createKafkaProducer(bootstrapServers: String): KafkaProducer[String, String] = {
    val producerConfig = new Properties()

    // Connection and serialization
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Reliability - critical for label statistics
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")                    // Wait for all replicas
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, "5")                   // Higher retries for batch job
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")     // Prevent duplicates on retry

    // Batching - optimize for throughput over latency
    producerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072")           // 128KB batches (1-3 messages)
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "200")               // Wait 200ms to batch workspaces

    // Compression - gzip works well for large JSON messages with Base64 data
    producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")

    // Timeouts - balanced for batch job with large messages
    producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000")    // 30s request timeout
    producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000")          // 60s max block time

    // Memory - conservative for multiple concurrent Spark producers
    producerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "16777216")      // 16MB per producer

    new KafkaProducer[String, String](producerConfig)
  }

  /**
   * Publishes a single row (workspace) to Kafka.
   * Extracts labels, builds message, and sends asynchronously.
   */
  private def publishRow(
    row: Row,
    producer: KafkaProducer[String, String],
    topic: String,
    partition: String,
    jobTimestamp: Instant
  ): Unit = {
    val workspaceId = row.getAs[String](WsCol)
    val nsGroup = row.getAs[String](NsGroupCol)
    val labelsArray = row.getAs[Seq[Row]]("labels")

    val labels = buildLabelsFromRows(labelsArray)
    val message = LabelStatisticsMessage(
      workspaceId = workspaceId,
      mosaicPartition = partition,
      nsGroup = nsGroup,
      jobTimestamp = jobTimestamp,
      labels = labels
    )

    sendToKafka(producer, topic, workspaceId, message, partition)
  }

  /**
   * Builds label statistics from Spark Row data.
   */
  private[labelchurnfinder] def buildLabelsFromRows(labelsArray: Seq[Row]): Seq[LabelStatDto] = {
    labelsArray.map { labelRow =>
      LabelStatDto(
        labelName = labelRow.getAs[String](0),
        ats1h = labelRow.getAs[Long](1),
        ats3d = labelRow.getAs[Long](2),
        ats7d = labelRow.getAs[Long](3),
        sketchLabelCard1h = sketchToBase64(labelRow.getAs[Array[Byte]](4)),
        sketchLabelCard3d = sketchToBase64(labelRow.getAs[Array[Byte]](5)),
        sketchLabelCard7d = sketchToBase64(labelRow.getAs[Array[Byte]](6))
      )
    }
  }

  /**
   * Sends a message to Kafka with error handling callback.
   */
  private def sendToKafka(
    producer: KafkaProducer[String, String],
    topic: String,
    key: String,
    message: LabelStatisticsMessage,
    partition: String
  ): Unit = {
    val record = new ProducerRecord[String, String](topic, key, message.asJson.noSpaces)

    producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
      if (exception != null) {
        logger.error(s"Failed to publish for workspace=$key, partition=$partition: ${exception.getMessage}")
      }
    })
  }
}

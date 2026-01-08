package filodb.gateway

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import monix.kafka._
import monix.reactive.Observable
import net.ceedubs.ficus.Ficus._
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory, Channels}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.ssl.SslContext
import org.jboss.netty.handler.ssl.util.SelfSignedCertificate
import org.jctools.queues.MpscGrowableArrayQueue
import org.rogach.scallop._
import org.rogach.scallop.ArgType
import org.rogach.scallop.ValueConverter

import filodb.coordinator.{FilodbSettings, ShardMapper, StoreFactory}
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.core.metadata.Schemas.{deltaCounter, deltaHistogram,
  gauge, otelCumulativeHistogram, otelDeltaHistogram, otelExpDeltaHistogram, promCounter, promHistogram}
import filodb.core.metrics.FilodbMetrics
import filodb.gateway.conversion._
import filodb.memory.MemFactory
import filodb.timeseries.TestTimeseriesProducer


/**
 * Gateway server to ingest source streams of data, shard, batch, and write output to Kafka
 * built using high performance Netty TCP code
 *
 * It usually takes one arg: the source config file which contains # Kafka partitions/shards and other config
 * Also pass in -Dconfig.file=.... as usual, with a config that points to the dataset metadata.
 * For local setups, simply run `./dev-gateway.sh`.
 * For help pass in `--help`.
 *
 * NOTE: set `kamon.prometheus.embedded-server.port` to avoid conflicting with FiloDB itself.
 *
 * There are options that can be used to generate test data, such as `--gen-hist-data`.  The -n and -p options can
 * also be used together to control the # of samples per series and # of time series.
 * To generate Histogram schema test data, one must create the following dataset:
 *   ./filo-cli -Dconfig.file=conf/timeseries-filodb-server.conf  --command create --dataset histogram \
 *      --dataColumns timestamp:ts,sum:long,count:long,h:hist:counter=true --partitionColumns metric:string,tags:map \
 *      --shardKeyColumns metric --metricColumn metric
 * create a Kafka topic:
 *   kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic histogram-dev
 * and use the `conf/histogram-dev-source.conf` config file.
 * Oh, and you have to observe on shards 1 and 3.
 */
object GatewayServer extends StrictLogging {
  Kamon.init

  // Get global configuration using universal FiloDB/Akka-based config
  val settings = new FilodbSettings()
  val config = settings.allConfig
  val storeFactory = StoreFactory(settings, Scheduler.io())

  // ==== Metrics ====
  val numInfluxMessages = FilodbMetrics.counter("num-influx-messages")
  val numInfluxParseErrors = FilodbMetrics.counter("num-influx-parse-errors")
  val numDroppedMessages = FilodbMetrics.counter("num-dropped-messages")
  val numContainersSent = FilodbMetrics.counter("num-containers-sent")

  object GatewayOptions {
    /**
     * A custom Scallop ValueConverter for an option that can be present with or without a value.
     * This allows a CLI syntax like `--my-option` or `--my-option value`.
     */
    implicit val optionalStringConverter: ValueConverter[Option[String]] = new ValueConverter[Option[String]] {
      def parse(s: List[(String, List[String])]): Either[String, Option[Option[String]]] = {
        s match {
          case (_, Nil) :: Nil       => Right(Some(None)) // present, no value
          case (_, v :: Nil) :: Nil  => Right(Some(Some(v))) // present, with value
          case Nil                   => Right(None) // not present
          case _                     => Left("Argument for this option can be supplied only once.")
        }
      }
      override val argType: ArgType.V = ArgType.LIST
    }
  }

  // Most options are for generating test data
  class GatewayOptions(args: Seq[String]) extends ScallopConf(args) {

    val samplesPerSeries = opt[Int](short = 'n', default = Some(100),
      descr = "# of samples per time series")
    val numSeriesPerMetric = opt[Int](short = 'p', default = Some(20), descr = "# of total time series per metric")
    val sourceConfigPath = trailArg[String](descr = "Path to source config, eg conf/timeseries-dev-source.conf")
    val genHistData = toggle(name = "gen-hist-data", descrYes = "Generate Prometheus hist-schema test data and exit")
    val genDeltaHistData = toggle(noshort = true, descrYes = "Generate delta-histogram-schema test data and exit")
    val genOtelCumulativeHistData = toggle(noshort = true,
      descrYes = "Generate otel-cumulative-histogram schema test data and exit")
    val genOtelDeltaHistData = toggle(noshort = true,
      descrYes = "Generate otel-delta-histogram schema test data and exit")
    val genOtelExpDeltaHistData = toggle(noshort = true,
      descrYes = "Generate otel-exponential-delta-histogram schema test data and exit")
    val genGaugeData = toggle(name = "gen-gauge-data", descrYes = "Generate Prometheus gauge-schema test data and exit")
    val genCounterData = toggle(name = "gen-counter-data",
      descrYes = "Generate Prometheus counter-schema test data and exit")
    val genDeltaCounterData = toggle(noshort = true, descrYes = "Generate delta-counter-schema test data and exit")
    val numMetrics = opt[Int](short = 'm', default = Some(1), descr = "# of metrics - use 2 to test binary joins")
    val publishIntervalSecs = opt[Int](short = 'i', default = Some(10), descr = "Publish interval between samples")
    val nameSpace = opt[String](name = "ns", default = Some("App-0"), descr = "FiloDB ingestion namespace")
    val workSpace = opt[String](name = "ws", default = Some("demo"), descr = "FiloDB ingestion workspace")

    verify()
  }

  //scalastyle:off method.length
  def main(args: Array[String]): Unit = {
    val userOpts = new GatewayOptions(args.toIndexedSeq)
    val numSamples = userOpts.samplesPerSeries() * userOpts.numSeriesPerMetric() * userOpts.numMetrics()
    val numSeries = userOpts.numSeriesPerMetric()

    val sourceConfig = ConfigFactory.parseFile(new java.io.File(userOpts.sourceConfigPath())).resolve()
    val numShards = sourceConfig.getInt("num-shards")

    val dataset = settings.datasetFromStream(sourceConfig)

    // NOTE: the spread MUST match the default spread used in the HTTP module for consistency between querying
    //       and ingestion sharding
    val spread = config.getInt("filodb.spread-default")
    val shardMapper = new ShardMapper(numShards)
    val queueFullWait = config.as[FiniteDuration]("gateway.queue-full-wait").toMillis

    val (shardQueues, containerStream) = shardingPipeline(config, numShards, dataset)

    def calcShardAndQueueHandler(buf: ChannelBuffer): Unit = {
      val initIndex = buf.readerIndex
      val len = buf.readableBytes
      numInfluxMessages.increment()
      InfluxProtocolParser.parse(buf) map { record =>
        logger.trace(s"Enqueuing: $record")
        val shard = shardMapper.ingestionShard(record.shardKeyHash, record.partitionKeyHash, spread)
        if (!shardQueues(shard).offer(record)) {
          // Prioritize recent data.  This means dropping messages when full, so new data may have a chance.
          logger.warn(s"Queue for shard=$shard is full.  Dropping data.")
          numDroppedMessages.increment()
          // Thread sleep queueFullWait
        }
      } getOrElse {
        numInfluxParseErrors.increment()
        logger.warn(s"Could not parse:\n${buf.toString(initIndex, len, Charset.defaultCharset)}")
      }
    }

    // TODO: allow configurable sinks, maybe multiple sinks for say writing to multiple Kafka clusters/DCs
    setupKafkaProducer(sourceConfig, containerStream)

    case class GeneratorConfig(metricType: Boolean,
                               name: String,
                               generator: () => Stream[InputRecord])

    val genHist = userOpts.genHistData.getOrElse(false)
    val genGaugeData = userOpts.genGaugeData.getOrElse(false)
    val genDeltaHist = userOpts.genDeltaHistData.getOrElse(false)
    val genCounterData = userOpts.genCounterData.getOrElse(false)
    val genDeltaCounterData = userOpts.genDeltaCounterData.getOrElse(false)
    val genOtelCumulativeHistData = userOpts.genOtelCumulativeHistData.getOrElse(false)
    val genOtelDeltaHistData = userOpts.genOtelDeltaHistData.getOrElse(false)
    val genOtelExpDeltaHistData = userOpts.genOtelExpDeltaHistData.getOrElse(false)

    val startTime = System.currentTimeMillis
    logger.info(s"Generating $numSamples samples starting at $startTime....")

    val allGenerators = Seq(
      GeneratorConfig(genHist, promHistogram.name,
        () => TestTimeseriesProducer.genHistogramData(startTime, numSeries, promHistogram,
          metricNameOverride = None, namespace = userOpts.nameSpace(),
          workspace = userOpts.workSpace())),
      GeneratorConfig(genOtelCumulativeHistData, otelCumulativeHistogram.name,
        () => TestTimeseriesProducer.genHistogramData(startTime, numSeries, otelCumulativeHistogram,
          metricNameOverride = None, namespace = userOpts.nameSpace(),
          workspace = userOpts.workSpace())),
      GeneratorConfig(genOtelDeltaHistData, otelDeltaHistogram.name,
        () => TestTimeseriesProducer.genHistogramData(startTime, numSeries, otelDeltaHistogram,
          metricNameOverride = None, namespace = userOpts.nameSpace(),
          workspace = userOpts.workSpace())),
      GeneratorConfig(genOtelExpDeltaHistData, otelExpDeltaHistogram.name,
        () => TestTimeseriesProducer.genHistogramData(startTime, numSeries, otelExpDeltaHistogram,
          metricNameOverride = None, namespace = userOpts.nameSpace(),
          workspace = userOpts.workSpace())),
      GeneratorConfig(genDeltaHist, deltaHistogram.name,
        () => TestTimeseriesProducer.genHistogramData(startTime, numSeries, deltaHistogram,
          metricNameOverride = None, namespace = userOpts.nameSpace(),
          workspace = userOpts.workSpace())),
      GeneratorConfig(genGaugeData, gauge.name,
        () => TestTimeseriesProducer.timeSeriesData(startTime, numSeries, userOpts.numMetrics(),
          userOpts.publishIntervalSecs(), gauge, userOpts.nameSpace(), userOpts.workSpace(),
          metricNameOverride = None)),
      GeneratorConfig(genCounterData, promCounter.name,
        () => TestTimeseriesProducer.timeSeriesCounterData(startTime, numSeries, userOpts.numMetrics(),
          userOpts.publishIntervalSecs(), userOpts.nameSpace(), userOpts.workSpace(),
          metricNameOverride = None)),
      GeneratorConfig(genDeltaCounterData, deltaCounter.name,
        () => TestTimeseriesProducer.timeSeriesData(startTime, numSeries, userOpts.numMetrics(),
          userOpts.publishIntervalSecs(), deltaCounter))
    )

    val streamsToGen = allGenerators.flatMap { config =>
      if (config.metricType) {
        logger.info(s"Adding ${config.name} data generator for $numSamples samples")
        Some(config.generator().take(numSamples))
      } else {
        None
      }
    }

    if (streamsToGen.nonEmpty) {
      val totalSamples = numSamples * streamsToGen.size
      logger.info(s"Generating a total of $totalSamples " +
        s"samples from ${streamsToGen.size} generator(s) starting at $startTime....")

      val finalObservable = Observable.fromIterable(streamsToGen.reduce(_ ++ _))

      // Use a blocking call to ensure the main thread waits for all samples to be generated and queued.
      implicit val scheduler = Scheduler.global // A scheduler is needed for runSyncUnsafe
      finalObservable.foreachL { rec =>
        val shard = shardMapper.ingestionShard(rec.shardKeyHash, rec.partitionKeyHash, spread)
        if (!shardQueues(shard).offer(rec)) {
          logger.warn(s"Queue for shard=$shard is full.  Dropping data.")
          numDroppedMessages.increment()
        }
      }.runSyncUnsafe()

      logger.info("All samples have been generated and queued.")
      // Wait for the async Kafka producer to flush the records
      Thread.sleep(10000)

      TestTimeseriesProducer.logQueryHelp(dataset.name, userOpts.numMetrics(), totalSamples, numSeries,
        startTime,
        genHist,
        genDeltaHist,
        genGaugeData,
        genCounterData,
        genOtelCumulativeHistData,
        genOtelDeltaHistData,
        genOtelExpDeltaHistData,
        userOpts.publishIntervalSecs(),
        userOpts.nameSpace(), userOpts.workSpace())
      logger.info(s"Waited for containers to be sent, exiting...")
      sys.exit(0)
    } else {
      setupTCPService(config, calcShardAndQueueHandler)
    }
  }
  //scalastyle:on method.length

  def setupTCPService(config: Config, handler: ChannelBuffer => Unit): Unit = {
    val influxPort = config.getInt("gateway.influx-port")

    // Configure SSL.
    val SSL = config.getBoolean("gateway.tcp.ssl-enabled")
    val sslCtx = if (SSL) {
      val ssc = new SelfSignedCertificate()
      Some(SslContext.newServerContext(ssc.certificate(), ssc.privateKey()))
    } else {
      None
    }

    // Configure the bootstrap.
    val bootstrap = new ServerBootstrap(
      new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()))

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      def getPipeline(): ChannelPipeline = {
        val p = Channels.pipeline();
        sslCtx.foreach { ctx => p.addLast("ssl", ctx.newHandler()) }
        p.addLast("influxProtocol", new NettySocketHandler(Some('\n'), handler));
        p
      }
    })

    val rcvBufferSize = config.getInt("gateway.tcp.netty-receive-buffer-size")
    val sendBufferSize = config.getInt("gateway.tcp.netty-send-buffer-size")
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.receiveBufferSize", rcvBufferSize)
    bootstrap.setOption("child.sendBufferSize", sendBufferSize)

    // Bind and start to accept incoming connections.
    logger.info(s"Starting GatewayServer with TCP port for Influx data at $influxPort....")
    bootstrap.bind(new InetSocketAddress(influxPort))
  }

  // Returns (Array[Queue] for shards, containerObservable)
  def shardingPipeline(config: Config, numShards: Int, dataset: Dataset):
  (Array[MpscGrowableArrayQueue[InputRecord]], Observable[(Int, Seq[Array[Byte]])]) = {
    val parallelism = config.getInt("gateway.producer-parallelism")
    val minQueueSize = config.getInt("gateway.min-queue-size")
    val maxQueueSize = config.getInt("gateway.max-queue-size")

    // Create queues and RecordBuilders, one per shard
    val shardQueues = (0 until numShards).map { _ =>
      new MpscGrowableArrayQueue[InputRecord](minQueueSize, maxQueueSize) }.toArray
    val lastSendTime = Array.fill(numShards)(0L)
    val builders = (0 until numShards).map(s => new RecordBuilder(MemFactory.onHeapFactory))
      .toArray
    val producing = Array.fill(numShards)(false)
    var curShard = 0
    // require(parallelism < numShards)

    // Create a multithreaded pipeline to read from the shard queues and populate the RecordBuilders.
    // The way it works is as follows:
    //   producing array above keeps track of which shards are being worked on at any time.
    //   The producing observable produces a stream of the next shard to work on.  If a shard is already being worked
    //    on then it will be skipped -- this ensures that a shard is never worked on in parallel
    //   Next tasks are created and executed to pull from queue and build records in a parallel pool
    //   Each Task produces (shard, Container) pairs which get flushed by the sink
    val shardIt = Iterator.from(0).map { _ =>
      while (producing(curShard)) {
        curShard = (curShard + 1) % numShards
        Thread sleep 1
      }  // else keep going.  If we have gone around just wait
      val shardToWorkOn = curShard
      producing(shardToWorkOn) = true
      curShard = (curShard + 1) % numShards
      shardToWorkOn
    }
    val containerStream = Observable.fromIteratorUnsafe(shardIt)
      .mapParallelUnordered(parallelism) { shard =>
        buildShardContainers(shard, shardQueues(shard), builders(shard), lastSendTime)
          .map { output =>
            // Mark this shard as done producing for now to allow another go
            producing(shard) = false
            output
          }
      }.filter { case (_, j) => j.nonEmpty }
    logger.info(s"Created $numShards container builder queues with $parallelism parallel workers...")
    (shardQueues, containerStream)
  }

  def buildShardContainers(shard: Int,
                           queue: MpscGrowableArrayQueue[InputRecord],
                           builder: RecordBuilder,
                           sendTime: Array[Long]): Task[(Int, Seq[Array[Byte]])] = Task.evalAsync {
    // While there are still messages in the queue and there aren't containers to send, pull and build
    while (!queue.isEmpty && builder.allContainers.length <= 1) {
      queue.poll().addToBuilder(builder)
      // TODO: add metrics
    }
    // Is there a container to send?  Or has the time since the last send been more than a second?
    // Send only full containers or if time has elapsed, send and reset current container
    val numContainers = builder.allContainers.length
    if (numContainers > 1 ||
      (numContainers > 0 && !builder.allContainers.head.isEmpty &&
        (System.currentTimeMillis - sendTime(shard)) > 1000)) {
      sendTime(shard) = System.currentTimeMillis
      val out = if (numContainers > 1) {   // First container probably full.  Send only the first container
        numContainersSent.increment(numContainers - 1)
        (shard, builder.nonCurrentContainerBytes(reset = true))
      } else {    // only one container.  Get the smallest bytes possible as its probably not full
        numContainersSent.increment()
        (shard, builder.optimalContainerBytes(reset = true))
      }
      logger.debug(s"Sending ${out._2.length} containers, ${out._2.map(_.size).sum} bytes from shard=$shard")
      out
    } else {
      (shard, Nil)
    }
  }

  def setupKafkaProducer(sourceConf: Config, containerStream: Observable[(Int, Seq[Array[Byte]])]): Future[Unit] = {
    // Now create Kafka config, sink
    // TODO: use the official KafkaIngestionStream stuff to parse the file.  This is just faster for now.
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = sourceConf.getStringList("sourceconfig.bootstrap.servers").asScala.toList
    )
    val topicName = sourceConf.getString("sourceconfig.filo-topic-name")

    implicit val io = Scheduler.io("kafka-producer")
    val sink = new KafkaContainerSink(producerCfg, topicName)
    sink.writeTask(containerStream)
      .runToFuture
      .map { _ => logger.info(s"Finished producing messages into topic $topicName") }
      // TODO: restart stream in case of failure?
      .recover { case NonFatal(e) => logger.error("Error occurred while producing messages to Kafka", e) }
  }
}
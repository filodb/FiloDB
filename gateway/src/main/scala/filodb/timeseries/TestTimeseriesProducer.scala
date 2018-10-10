package filodb.timeseries

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import monix.execution.Scheduler
import monix.kafka._
import monix.reactive.Observable
import org.rogach.scallop._

import filodb.coordinator.ShardMapper
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.gateway.KafkaContainerSink
import filodb.memory.MemFactory
import filodb.prometheus.FormatConversion

sealed trait DataOrCommand
final case class DataSample(tags: Map[String, String], timestamp: Long, value: Double) extends DataOrCommand
case object FlushCommand extends DataOrCommand

/**
  * Simple driver to produce time series data into local kafka similar. Data format is similar to
  * prometheus metric sample.
  * This is for development testing purposes only. TODO: Later evolve this to accept prometheus formats.
  *
  * Run as `java -cp classpath filodb.timeseries.TestTimeseriesProducer --help`
  *
  */
object TestTimeseriesProducer extends StrictLogging {
  import collection.JavaConverters._

  class ProducerOptions(args: Seq[String]) extends ScallopConf(args) {
    val samplesPerSeries = opt[Int](short = 'n', default = Some(100),
                                    descr="# of samples per time series")
    val startMinutesAgo = opt[Int](short='t')
    val numSeries = opt[Int](short='p', default = Some(20), descr="# of total time series")
    val sourceConfigPath = opt[String](required = true, short = 'c',
                                       descr="Path to source conf file eg conf/timeseries-dev-source.conf")
    verify()
  }

  val oneBitMask = 0x1
  val twoBitMask = 0x3
  val rand = Random
  // start from a random day in the last 5 years

  def main(args: Array[String]): Unit = {
    val conf = new ProducerOptions(args)
    val sourceConfig = ConfigFactory.parseFile(new java.io.File(conf.sourceConfigPath()))
    val numSamples = conf.samplesPerSeries() * conf.numSeries()
    val numTimeSeries = conf.numSeries()
    // to get default start time, look at numSamples and calculate a startTime that ends generation at current time
    val startMinutesAgo = conf.startMinutesAgo.toOption
      .getOrElse((numSamples.toDouble / numTimeSeries / 6).ceil.toInt )  // at 6 samples per minute

    Await.result(produceMetrics(sourceConfig, numSamples, numTimeSeries, startMinutesAgo), 1.hour)
  }

  /**
    * Produce metrics
    * @param conf the sourceConfig
    * @param numSamples number of samples to produce
    * @param numTimeSeries number of time series partitions to produce
    * @param startMinutesAgo the samples will carry a timestamp starting from these many minutes ago
    * @return
    */
  def produceMetrics(conf: Config, numSamples: Int, numTimeSeries: Int, startMinutesAgo: Long): Future[Unit] = {
    val startTime = System.currentTimeMillis() - startMinutesAgo.minutes.toMillis
    val numShards = conf.getInt("num-shards")

    // TODO: use the official KafkaIngestionStream stuff to parse the file.  This is just faster for now.
    val producerCfg = KafkaProducerConfig.default.copy(
      bootstrapServers = conf.getString("sourceconfig.bootstrap.servers").split(',').toList
    )
    val topicName = conf.getString("sourceconfig.filo-topic-name")

    logger.info(s"Started producing $numSamples messages into topic $topicName with timestamps " +
      s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago")

    implicit val io = Scheduler.io("kafka-producer")

    val shardKeys = Set("__name__", "job")  // the tag keys used to compute the shard key hash
    val batchedData = timeSeriesData(startTime, numShards, numTimeSeries)
                        .take(numSamples)
                        .grouped(128)
    val containerStream = batchSingleThreaded(Observable.fromIterator(batchedData),
                                              FormatConversion.dataset, numShards, shardKeys)
    val sink = new KafkaContainerSink(producerCfg, topicName)
    sink.writeTask(containerStream)
        .runAsync
        .map { _ =>
          logger.info(s"Finished producing $numSamples messages into topic $topicName with timestamps " +
            s"from about ${(System.currentTimeMillis() - startTime) / 1000 / 60} minutes ago at $startTime")
          val startQuery = startTime / 1000
          val endQuery = startQuery + 300
          val query =
            s"""./filo-cli '-Dakka.remote.netty.tcp.hostname=127.0.0.1' --host 127.0.0.1 --dataset timeseries """ +
            s"""--promql 'heap_usage{dc="DC0",job="App-0"}' --start $startQuery --end $endQuery --limit 15"""
          logger.info(s"Sample Query you can use: \n$query")
          val q = URLEncoder.encode("heap_usage{dc=\"DC0\",job=\"App-0\"}", StandardCharsets.UTF_8.toString)
          val url = s"http://localhost:8080/promql/timeseries/api/v1/query_range?" +
            s"query=$q&start=$startQuery&end=$endQuery&step=15"
          logger.info(s"Sample URL you can use to query: \n$url")
        }
        .recover { case NonFatal(e) =>
          logger.error("Error occurred while producing messages to Kafka", e)
        }
  }

  /**
    * Generate time series data.
    *
    * @param startTime    Start time stamp
    * @param numShards the number of shards or Kafka partitions
    * @param numTimeSeries number of instances or time series
    * @return stream of a 2-tuple (kafkaParitionId , sampleData)
    */
  def timeSeriesData(startTime: Long, numShards: Int, numTimeSeries: Int = 16): Stream[DataSample] = {
    // TODO For now, generating a (sinusoidal + gaussian) time series. Other generators more
    // closer to real world data can be added later.
    Stream.from(0).map { n =>
      val instance = n % numTimeSeries
      val dc = instance & oneBitMask
      val partition = (instance >> 1) & twoBitMask
      val app = (instance >> 3) & twoBitMask
      val host = (instance >> 4) & twoBitMask
      val timestamp = startTime + (n.toLong / numTimeSeries) * 10000 // generate 1 sample every 10s for each instance
      val value = 15 + Math.sin(n + 1) + rand.nextGaussian()

      val tags = Map("__name__" -> "heap_usage",
                     "dc"       -> s"DC$dc",
                     "job"      -> s"App-$app",
                     "partition" -> s"partition-$partition",
                     "host"     -> s"H$host",
                     "instance" -> s"Instance-$instance")
      DataSample(tags, timestamp, value)
    }
  }

  // scalastyle:off method.length
  /**
   * Batches the source data stream into separate RecordContainers per shard, single threaded,
   * using proper logic to calculate hashes correctly for shard and partition hashes.
   * @param {[Dataset]} dataset: Dataset the Dataset schema to use for encoding
   * @param {[Int]} numShards: Int the total number of shards or Kafka partitions
   * @param shardKeys The hash keys to use for shard number calculation
   */
  def batchSingleThreaded(items: Observable[Seq[DataSample]],
                          dataset: Dataset,
                          numShards: Int,
                          shardKeys: Set[String],
                          flushInterval: FiniteDuration = 1.second)
                         (implicit io: Scheduler): Observable[(Int, Seq[Array[Byte]])] = {
    val builders = (0 until numShards).map(s => new RecordBuilder(MemFactory.onHeapFactory, dataset.ingestionSchema))
                                      .toArray
    val shardMapper = new ShardMapper(numShards)
    val spread = if (numShards >= 2) { (Math.log10(numShards / 2) / Math.log10(2.0)).toInt } else { 0 }

    var streamIsDone: Boolean = false
    // Flush at 1 second intervals until original item stream is complete.  Also send one last flush at the end.
    val itemStream = items.doOnComplete(() => streamIsDone = true)
    val flushStream = Observable.intervalAtFixedRate(flushInterval).map(n => FlushCommand)
                                .takeWhile(n => !streamIsDone)
    (Observable.merge(itemStream, flushStream) ++ Observable.now(FlushCommand))
      .flatMap {
        case s: Seq[DataSample] @unchecked =>
          logger.debug(s"Adding batch of ${s.length} samples")
          s.foreach { case DataSample(tags, timestamp, value) =>
            // Compute keys/values for shard calculation vs original
            // Specifically we need to drop _bucket _sum _count etc from __name__ to put histograms in same shard
            val scalaKVs = tags.toSeq
            val originalKVs = new java.util.ArrayList(scalaKVs.asJava)
            val forShardKVs = scalaKVs.map { case (k, v) =>
                                val trimmedVal = RecordBuilder.trimShardColumn(dataset, k, v)
                                (k, trimmedVal)
                              }
            val kvsForShardCalc = new java.util.ArrayList(forShardKVs.asJava)

            // Get hashes and sort tags of the keys/values for shard calculation
            val hashes = RecordBuilder.sortAndComputeHashes(kvsForShardCalc)

            // Compute partition, shard key hashes and compute shard number
            // TODO: what to do if not all shard keys included?  Just return a 0 hash?  Or maybe random?
            val shardKeyHash = 0 // TODO: this code is being replaced w/ the PrometheusInputRecord
            val partKeyHash = RecordBuilder.combineHashExcluding(kvsForShardCalc, hashes, shardKeys)
            val shard = shardMapper.ingestionShard(shardKeyHash, partKeyHash, spread)

            // Add stuff to RecordContainer for correct partition/shard
            originalKVs.sort(RecordBuilder.stringPairComparator)
            val builder = builders(shard)
            builder.startNewRecord()
            builder.addLong(timestamp)
            builder.addDouble(value)
            builder.addSortedPairsAsMap(originalKVs, hashes)
            builder.endRecord()
          }
          Observable.empty

        case FlushCommand =>  // now produce records, turn into observable
          Observable.fromIterable(flushBuilders(builders))
      }
  }
  // scalastyle:on method.length

  private def flushBuilders(builders: Array[RecordBuilder]): Seq[(Int, Seq[Array[Byte]])] = {
    builders.zipWithIndex.map { case (builder, shard) =>
      logger.debug(s"Flushing shard $shard with ${builder.allContainers.length} containers")

      // get optimal byte arrays out of containers and reset builder so it can keep going
      if (builder.allContainers.nonEmpty) {
        (shard, builder.optimalContainerBytes(true))
      } else {
        (0, Nil)
      }
    }
  }
}


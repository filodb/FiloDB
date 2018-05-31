package filodb.timeseries

import java.lang.{Long => JLong}

import scala.concurrent.duration._

import com.typesafe.scalalogging.StrictLogging
import monix.eval.Task
import monix.execution.schedulers.SchedulerService
import monix.kafka._
import monix.reactive.Observable
import org.apache.kafka.clients.producer.ProducerRecord

import filodb.coordinator.ShardMapper
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Dataset
import filodb.memory.MemFactory

/**
 * This class batches time series records into RecordContainers for each shard and flushes them to Kafka,
 * using proper logic to calculate hashes correctly for shard and partition hashes.
 * @param {[Dataset]} dataset: Dataset the Dataset schema to use for encoding
 * @param {[Int]} numShards: Int the total number of shards or Kafka partitions
 * @param shardKeys The hash keys to use for shard number calculation
 * @param producerCfg the KafkaProducerConfig to use for writing to Kafka
 */
class KafkaContainerSink(dataset: Dataset,
                         numShards: Int,
                         shardKeys: Set[String],
                         producerCfg: KafkaProducerConfig) extends StrictLogging {
  import collection.JavaConverters._

  val builders = (0 until numShards).map(s => new RecordBuilder(MemFactory.onHeapFactory, dataset.ingestionSchema))
                                    .toArray
  val shardMapper = new ShardMapper(numShards)
  val spread = (Math.log10(numShards / 2) / Math.log10(2.0)).toInt

  /**
   * Batch and write, returning a Task which can be hooked into when the stream completes.
   * Currently flushes happen at a regular interval, with an extra flush happening when the item stream ends.
   * @param items an Observable of batches of DataSamples
   */
  // TODO: abstract out some of this stuff so we can accept records of different schemas
  def batchAndWrite(items: Observable[Seq[DataSample]],
                    topicName: String)
                   (implicit io: SchedulerService): Task[Unit] = {
    var streamIsDone: Boolean = false
    val producer = KafkaProducerSink[JLong, Array[Byte]](producerCfg, io)
    // Flush at 1 second intervals until original item stream is complete.  Also send one last flush at the end.
    val itemStream = items.doOnComplete(() => streamIsDone = true)
    val flushStream = Observable.intervalAtFixedRate(1.second).map(n => FlushCommand)
                                .takeWhile(n => !streamIsDone)
    (Observable.merge(itemStream, flushStream) ++ Observable.now(FlushCommand))
      .flatMap {
        case s: Seq[DataSample] @unchecked =>
          logger.debug(s"Adding batch of ${s.length} samples")
          s.foreach { case DataSample(tags, timestamp, value) =>
            // Get hashes and sort tags
            val javaTags = new java.util.ArrayList(tags.toSeq.asJava)
            val hashes = RecordBuilder.sortAndComputeHashes(javaTags)

            // Compute partition, shard key hashes and compute shard number
            // TODO: what to do if not all shard keys included?  Just return a 0 hash?  Or maybe random?
            val shardKeyHash = RecordBuilder.combineHashIncluding(javaTags, hashes, shardKeys).get
            val partKeyHash = RecordBuilder.combineHashExcluding(javaTags, hashes, shardKeys)
            val shard = shardMapper.ingestionShard(shardKeyHash, partKeyHash, spread)

            // Add stuff to RecordContainer for correct partition/shard
            val builder = builders(shard)
            builder.startNewRecord()
            builder.addLong(timestamp)
            builder.addDouble(value)
            builder.addSortedPairsAsMap(javaTags, hashes)
            builder.endRecord()
          }
          Observable.empty

        case FlushCommand =>  // now produce records, turn into observable
          Observable.fromIterable(flushBuilders(topicName))
      }.bufferIntrospective(5)
      .consumeWith(producer)
  }

  private def flushBuilders(topicName: String): Seq[ProducerRecord[JLong, Array[Byte]]] = {
    builders.zipWithIndex.flatMap { case (builder, shard) =>
      logger.debug(s"Flushing shard $shard with ${builder.allContainers.length} containers")

      // get optimal byte arrays out of containers and reset builder so it can keep going
      if (builder.allContainers.nonEmpty) {
        builder.optimalContainerBytes(true).map { bytes =>
          new ProducerRecord[JLong, Array[Byte]](topicName, shard, shard.toLong: JLong, bytes) }
      } else {
        Nil
      }
    }
  }
}
package filodb.cardbuster

import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.cassandra.columnstore.CassandraTokenRangeSplit
import filodb.downsampler.chunk.DownsamplerSettings
import filodb.downsampler.index.DSIndexJobSettings

object CardinalityBusterMain extends App {

  val dsSettings = new DownsamplerSettings()
  val dsIndexJobSettings = new DSIndexJobSettings(dsSettings)

  val iu = new CardinalityBuster(dsSettings, dsIndexJobSettings)
  val sparkConf = new SparkConf(loadDefaults = true)
  iu.run(sparkConf)

}

object BusterContext extends StrictLogging {
  lazy protected[cardbuster] val log: Logger = logger
}

/**
 * Requires following typesafe config properties:
 *
 * filodb.cardbuster.delete-pk-filters = [
 *  {
 *     _ns_ = "bulk_ns"
 *     _ws_ = "tag_value_as_regex"
 *  }
 * ]
 * filodb.cardbuster.delete-startTimeGTE = "ISO_TIME"
 * filodb.cardbuster.delete-startTimeLTE = "ISO_TIME"
 * filodb.cardbuster.delete-endTimeGTE = "ISO_TIME"
 * filodb.cardbuster.delete-endTimeLTE = "ISO_TIME"
 *
 */
class CardinalityBuster(dsSettings: DownsamplerSettings, dsIndexJobSettings: DSIndexJobSettings) extends Serializable {

  def run(conf: SparkConf): SparkSession = {
    val spark = SparkSession.builder()
      .appName("FiloDB_Cardinality_Buster")
      .config(conf)
      .getOrCreate()

    val inDownsampleTables = conf.getBoolean("spark.filodb.cardbuster.inDownsampleTables",
                                                        true)
    val isSimulation = conf.getBoolean("spark.filodb.cardbuster.isSimulation", true)
    BusterContext.log.info(s"This is the Cardinality Buster. Starting job." +
      s" isSimulation=$isSimulation " +
      s" inDownsampleTables=$inDownsampleTables ")

    val numShards = dsIndexJobSettings.numShards
    val busterForShard = new PerShardCardinalityBuster(dsSettings, inDownsampleTables)

    val numPksDeleted = spark.sparkContext
      .makeRDD(0 until numShards)
      .mapPartitions { shards =>
        Kamon.init() // kamon init should be first thing in worker jvm
        val splits = busterForShard.colStore.getScanSplits(busterForShard.dataset)
        for { sh <- shards
              sp <- splits.flatMap(_.asInstanceOf[CassandraTokenRangeSplit].tokens).iterator } yield {
          (sh, sp)
        }
      }.map { case (shard, sp) =>
        Kamon.init() // kamon init should be first thing in worker jvm
        busterForShard.bustIndexRecords(shard, sp, isSimulation)
      }.sum()
    BusterContext.log.info(s"CardinalityBuster completed successfully with " +
      s"isSimulation=$isSimulation numPksDeleted=$numPksDeleted")
    spark
  }

}

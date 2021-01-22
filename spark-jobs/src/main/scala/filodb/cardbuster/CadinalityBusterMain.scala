package filodb.cardbuster

import com.typesafe.scalalogging.{Logger, StrictLogging}
import kamon.Kamon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
 *     _ws_ = "bulk_ws"
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

    val inDownsampleTables = spark.sparkContext.getConf.getBoolean("spark.filodb.cardbuster.inDownsampleTables",
                                                        true)
    BusterContext.log.info(s"This is the Cardinality Buster. Starting job. inDownsampleTables=$inDownsampleTables ")

    val numShards = dsIndexJobSettings.numShards
    val busterForShard = new PerShardCardinalityBuster(dsSettings, inDownsampleTables)

    spark.sparkContext
      .makeRDD(0 until numShards)
      .foreach { shard =>
        Kamon.init() // kamon init should be first thing in worker jvm
        busterForShard.bustIndexRecords(shard)
      }
    BusterContext.log.info(s"CardinalityBuster completed successfully")
    spark
  }

}

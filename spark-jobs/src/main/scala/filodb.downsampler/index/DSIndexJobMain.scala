package filodb.downsampler.index

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import filodb.downsampler.BatchDownsampler._

object DSIndexJobMain extends App {

  val iu = new IndexJobDriver
  val sparkConf = new SparkConf(loadDefaults = true)
  iu.run(sparkConf)
  iu.shutdown()
}

class IndexJobDriver extends StrictLogging {
  import scala.concurrent.duration._

  import DSIndexJobSettings._

  def run(conf: SparkConf): Unit = {
    val spark = SparkSession.builder()
      .appName("FiloDB_DS_IndexUpdater")
      .config(conf)
      .getOrCreate()

    logger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")

    val datasetRef = downsampleRefsByRes(5 minutes)
    val rdd = spark.sparkContext
      .makeRDD(0 until numShards)
      .mapPartitions { shardIter =>
        import DSIndexJob._
        shardIter.map(shard => {
          updateDSPartKeyIndex(shard)
        })
      }
    rdd.foreach(_ => {}) //run job and ignore output
    spark.sparkContext.stop()

    logger.info(s"IndexUpdater Driver completed successfully")
  }

  def shutdown(): Unit = {
    import DSIndexJob._
    rawCassandraColStore.shutdown()
    downsampleCassandraColStore.shutdown()
  }

}

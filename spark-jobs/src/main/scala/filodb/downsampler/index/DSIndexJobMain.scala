package filodb.downsampler.index

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DSIndexJobMain extends App {

  val iu = new IndexJobDriver
  val sparkConf = new SparkConf(loadDefaults = true)
  iu.run(sparkConf)
  iu.shutdown()
}

class IndexJobDriver extends StrictLogging {
  import DSIndexJobSettings._

  def run(conf: SparkConf): Unit = {
    import DSIndexJob._
    val spark = SparkSession.builder()
      .appName("FiloDB_DS_IndexUpdater")
      .config(conf)
      .getOrCreate()

    logger.info(s"Spark Job Properties: ${spark.sparkContext.getConf.toDebugString}")

    val rdd = spark.sparkContext
      .makeRDD(0 until numShards)
      .foreach(updateDSPartKeyIndex)

    spark.sparkContext.stop()

    logger.info(s"IndexUpdater Driver completed successfully")
  }

  def shutdown(): Unit = {
    import DSIndexJob._
    rawCassandraColStore.shutdown()
    downsampleCassandraColStore.shutdown()
  }

}

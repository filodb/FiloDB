package filodb.jmh

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import filodb.spark.{FiloDriver, FiloExecutor}

/**
 * Creates Cassandra test data for the SparkReadBenchmark.  Note that only 1 partition
 * is created for the test data, so that forces reads to be single threaded.
 * Please run this before running the SparkReadBenchmark.
 */
object CreateCassTestData extends App {
  val NumRows = 5000000

  val conf = (new SparkConf).setMaster("local[4]")
                            .setAppName("test")
                            .set("filodb.cassandra.keyspace", "filodb")
                            .set("filodb.memtable.min-free-mb", "10")
                            .set("spark.ui.enabled", "false")
                            .set("spark.driver.memory", "3g")
                            .set("spark.executor.memory", "5g")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  case class DummyRow(data: Int, rownum: Int)

  //scalastyle:off
  val randomIntsRdd = sc.parallelize((1 to NumRows).map { n => DummyRow(util.Random.nextInt, n)})
  import sql.implicits._
  val randomDF = randomIntsRdd.toDF()
  println(s"randomDF: $randomDF")

  println("Writing random DF to FiloDB...")
  randomDF.write.format("filodb.spark").
               option("dataset", "randomInts").
               option("row_keys", "rownum").
               option("segment_key", ":round rownum 30000").
               mode(SaveMode.Overwrite).
               save()

  println("Now waiting a couple secs for writes to finish...")
  Thread sleep 5000

  println("Done!")

  sc.stop()
  FiloDriver.shutdown()
  FiloExecutor.shutdown()
  sys.exit(0)
}
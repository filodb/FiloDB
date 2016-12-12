package filodb.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Use local-cluster mode to test the finer points of ingestion on a cluster, especially
 * the ability to flush data and gather stats on all cluster nodes
 *
 * NOTE: Need to set SPARK_HOME and SPARK_SCALA_VERSION in order for this test to even run.
 * Therefore it should be disabled by default.
 */
class ClusterIngestTest extends SparkTestBase {

  val currClassPath = sys.props("java.class.path")

  // Setup SQLContext and a sample DataFrame
  val sparkSession = SparkSession.builder().master("local-cluster[2,1,1024]").appName("test").config("spark.driver.extraClassPath", currClassPath)
  .config("spark.executor.extraClassPath", currClassPath)
  .config("spark.filodb.cassandra.keyspace", "unittest")
  .config("spark.filodb.memtable.min-free-mb", "10")
  .config("spark.ui.enabled", "false").getOrCreate()

  val sc = sparkSession.sparkContext
  FiloDriver.init(sc)

  import filodb.core.GdeltTestData._
  import org.apache.spark.sql.functions._

  val segCol = ":string 0"
  val testProjections = Seq(dataset1.projections.head)

  it("should be able to write in cluster with multi-column partition keys") {
    import sparkSession.implicits._

    val gdeltDF = sc.parallelize(records.toSeq).toDF()
    // Note: we need to sort to ensure all keys for a given partition key are on same node
    gdeltDF.sort("actor2Code").write.format("filodb.spark").
                 option("dataset", dataset1.name).
                 option("row_keys", "eventId").
                 option("segment_key", segCol).
                 option("partition_keys", ":getOrElse actor2Code --,:getOrElse year -1").
                 mode(SaveMode.Overwrite).
                 save()

    // TODO: test getting ingestion stats from all nodes

    val df = sparkSession.read.format("filodb.spark").option("dataset", dataset1.name).load()
    df.agg(sum("numArticles")).collect().head(0) should equal (492)
  }
}

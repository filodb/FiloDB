package filodb.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}
import org.apache.spark.filodb.FiloDriver

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
  val conf = (new SparkConf).setMaster("local-cluster[2,1,1024]")
                            .setAppName("test")
                            .set("spark.driver.extraClassPath", currClassPath)
                            .set("spark.executor.extraClassPath", currClassPath)
                            .set("spark.filodb.cassandra.keyspace", "unittest")
                            .set("spark.filodb.cassandra.admin-keyspace", "unittest")
                            .set("spark.filodb.memtable.min-free-mb", "10")
                            .set("spark.ui.enabled", "false")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  FiloDriver.init(sc)

  import filodb.core.GdeltTestData._
  import org.apache.spark.sql.functions._

  val segCol = ":string 0"
  val testProjections = Seq(dataset1.projections.head)

  it("should be able to write in cluster with multi-column partition keys") {
    import sql.implicits._

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

    val df = sql.read.format("filodb.spark").option("dataset", dataset1.name).load()
    df.agg(sum("numArticles")).collect().head(0) should equal (492)
  }
}

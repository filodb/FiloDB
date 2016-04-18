package filodb.spark

import com.typesafe.config.ConfigFactory
import java.nio.file.Paths
import org.apache.spark.{SparkContext, SparkException, SparkConf}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.scalatest.time.{Millis, Seconds, Span}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}
import filodb.coordinator.NodeCoordinatorActor.Reset

import org.scalatest.{FunSpec, BeforeAndAfter, BeforeAndAfterAll, Matchers}
import org.scalatest.concurrent.ScalaFutures

/**
 * Use local-cluster mode to test the finer points of ingestion on a cluster, especially
 * the ability to flush data and gather stats on all cluster nodes
 *
 * NOTE: Need to set SPARK_HOME and SPARK_SCALA_VERSION in order for this test to even run.
 * Therefore it should be disabled by default.
 */
class ClusterIngestTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll
with Matchers with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(15, Seconds), interval = Span(250, Millis))

  val currClassPath = sys.props("java.class.path")

  // Setup SQLContext and a sample DataFrame
  val conf = (new SparkConf).setMaster("local-cluster[2,1,1024]")
                            .setAppName("test")
                            .set("spark.driver.extraClassPath", currClassPath)
                            .set("spark.executor.extraClassPath", currClassPath)
                            .set("spark.filodb.cassandra.keyspace", "unittest")
                            .set("spark.filodb.memtable.min-free-mb", "10")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  import filodb.core.GdeltTestData._
  import org.apache.spark.sql.functions._

  val segCol = ":string 0"

  // This is the same code that the Spark stuff uses.  Make sure we use exact same environment as real code
  // so we don't have two copies of metaStore that could be configured differently.
  FiloSetup.init(sc)

  val metaStore = FiloSetup.metaStore
  val columnStore = FiloSetup.columnStore

  override def beforeAll() {
    metaStore.initialize("unittest").futureValue(defaultPatience)
    columnStore.initializeProjection(dataset1.projections.head).futureValue(defaultPatience)
  }

  override def afterAll() {
    super.afterAll()
    sc.stop()
  }

  before {
    metaStore.clearAllData("unittest").futureValue(defaultPatience)
    columnStore.clearSegmentCache()
    try {
      columnStore.clearProjectionData(dataset1.projections.head).futureValue(defaultPatience)
    } catch {
      case e: Exception =>
    }
    FiloSetup.coordinatorActor ! Reset
  }

  implicit val ec = FiloSetup.ec

  ignore("should be able to write in cluster with multi-column partition keys") {
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

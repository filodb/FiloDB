package filodb.spark

import org.apache.spark.sql.{SparkSession, SaveMode}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, DataColumn, Dataset}


/**
 * Test the InMemoryColumnStore
 */
class InMemoryStoreTest extends SparkTestBase {

  val currClassPath = sys.props("java.class.path")

  // Setup SQLContext and a sample DataFrame
  val sess = SparkSession.builder.master("local[4]")
                                 .appName("test")
                                 .config("spark.filodb.store", "in-memory")
                                 .config("spark.filodb.memtable.min-free-mb", "10")
                                 .config("spark.ui.enabled", "false")
                                 .getOrCreate
  val sc = sess.sparkContext
  FiloDriver.init(sc)

  import filodb.core.GdeltTestData._
  import org.apache.spark.sql.functions._

  val segCol = ":string 0"
  val testProjections = Seq(dataset1.projections.head)

  it("should be able to write to InMemoryColumnStore with multi-column partition keys") {
    import sess.implicits._

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

    val df = sess.read.format("filodb.spark").option("dataset", dataset1.name).load()
    df.agg(sum("numArticles")).collect().head(0) should equal (492)
  }
}

package filodb.spark

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 * Test the InMemoryColumnStore
 */
class InMemoryStoreTest extends SparkTestBase {

  val currClassPath = sys.props("java.class.path")

  // Setup SQLContext and a sample DataFrame
  val sess = SparkSession.builder.master("local[4]")
                                 .appName("test")
                                 .config("spark.ui.enabled", "false")
                                 .getOrCreate
  val sc = sess.sparkContext
  FiloDriver.init(sc)

  import org.apache.spark.sql.functions._

  import filodb.core.GdeltTestData._

  val testDatasets = Seq(dataset1.ref)

  it("should be able to write to InMemoryColumnStore with multi-column partition keys") {
    import sess.implicits._

    val gdeltDF = sc.parallelize(records.toSeq).toDF()
    // Note: we need to sort to ensure all keys for a given partition key are on same node
    gdeltDF.sort("actor2Code").write.format("filodb.spark").
                 option("dataset", dataset1.name).
                 option("row_keys", "eventId").
                 option("partition_columns", "actor2Code:string,year:int").
                 mode(SaveMode.Overwrite).
                 save()

    // TODO: test getting ingestion stats from all nodes

    val df = sess.read.format("filodb.spark").option("dataset", dataset1.name).load()
    df.agg(sum("numArticles")).collect().head(0) should equal (492)
  }
}

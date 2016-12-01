package filodb.spark

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._
import _root_.filodb.core._
import _root_.filodb.core.metadata.{Column, DataColumn, Dataset}
import org.apache.spark.filodb.FiloDriver

import scala.util.Try
import scalax.file.Path

/**
 * Test the InMemoryColumnStore
 */
class InMemoryStoreTest extends SparkTestBase {

  val currClassPath = sys.props("java.class.path")

  // Setup SQLContext and a sample DataFrame
  val conf = (new SparkConf).setMaster("local[4]")
                            .setAppName("test")
                            .set("spark.filodb.store", "in-memory")
                            .set("spark.filodb.memtable.min-free-mb", "10")
                            .set("spark.ui.enabled", "false")
                            .set("write-ahead-log.memtable-wal-dir","/tmp/filodb/wal")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  FiloDriver.init(sc)

  import filodb.core.GdeltTestData._
  import org.apache.spark.sql.functions._

  val segCol = ":string 0"
  val testProjections = Seq(dataset1.projections.head)

  after {
    val walDir = conf.get("write-ahead-log.memtable-wal-dir")
    val path = Path.fromString (walDir)
    Try(path.deleteRecursively(continueOnFailure = false))
  }

  it("should be able to write to InMemoryColumnStore with multi-column partition keys") {
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

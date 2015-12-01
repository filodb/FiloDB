package filodb.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkException, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.time.{Millis, Seconds, Span}
import scala.collection.mutable
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.core.columnstore.SegmentSpec

import org.scalatest.{FunSpec, BeforeAndAfter, BeforeAndAfterAll, Matchers}
import org.scalatest.concurrent.ScalaFutures

case class NameRecord(first: Option[String], last: Option[String],
                      age: Option[Long], league: Option[String])

class StreamingTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll
with Matchers with ScalaFutures {
  import SegmentSpec._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  // Setup SQLContext and a sample DataFrame
  val conf = (new SparkConf).setMaster("local[4]")
                            .setAppName("test")
                            .set("filodb.cassandra.keyspace", "unittest")
                            .set("filodb.memtable.min-free-mb", "10")
  val ssc = new StreamingContext(conf, Milliseconds(700))
  val sql = new SQLContext(ssc.sparkContext)

  // This is the same code that the Spark stuff uses.  Make sure we use exact same environment as real code
  // so we don't have two copies of metaStore that could be configured differently.
  val filoConfig = FiloSetup.configFromSpark(ssc.sparkContext)
  FiloSetup.init(filoConfig)

  val metaStore = FiloSetup.metaStore
  val columnStore = FiloSetup.columnStore

  override def beforeAll() {
    metaStore.initialize().futureValue
    columnStore.initializeProjection(largeDataset.projections.head).futureValue
  }

  override def afterAll() {
    super.afterAll()
    ssc.stop(true, true)
  }

  before {
    metaStore.clearAllData().futureValue
    columnStore.clearSegmentCache()
    try {
      columnStore.clearProjectionData(largeDataset.projections.head).futureValue
    } catch {
      case e: Exception =>
    }
  }

  implicit val ec = FiloSetup.ec

  it("should ingest successive streaming RDDs as DataFrames...") {
    val queue = mutable.Queue[RDD[NameRecord]]()
    val _names = lotLotNames
    _names.map(n => NameRecord(n._1, n._2, n._3, n._4)).
                grouped(200).
                foreach { g => queue += ssc.sparkContext.parallelize(g, 1) }
    val nameChunks = ssc.queueStream(queue)
    import sql.implicits._
    nameChunks.foreachRDD { rdd =>
      rdd.toDF.write.format("filodb.spark").
          option("dataset", largeDataset.name).
          option("sort_column", "age").
          option("partition_column", "league").save()
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(8000)

    import org.apache.spark.sql.functions._

    // Now, read back the names to make sure they were all written
    val df = sql.read.format("filodb.spark").option("dataset", largeDataset.name).load()
    df.select(count("age")).collect.head(0) should equal (lotLotNames.length)
  }
}
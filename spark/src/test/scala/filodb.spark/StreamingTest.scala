package filodb.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.scalatest.time.{Millis, Seconds, Span}
import scala.collection.mutable

import filodb.core._

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures


case class NameRecord(first: Option[String], last: Option[String],
                      age: Option[Long], league: Option[String])

class StreamingTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll
with Matchers with ScalaFutures {
  import NamesTestData._

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  // Setup SQLContext and a sample DataFrame
  val sess = SparkSession.builder.master("local[4]")
                                 .appName("test")
                                 .config("spark.filodb.cassandra.keyspace", "unittest")
                                 .config("spark.filodb.cassandra.admin-keyspace", "unittest")
                                 .config("spark.filodb.memtable.min-free-mb", "10")
                                 .config("spark.ui.enabled", "false")
                                 .getOrCreate

  val ssc = new StreamingContext(sess.sparkContext, Milliseconds(700))

  // This is the same code that the Spark stuff uses.  Make sure we use exact same environment as real code
  // so we don't have two copies of metaStore that could be configured differently.
  val filoConfig = FiloDriver.initAndGetConfig(ssc.sparkContext)

  val metaStore = FiloDriver.metaStore

  override def beforeAll(): Unit = {
    metaStore.initialize().futureValue
    // columnStore.initializeProjection(largeDataset.projections.head).futureValue
  }

  override def afterAll(): Unit = {
    super.afterAll()
    ssc.stop(true, true)
  }

  before {
    metaStore.clearAllData().futureValue
    try {
      FiloDriver.client.truncateDataset(dataset.ref)
    } catch {
      case e: Exception =>
    }
  }

  after {
    FiloExecutor.memStore.reset()
  }

  implicit val ec = FiloDriver.ec

  it("should ingest successive streaming RDDs as DataFrames...") {
    val queue = mutable.Queue[RDD[NameRecord]]()
    val _names = lotLotNames
    _names.map(n => NameRecord(n._1, n._2, n._3, n._5)).
                grouped(200).
                foreach { g => queue += ssc.sparkContext.parallelize(g, 1) }
    val nameChunks = ssc.queueStream(queue)
    import sess.implicits._
    nameChunks.foreachRDD { rdd =>
      rdd.toDF.write.format("filodb.spark").
          option("dataset", dataset.name).
          option("row_keys", "age").
          option("partition_columns", "league:string").
          // Flushing after each small batch would be very inefficient...
          option("flush_after_write", "false").
          mode(SaveMode.Append).save()
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(16000)

    // Flush after end of stream.  This is only needed for this test to get definitive results; in a real
    // streaming app this would not be needed, ever....
    FiloDriver.client.flushByName(dataset.name, Some("unittest"))

    import org.apache.spark.sql.functions._

    // Now, read back the names to make sure they were all written
    val df = sess.read.format("filodb.spark").option("dataset", dataset.name).load()
    df.select(count("age")).collect.head should equal (lotLotNames.length)
  }
}
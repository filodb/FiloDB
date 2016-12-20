package filodb.jmh

import ch.qos.logback.classic.{Level, Logger}
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

import filodb.core._
import filodb.core.metadata.{Column, Dataset, RichProjection}
import filodb.spark.{SparkRowReader, FiloDriver, TypeConverters}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.sum
import org.apache.spark.{SparkContext, SparkException, SparkConf}
import org.velvia.filo.{RowReader, TupleRowReader}

// Spark CassandraColumnStore benchmark
// NOTE: before running this test, MUST do sbt jmh/run on CreateCassTestData to populate
// the randomInts FiloDB table in Cassandra.
@State(Scope.Benchmark)
class SparkCassBenchmark {
  org.slf4j.LoggerFactory.getLogger("filodb").asInstanceOf[Logger].setLevel(Level.ERROR)

  // Now create an RDD[Row] out of it, and a Schema, -> DataFrame
  val conf = (new SparkConf).setMaster("local[4]")
                            .setAppName("test")
                            // .set("spark.sql.tungsten.enabled", "false")
                            .set("spark.filodb.cassandra.keyspace", "filodb")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)
  // Below is to make sure that Filo actor system stuff is run before test code
  // so test code is not hit with unnecessary slowdown
  val filoConfig = FiloDriver.initAndGetConfig(sc)

  @TearDown
  def shutdownFiloActors(): Unit = {
    FiloDriver.shutdown()
    sc.stop()
  }

  val cassDF = sql.read.format("filodb.spark").option("dataset", "randomInts").load()

  // NOTE: before running this test, MUST do sbt jmh/run on CreateCassTestData to populate
  // the randomInts FiloDB table in Cassandra.
  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def sparkCassSum(): Any = {
    cassDF.agg(sum(cassDF("data"))).collect().head
  }
}
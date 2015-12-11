package filodb.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._
import org.scalatest.concurrent.{AsyncAssertions, ScalaFutures}

trait SparkTest extends FunSpec with ScalaFutures
with Matchers with Assertions
with AsyncAssertions
with BeforeAndAfterAll {
  self: BeforeAndAfterAll with Suite =>
  // Setup SQLContext and a sample DataFrame
  val conf = (new SparkConf).setMaster("local[4]")
    .setAppName("test")
    .set("filodb.cassandra.keyspace", "unittest")
    .set("filodb.memtable.min-free-mb", "10")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)


}

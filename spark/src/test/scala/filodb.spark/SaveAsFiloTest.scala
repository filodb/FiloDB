package filodb.spark

import filodb.cassandra.CassandraTest
import filodb.core.metadata.Column
import filodb.core.store.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
/**
 * Test saveAsFiloDataset
 */
class SaveAsFiloTest extends CassandraTest {

  // Setup SQLContext and a sample DataFrame
  val conf = (new SparkConf).setMaster("local[4]")
    .setAppName("test")
    .set("filodb.cassandra.hosts", "localhost")
    .set("filodb.cassandra.port", "9142")
    .set("filodb.cassandra.keyspace", "unittest")
    .set("filodb.memtable.min-free-mb", "10")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)


  override def beforeAll(): Unit = {
    super.beforeAll()
    Filo.init(sc)
    Await.result(Filo.columnStore.initialize, 10 seconds)
    Await.result(Filo.metaStore.initialize, 10 seconds)
    Filo.metaStore.addProjection(dataset.projectionInfoSeq.head)
  }

  override def afterAll() {
    super.afterAll()
    Await.result(Filo.columnStore.clearAll, 10 seconds)
    Await.result(Filo.metaStore.clearAll, 10 seconds)
    sc.stop()
  }

  implicit val ec = Filo.executionContext

  val schema = Seq(
    Column("id", "jsonds", 0, Column.ColumnType.LongColumn),
    Column("monthYear", "jsonds", 0, Column.ColumnType.LongColumn),
    Column("sqlDate", "jsonds", 0, Column.ColumnType.StringColumn),
    Column("year", "jsonds", 0, Column.ColumnType.LongColumn))

  val dataset = Dataset("jsonds", schema, "year", "id", "sqlDate", "monthYear")

  // Sample data.  Note how we must create a partitioning column.
  val jsonRows = Seq(
    """{"id":0,"sqlDate":"2015/03/15T15:00Z","monthYear":32015,"year":2015}""",
    """{"id":1,"sqlDate":"2015/03/15T16:00Z","monthYear":42015,"year":2014}""",
    """{"id":2,"sqlDate":"2015/03/15T17:00Z","monthYear":42015,"year":2015}"""
  )

  val dataDF = sql.read.json(sc.parallelize(jsonRows, 1))

  import filodb.spark._
  import org.apache.spark.sql.functions._

  it("should write table to a Filo table and read from it") {
    sql.saveAsFiloDataset(dataDF, "jsonds")

    // Now read stuff back and ensure it got written
    val df = sql.sql("select * from jsonds")
  }


}

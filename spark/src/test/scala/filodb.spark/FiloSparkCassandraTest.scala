package filodb.spark

import filodb.cassandra.CassandraTest
import filodb.core.metadata.Column
import filodb.core.store.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Test saveAsFiloDataset
 */
class FiloSparkCassandraTest extends CassandraTest {

  // Setup SQLContext and a sample DataFrame
  val conf = (new SparkConf(false)).setMaster("local[4]")
    .setAppName("test")
    .set("spark.filodb.cassandra.hosts", "localhost")
    .set("spark.filodb.cassandra.port", "9142")
    .set("spark.filodb.cassandra.keyspace", "unittest")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  override def beforeAll(): Unit = {
    super.beforeAll()
    Filo.init(configFromSpark(sc))
    Filo.parse(Filo.columnStore.initialize)(x => x)
    Filo.parse(Filo.metaStore.initialize)(x => x)
    Filo.metaStore.addProjection(dataset.projectionInfoSeq.head)
    sql.saveAsFiloDataset(dataDF, "jsonds")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Filo.parse(Filo.columnStore.clearAll)(x=>x)
    Filo.parse(Filo.metaStore.clearAll)(x=>x)
    sc.stop()
  }

  implicit val ec = Filo.executionContext

  val schema = Seq(
    Column("id", "jsonds", 0, Column.ColumnType.LongColumn),
    Column("sqlDate", "jsonds", 0, Column.ColumnType.StringColumn),
    Column("monthYear", "jsonds", 0, Column.ColumnType.LongColumn),
    Column("year", "jsonds", 0, Column.ColumnType.LongColumn))

  // Table name,Schema, Part Key, Primary Key, Sort Order,Segment
  val dataset = Dataset("jsonds", schema, "year", "id", "monthYear")

  val jsonRows = Seq(
    """{"id":0,"sqlDate":"2015/03/15T15:00Z","monthYear":32015,"year":2015}""",
    """{"id":1,"sqlDate":"2015/03/15T16:00Z","monthYear":42015,"year":2014}""",
    """{"id":2,"sqlDate":"2015/03/15T17:00Z","monthYear":42015,"year":2015}"""
  )

  val dataDF = sql.read.json(sc.parallelize(jsonRows, 1))

  import filodb.spark._

  it("should write table to a Filo table and read from it") {
    val jsonDS = sql.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds")
    val df = sql.sql("select id,sqlDate from jsonds")
    df.show()
    df.count() should be(3)
    val df2 = sql.sql("select * from jsonds")
    df2.show()
    df2.count() should be(3)

  }
  it("should read when partition key is specified") {
    val jsonDS = sql.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds")
    val df = sql.sql("select sqlDate from jsonds where year=2015")
    df.show()
    df.count() should be(2)
  }

  it("should read when segment key is specified") {
    val jsonDS = sql.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds1")
    val all = sql.sql("select * from jsonds1")
    all.show()
    all.count should be(3)

    val df = sql.sql("select sqlDate from jsonds1 where monthYear=42015")
    df.show()
    df.count() should be(2)
    val df1 = sql.sql("select sqlDate from jsonds1 where monthYear >0 AND monthYear < 32016")
    df1.show()
    df1.count() should be(1)
    val df2 = sql.sql("select sqlDate from jsonds1 where monthYear >32014 AND monthYear < 42016")
    df2.show()
    df2.count() should be(3)
    val df3 = sql.sql("select sqlDate from jsonds1 where monthYear >=32015 AND monthYear < 42015")
    df3.show()
    df3.count() should be(1)
    val df4 = sql.sql("select monthYear,sqlDate from jsonds1 where monthYear >=32015 AND monthYear <= 42015")
    df4.show()
    df4.count() should be(3)
    val df5 = sql.sql("select id,sqlDate from jsonds1 where monthYear >32015 AND monthYear <= 42015")
    df5.show()
    df5.count() should be(2)

  }
  it("should read when partition key and segment keys are specified") {
    val jsonDS = sql.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds2")
    val df = sql.sql("select sqlDate,monthYear from jsonds2 where year=2015 AND monthYear=42015")
    df.show()
    df.count() should be(1)

    val df2 = sql.sql("select sqlDate,id from jsonds2 where year=2015 AND monthYear > 32015")
    df2.show()
    df2.count() should be(1)
  }
}

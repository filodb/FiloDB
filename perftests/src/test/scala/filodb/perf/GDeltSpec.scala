package filodb.perf

import filodb.cassandra.CassandraTest
import filodb.core.metadata.Column
import filodb.core.store.Dataset
import filodb.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

class GDeltSpec extends CassandraTest {
  val gdeltSchema = Seq(Column("GLOBALEVENTID", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("SQLDATE", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("MonthYear", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("Year", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("FractionDate", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("Actor1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Name", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1CountryCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1KnownGroupCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1EthnicCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Religion1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Religion2Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Type1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Type2Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Type3Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Name", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2CountryCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2KnownGroupCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2EthnicCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Religion1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Religion2Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Type1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Type2Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Type3Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("IsRootEvent", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("EventCode", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("EventBaseCode", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("EventRootCode", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("QuadClass", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("GoldsteinScale", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("NumMentions", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("NumSources", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("NumArticles", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("AvgTone", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("Actor1Geo_Type", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("Actor1Geo_FullName", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Geo_CountryCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Geo_ADM1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor1Geo_Lat", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("Actor1Geo_Long", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("Actor1Geo_FeatureID", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("Actor2Geo_Type", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("Actor2Geo_FullName", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Geo_CountryCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Geo_ADM1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("Actor2Geo_Lat", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("Actor2Geo_Long", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("Actor2Geo_FeatureID", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("ActionGeo_Type", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("ActionGeo_FullName", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("ActionGeo_CountryCode", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("ActionGeo_ADM1Code", "gdelt", 0, Column.ColumnType.StringColumn),
    Column("ActionGeo_Lat", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("ActionGeo_Long", "gdelt", 0, Column.ColumnType.DoubleColumn),
    Column("ActionGeo_FeatureID", "gdelt", 0, Column.ColumnType.IntColumn),
    Column("DATEADDED", "gdelt", 0, Column.ColumnType.IntColumn))

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

  val dataset = Dataset("gdelt", gdeltSchema, "Year", "GLOBALEVENTID", "MonthYear")
  implicit val ec = Filo.executionContext


  it("should write gdelt csv to a Filo table and read from it") {
    val gdeltFile = "GDELT-1979-1984-100000.csv"
    val gdelt = sql.read.format("com.databricks.spark.csv")
      .option("path", gdeltFile)
      .option("header", "true")
      .option("inferSchema", "true")
      .load()
    sql.saveAsFiloDataset(gdelt, "gdelt")

  }
}

package filodb.spark.client

import scala.collection.Map
import scala.collection.immutable.{List, Seq}
import scala.language.postfixOps
import filodb.cassandra.CassandraTest
import filodb.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import scala.concurrent.duration._
import scala.concurrent.Await

class FiloInterpreterTest extends CassandraTest {

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
  }

  override def afterAll() {
    super.afterAll()
    Await.result(Filo.columnStore.clearAll, 10 seconds)
    Await.result(Filo.metaStore.clearAll, 10 seconds)
    sc.stop()
  }

  implicit val ec = Filo.executionContext

  val createTable = "CREATE TABLE jsonds (id LongColumn,sqlDate StringColumn," +
    "monthYear LongColumn,year LongColumn) PRIMARY KEY (id) " +
    "PARTITION BY (year) SEGMENT BY (monthYear) SORT BY (sqlDate)"

  val createTableWithoutParition = "CREATE TABLE jsonds (id LongColumn,sqlDate StringColumn," +
    "monthYear LongColumn,year LongColumn) PRIMARY KEY (id) " +
    "SEGMENT BY (monthYear) SORT BY (sqlDate)"

  val createTableWithoutPrimary = "CREATE TABLE jsonds (id LongColumn,sqlDate StringColumn," +
    "monthYear LongColumn,year LongColumn) " +
    "PARTITION BY (year) SEGMENT BY (monthYear) SORT BY (sqlDate)"

  val createTableWithoutSegment = "CREATE TABLE jsonds (id LongColumn,sqlDate StringColumn," +
    "monthYear LongColumn,year LongColumn) PRIMARY KEY (id) " +
    "PARTITION BY (year) SORT BY (sqlDate)"

  val loadTableWithoutFormat = "LOAD './src/test/resources/filoData.json' INTO jsonds"

  val loadTable = "LOAD './src/test/resources/filoData.json' INTO jsonds WITH FORMAT 'json'"

  val loadTableWithOptions = "LOAD './src/test/resources/filoData.json' INTO jsonds WITH FORMAT 'json' WITH OPTIONS " +
    "('samplingRatio' '1.0' )"

  it("should parse create statement properly") {
    val create = SimpleParser.parseCreate(createTable)
    create.partitionCols should contain theSameElementsAs Seq("year")
    create.primaryCols should contain theSameElementsAs Seq("id")
    create.segmentCols should contain theSameElementsAs Seq("monthYear")
    create.sortCols should contain theSameElementsAs Seq("sqlDate")
    create.tableName should be("jsonds")
    val cols = Map("id" -> "LongColumn", "sqlDate" -> "StringColumn", "monthYear" -> "LongColumn", "year" -> "LongColumn")
    create.columns should contain theSameElementsAs cols
  }

  it("should parse load statement properly") {
    val load = SimpleParser.parseLoad(loadTableWithOptions)
    load.format should be("json")
    load.tableName should be("jsonds")
    load.url should be("./src/test/resources/filoData.json")
    val options = Map("samplingRatio" -> "1.0")
    load.options should contain theSameElementsAs options
  }

  it("should write table to a Filo table and read from it") {
    FiloInterpreter.interpret(createTable, sql)
    FiloInterpreter.interpret(loadTable, sql)
    // Now read stuff back and ensure it got written
    val jsonDS = sql.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds")
    val df = FiloInterpreter.interpret("select id,sqlDate from jsonds", sql).asInstanceOf[DataFrame]
    df.show()
    df.count() should be(3)
    val df2 = FiloInterpreter.interpret("select * from jsonds", sql).asInstanceOf[DataFrame]
    df2.show()
    df2.count() should be(3)
  }

  it("should not read when partition key is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(createTableWithoutParition, sql)
    }
  }

  it("should not read when segment key is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(createTableWithoutSegment, sql)
    }
  }

  it("should not read when primary key is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(createTableWithoutPrimary, sql)
    }
  }

  it("should not read when format is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(loadTableWithoutFormat, sql)
    }

  }

  it("should read options in load statement when specified") {
    FiloInterpreter.interpret(createTable, sql)
    FiloInterpreter.interpret(loadTableWithOptions, sql)
    // Now read stuff back and ensure it got written
    val jsonDS = sql.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds")
    val df = FiloInterpreter.interpret("select id,sqlDate from jsonds", sql).asInstanceOf[DataFrame]
    df.show()
    df.count() should be(3)
    val df2 = FiloInterpreter.interpret("select * from jsonds", sql).asInstanceOf[DataFrame]
    df2.show()
    df2.count() should be(3)
  }
}

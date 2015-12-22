package filodb.spark.client

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.language.postfixOps
import filodb.cassandra.CassandraTest
import filodb.spark._
import org.apache.spark.sql.DataFrame
import scala.concurrent.duration._

class FiloInterpreterTest extends CassandraTest {

  val conf = (new SparkConf(false)).setMaster("local[4]")
    .setAppName("test")
    .set("spark.filodb.cassandra.hosts", "localhost")
    .set("spark.filodb.cassandra.port", "9142")
    .set("spark.filodb.cassandra.keyspace", "unittest")
  val sc = new SparkContext(conf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    FiloInterpreter.init(sc)
  }

  override def afterAll() {
    super.afterAll()
    FiloInterpreter.stop()
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
    "('samplingRatio':'1.0' )"

  val showTables = "SHOW TABLES"

  val describeTable = "DESCRIBE TABLE jsonds"

  val describeProjection = "DESCRIBE PROJECTION jsonds 0"

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
    FiloInterpreter.interpret(createTable)
    FiloInterpreter.interpret(loadTable)
    // Now read stuff back and ensure it got written
    val df = FiloInterpreter.interpret("select id,sqlDate from jsonds")
    val data = Await.result(df,10 seconds)
    data.count() should be(3)
    print(FiloInterpreter.getStringFromDF(20,data))
    val df2 = FiloInterpreter.interpret("select * from jsonds")
    val data2 = Await.result(df2,10 seconds)
    data2.count() should be(3)
    print(FiloInterpreter.getStringFromDF(20,data2))
  }

  it("should not read when partition key is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(createTableWithoutParition)
    }
  }

  it("should not read when segment key is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(createTableWithoutSegment)
    }
  }

  it("should not read when primary key is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(createTableWithoutPrimary)
    }
  }

  it("should not read when format is not specified") {
    intercept[IllegalArgumentException] {
      FiloInterpreter.interpret(loadTableWithoutFormat)
    }

  }

  it("should read options in load statement when specified") {
    FiloInterpreter.interpret(createTable)
    FiloInterpreter.interpret(loadTableWithOptions)
    // Now read stuff back and ensure it got written
    val df = FiloInterpreter.interpret("select id,sqlDate from jsonds")
    val data = Await.result(df,10 seconds)
    data.count() should be(3)
    print(FiloInterpreter.getStringFromDF(20,data))
    val df2 = FiloInterpreter.interpret("select * from jsonds")
    val data2 = Await.result(df2,10 seconds)
    data2.count() should be(3)
    print(FiloInterpreter.getStringFromDF(20,data2))
  }

  it("should show tables when specified") {
    FiloInterpreter.interpret(createTable)
    FiloInterpreter.interpret(loadTable)
    val df = FiloInterpreter.interpret(showTables)
    val data = Await.result(df,10 seconds)
    print(FiloInterpreter.getStringFromDF(20,data))
  }

  it("it should describe table when specified") {
    FiloInterpreter.interpret(createTable)
    FiloInterpreter.interpret(loadTable)
    val df = FiloInterpreter.interpret(describeTable)
    val data = Await.result(df,10 seconds)
    print(FiloInterpreter.getStringFromDF(20,data))
  }

  it("it should describe projection when specified") {
    FiloInterpreter.interpret(createTable)
    FiloInterpreter.interpret(loadTable)
    val df = FiloInterpreter.interpret(describeProjection)
    val data = Await.result(df,10 seconds)
    print(FiloInterpreter.getStringFromDF(20,data))
  }

}

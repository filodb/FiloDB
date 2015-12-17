package filodb.spark.client

import scala.collection.Map
import scala.collection.immutable.Seq
import scala.language.postfixOps
import filodb.cassandra.CassandraTest
import filodb.spark._
import org.apache.spark.sql.DataFrame

class FiloInterpreterTest extends CassandraTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    FiloInterpreter.init()
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
    FiloInterpreter.interpret(createTable)
    FiloInterpreter.interpret(loadTable)
    // Now read stuff back and ensure it got written
    val jsonDS = FiloInterpreter.getSqlContext.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds")
    val df = FiloInterpreter.interpret("select id,sqlDate from jsonds").asInstanceOf[DataFrame]
    df.count() should be(3)
    val df2 = FiloInterpreter.interpret("select * from jsonds").asInstanceOf[DataFrame]
    df2.count() should be(3)
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
    val jsonDS = FiloInterpreter.getSqlContext.read.format("filodb.spark").option("dataset", "jsonds").load()
    jsonDS.registerTempTable("jsonds")
    val df = FiloInterpreter.interpret("select id,sqlDate from jsonds").asInstanceOf[DataFrame]
    df.count() should be(3)
    val df2 = FiloInterpreter.interpret("select * from jsonds").asInstanceOf[DataFrame]
    df2.count() should be(3)
  }
}

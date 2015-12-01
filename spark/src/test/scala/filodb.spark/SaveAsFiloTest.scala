package filodb.spark

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkException, SparkConf}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.scalatest.time.{Millis, Seconds, Span}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.coordinator.NodeCoordinatorActor.Reset

import org.scalatest.{FunSpec, BeforeAndAfter, BeforeAndAfterAll, Matchers}
import org.scalatest.concurrent.ScalaFutures

object SaveAsFiloTest {
  val system = ActorSystem("test")
}

/**
 * Test saveAsFiloDataset
 */
class SaveAsFiloTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll
with Matchers with ScalaFutures {

  implicit val defaultPatience =
    PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  // Setup SQLContext and a sample DataFrame
  val conf = (new SparkConf).setMaster("local[4]")
                            .setAppName("test")
                            .set("filodb.cassandra.keyspace", "unittest")
                            .set("filodb.memtable.min-free-mb", "10")
  val sc = new SparkContext(conf)
  val sql = new SQLContext(sc)

  val partitionCol = "_partition"
  val ds1 = Dataset("gdelt1", "id")
  val ds2 = Dataset("gdelt2", "id")
  val ds3 = Dataset("gdelt3", "id", partitionCol)
  val test1 = Dataset("test1", "id")

  // This is the same code that the Spark stuff uses.  Make sure we use exact same environment as real code
  // so we don't have two copies of metaStore that could be configured differently.
  val filoConfig = FiloSetup.configFromSpark(sc)
  FiloSetup.init(filoConfig)

  val metaStore = FiloSetup.metaStore
  val columnStore = FiloSetup.columnStore

  override def beforeAll() {
    metaStore.initialize().futureValue
    columnStore.initializeProjection(ds1.projections.head).futureValue
    columnStore.initializeProjection(ds2.projections.head).futureValue
    columnStore.initializeProjection(ds3.projections.head).futureValue
  }

  override def afterAll() {
    super.afterAll()
    sc.stop()
  }

  before {
    metaStore.clearAllData().futureValue
    columnStore.clearSegmentCache()
    try {
      columnStore.clearProjectionData(ds1.projections.head).futureValue
      columnStore.clearProjectionData(ds2.projections.head).futureValue
      columnStore.clearProjectionData(ds3.projections.head).futureValue
      columnStore.clearProjectionData(test1.projections.head).futureValue
    } catch {
      case e: Exception =>
    }
    FiloSetup.coordinatorActor ! Reset
  }

  implicit val ec = FiloSetup.ec

  // Sample data.  Note how we must create a partitioning column.
  val jsonRows = Seq(
    """{"id":0,"sqlDate":"2015/03/15T15:00Z","monthYear":32015,"year":2015}""",
    """{"id":1,"sqlDate":"2015/03/15T16:00Z","monthYear":42015}""",
    """{"id":2,"sqlDate":"2015/03/15T17:00Z",                  "year":2015}"""
  )
  val dataDF = sql.read.json(sc.parallelize(jsonRows, 1))

  import filodb.spark._
  import org.apache.spark.sql.functions._

  it("should create missing columns and partitions and write table") {
    sql.saveAsFiloDataset(dataDF, "gdelt1", "id",
                          writeTimeout = 2.minutes)

    // Now read stuff back and ensure it got written
    val df = sql.filoDataset("gdelt1")
    df.select(count("id")).collect().head(0) should equal (3)
    df.agg(sum("year")).collect().head(0) should equal (4030)
  }

  it("should throw ColumnTypeMismatch if existing columns are not same type") {
    metaStore.newDataset(ds2).futureValue should equal (Success)
    val idStrCol = Column("id", "gdelt2", 0, Column.ColumnType.StringColumn)
    metaStore.newColumn(idStrCol).futureValue should equal (Success)

    intercept[ColumnTypeMismatch] {
      sql.saveAsFiloDataset(dataDF, "gdelt2", "id")
    }
  }

  it("should write table if there are existing matching columns") {
    metaStore.newDataset(ds3).futureValue should equal (Success)
    val idStrCol = Column("id", "gdelt3", 0, Column.ColumnType.LongColumn)
    metaStore.newColumn(idStrCol).futureValue should equal (Success)

    sql.saveAsFiloDataset(dataDF, "gdelt3", "id",
                          writeTimeout = 2.minutes)

    // Now read stuff back and ensure it got written
    val df = sql.filoDataset("gdelt3")
    df.select(count("id")).collect().head(0) should equal (3)
  }

  it("should write and read using DF write() and read() APIs") {
    dataDF.write.format("filodb.spark").
                 option("dataset", "test1").
                 option("sort_column", "id").
                 mode(SaveMode.Overwrite).
                 save()
    val df = sql.read.format("filodb.spark").option("dataset", "test1").load()
    df.agg(sum("year")).collect().head(0) should equal (4030)
  }

  val jsonRows2 = Seq(
    """{"id":3,"sqlDate":"2015/03/15T18:00Z","monthYear":32015,"year":2016}""",
    """{"id":4,"sqlDate":"2015/03/15T19:00Z","monthYear":42015}""",
    """{"id":5,"sqlDate":"2015/03/15T19:30Z",                  "year":2016}"""
  )
  val dataDF2 = sql.read.json(sc.parallelize(jsonRows2, 1))

  it("should overwrite existing data if mode=Overwrite") {
    dataDF.write.format("filodb.spark").
                 option("dataset", "gdelt1").
                 option("sort_column", "id").
                 save()

    // Data is different, should not append, should overwrite
    dataDF2.write.format("filodb.spark").
                 option("dataset", "gdelt1").
                 option("sort_column", "id").
                 mode(SaveMode.Overwrite).
                 save()

    val df = sql.read.format("filodb.spark").option("dataset", "gdelt1").load()
    df.agg(sum("year")).collect().head(0) should equal (4032)
  }

  it("should append data in Append mode") {
    dataDF.write.format("filodb.spark").
                 option("dataset", "gdelt2").
                 option("sort_column", "id").
                 mode(SaveMode.Append).
                 save()

    dataDF2.write.format("filodb.spark").
                 option("dataset", "gdelt2").
                 option("sort_column", "id").
                 mode(SaveMode.Append).
                 save()

    val df = sql.read.format("filodb.spark").option("dataset", "gdelt2").load()
    df.agg(sum("year")).collect().head(0) should equal (8062)
  }

  // Also test that we can read back from just one partition
  it("should be able to write with a user-specified partitioning column") {
    dataDF.write.format("filodb.spark").
                 option("dataset", "test1").
                 option("sort_column", "id").
                 option("partition_column", "year").
                 option("default_partition_key", "<none>").
                 mode(SaveMode.Overwrite).
                 save()
    val df = sql.read.format("filodb.spark").option("dataset", "test1").load()
    df.agg(sum("id")).collect().head(0) should equal (3)
    df.registerTempTable("test1")
    sql.sql("SELECT sum(id) FROM test1 WHERE year = 2015").collect.head(0) should equal (2)
    // The below _should_ work but somehow Spark throws an error can't figure out year should be included
    // df.agg(sum("id")).where(df("year") === 2015).collect().head(0) should equal (2)
  }
}
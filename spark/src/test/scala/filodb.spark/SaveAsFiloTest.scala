package filodb.spark

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.{SaveMode, SQLContext}
import scala.concurrent.duration._

import filodb.core._
import filodb.core.metadata.{Column, Dataset}
import filodb.cassandra.AllTablesTest
import filodb.cassandra.metastore.CassandraMetaStore

import org.scalatest.{FunSpec, BeforeAndAfter, BeforeAndAfterAll}

object SaveAsFiloTest {
  val system = ActorSystem("test")
}

/**
 * Test saveAsFiloDataset
 */
class SaveAsFiloTest extends FunSpec with BeforeAndAfter with BeforeAndAfterAll with AllTablesTest {
  // Setup SQLContext and a sample DataFrame
  val sc = new SparkContext("local[4]", "test")
  val sql = new SQLContext(sc)

  val partitionCol = "_partition"
  val ds1 = Dataset("gdelt1", "id")
  val ds2 = Dataset("gdelt2", "id")
  val ds3 = Dataset("gdelt3", "id", partitionCol)

  override def beforeAll() {
    metaStore.initialize().futureValue
    try {
      columnStore.clearProjectionData(ds1.projections.head).futureValue
      columnStore.clearProjectionData(ds2.projections.head).futureValue
      columnStore.clearProjectionData(ds3.projections.head).futureValue
    } catch {
      case e: Exception =>
    }
    if (FiloSetup.config != null) FiloSetup.scheduler.reset()
  }

  override def afterAll() {
    super.afterAll()
    sc.stop()
  }

  before {
    metaStore.clearAllData().futureValue
  }

  after {
    FiloSetup.clearState()
  }

  // Sample data.  Note how we must create a partitioning column.
  val jsonRows = Seq(
    """{"id":0,"sqlDate":"2015/03/15T15:00Z","monthYear":32015,"year":2015}""",
    """{"id":1,"sqlDate":"2015/03/15T16:00Z","monthYear":42015}""",
    """{"id":2,"sqlDate":"2015/03/15T17:00Z",                  "year":2015}"""
  )
  val dataDF = sql.read.json(sc.parallelize(jsonRows, 1))

  import filodb.spark._
  import org.apache.spark.sql.functions._

  it("should error out if dataset not created and createDataset=false") {
    val err = intercept[SparkException] {
      sql.saveAsFiloDataset(dataDF, "gdelt1", "id")
    }
    err.getMessage should include ("DatasetNotFound")
  }

  it("should create missing columns and partitions and write table") {
    sql.saveAsFiloDataset(dataDF, "gdelt1", "id",
                          createDataset=true,
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
      sql.saveAsFiloDataset(dataDF, "gdelt2", "id", createDataset=true)
    }
  }

  ignore("should write table if there are existing matching columns") {
    metaStore.newDataset(ds3).futureValue should equal (Success)
    val idStrCol = Column("id", "gdelt3", 0, Column.ColumnType.LongColumn)
    metaStore.newColumn(idStrCol).futureValue should equal (Success)

    sql.saveAsFiloDataset(dataDF, "gdelt3", "id",
                          writeTimeout = 2.minutes)

    // Now read stuff back and ensure it got written
    val df = sql.filoDataset("gdelt3")
    df.select(count("id")).collect().head(0) should equal (3)
  }

  ignore("should write and read using DF write() and read() APIs") {
    dataDF.write.format("filodb.spark").
                 option("dataset", "test1").
                 option("sort_column", "id").
                 mode(SaveMode.Overwrite).
                 save()
    val df = sql.read.format("filodb.spark").option("dataset", "test1").load()
    df.agg(sum("year")).collect().head(0) should equal (4030)
  }

  it("should write in Append mode without first creating dataset") (pending)

  it("should be able to write with a user-specified partitioning column") (pending)
}
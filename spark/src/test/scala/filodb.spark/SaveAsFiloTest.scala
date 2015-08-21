package filodb.spark

import akka.actor.ActorSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.concurrent.duration._

import filodb.core.cassandra.AllTablesTest
import filodb.core.metadata.Column
import filodb.core.messages._

object SaveAsFiloTest {
  val system = ActorSystem("test")
}

/**
 * Test saveAsFiloDataset
 */
class SaveAsFiloTest extends AllTablesTest(SaveAsFiloTest.system) {
  // Setup SQLContext and a sample DataFrame
  val sc = new SparkContext("local[4]", "test")
  val sql = new SQLContext(sc)

  override def beforeAll() {
    createAllTables()
  }

  override def afterAll() {
    super.afterAll()
    sc.stop()
  }

  before {
    truncateAllTables()
  }

  // Sample data
  val jsonRows = Seq(
    """{"id":0,"sqlDate":"2015/03/15T15:00Z","monthYear":32015,"year":2015}""",
    """{"id":1,"sqlDate":"2015/03/15T16:00Z","monthYear":42015}""",
    """{"id":2,"sqlDate":"2015/03/15T17:00Z",                  "year":2015}"""
  )
  val dataDF = sql.read.json(sc.parallelize(jsonRows, 1))

  import filodb.spark._
  import org.apache.spark.sql.functions._

  it("should error out if dataset not created and createDataset=false") {
    intercept[DatasetNotFound] {
      sql.saveAsFiloDataset(dataDF, AllTablesTest.CassConfig, "gdelt1")
    }
  }

  it("should create missing columns and partitions and write table") {
    sql.saveAsFiloDataset(dataDF, AllTablesTest.CassConfig, "gdelt1",
                          createDataset=true,
                          writeTimeout = 2.minutes)

    // Now read stuff back and ensure it got written
    val df = sql.filoDataset(AllTablesTest.CassConfig, "gdelt1")
    df.select(count("id")).collect().head(0) should equal (3)
    df.agg(sum("year")).collect().head(0) should equal (4030)
  }

  it("should throw ColumnTypeMismatch if existing columns are not same type") {
    datastore.newDataset("gdelt2").futureValue should equal (Success)
    val idStrCol = Column("id", "gdelt2", 0, Column.ColumnType.StringColumn)
    datastore.newColumn(idStrCol).futureValue should equal (Success)

    intercept[ColumnTypeMismatch] {
      sql.saveAsFiloDataset(dataDF, AllTablesTest.CassConfig, "gdelt2", createDataset=true)
    }
  }

  it("should write table if there are existing matching columns") {
    datastore.newDataset("gdelt3").futureValue should equal (Success)
    val idStrCol = Column("id", "gdelt3", 0, Column.ColumnType.LongColumn)
    datastore.newColumn(idStrCol).futureValue should equal (Success)

    sql.saveAsFiloDataset(dataDF, AllTablesTest.CassConfig, "gdelt3",
                          writeTimeout = 2.minutes)

    // Now read stuff back and ensure it got written
    val df = sql.filoDataset(AllTablesTest.CassConfig, "gdelt3")
    df.select(count("id")).collect().head(0) should equal (3)
  }
}
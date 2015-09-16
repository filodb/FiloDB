package filodb.spark

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.{Column => SparkColumn}
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

  override def beforeAll() {
    metaStore.initialize().futureValue
  }

  override def afterAll() {
    super.afterAll()
    sc.stop()
  }

  before {
    metaStore.clearAllData().futureValue
  }

  // Sample data.  Note how we must create a partitioning column.
  val jsonRows = Seq(
    """{"id":0,"sqlDate":"2015/03/15T15:00Z","monthYear":32015,"year":2015}""",
    """{"id":1,"sqlDate":"2015/03/15T16:00Z","monthYear":42015}""",
    """{"id":2,"sqlDate":"2015/03/15T17:00Z",                  "year":2015}"""
  )
  val dataDF = sql.read.json(sc.parallelize(jsonRows, 1))
                       .withColumn(":partition", new SparkColumn(Literal("/0")))

  import filodb.spark._
  import org.apache.spark.sql.functions._

  it("should error out if dataset not created and createDataset=false") {
    intercept[DatasetNotFound] {
      sql.saveAsFiloDataset(dataDF, "gdelt1", "id", ":partition")
    }
  }

  it("should create missing columns and partitions and write table") {
    sql.saveAsFiloDataset(dataDF, "gdelt1", "id", ":partition",
                          createDataset=true,
                          writeTimeout = 2.minutes)

    // Now read stuff back and ensure it got written
    val df = sql.filoDataset("gdelt1")
    df.select(count("id")).collect().head(0) should equal (3)
    df.agg(sum("year")).collect().head(0) should equal (4030)
  }

  it("should throw ColumnTypeMismatch if existing columns are not same type") {
    val ds = Dataset("gdelt2", "id")
    metaStore.newDataset(ds).futureValue should equal (Success)
    val idStrCol = Column("id", "gdelt2", 0, Column.ColumnType.StringColumn)
    metaStore.newColumn(idStrCol).futureValue should equal (Success)

    intercept[ColumnTypeMismatch] {
      sql.saveAsFiloDataset(dataDF, "gdelt2", "id", ":partition", createDataset=true)
    }
  }

  it("should write table if there are existing matching columns") {
    val ds = Dataset("gdelt3", "id")
    metaStore.newDataset(ds).futureValue should equal (Success)
    val idStrCol = Column("id", "gdelt3", 0, Column.ColumnType.LongColumn)
    metaStore.newColumn(idStrCol).futureValue should equal (Success)

    sql.saveAsFiloDataset(dataDF, "gdelt3", "id", ":partition",
                          writeTimeout = 2.minutes)

    // Now read stuff back and ensure it got written
    val df = sql.filoDataset("gdelt3")
    df.select(count("id")).collect().head(0) should equal (3)
  }

  it("should write and read using DF write() and read() APIs") {
    dataDF.write.format("filodb.spark").
                 option("dataset", "test1").
                 option("sort_column", "id").
                 option("partition_column", ":partition").
                 save()
    val df = sql.read.format("filodb.spark").option("dataset", "test1").load()
    df.agg(sum("year")).collect().head(0) should equal (4030)
  }
}
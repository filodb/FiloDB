package filodb.labelchurnfinder

import java.io.File
import java.time.Instant

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.downsampler.chunk.DownsamplerSettings

/**
 * Tests of the label churn finder code that does not read from Cassandra.
 */
class PartKeyExportSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {
  private val baseConf = ConfigFactory.parseFile(new File("conf/timeseries-filodb-server.conf")).resolve()
  private val jobConfig = ConfigFactory.parseString("filodb.labelchurnfinder.raw-dataset-name = prometheus")
  private val lcf = new LabelChurnFinder(new DownsamplerSettings(jobConfig.withFallback(baseConf)))

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val sparkConf = new SparkConf(loadDefaults = true)
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.driver.host", "127.0.0.1")
    sparkConf.set("spark.driver.bindAddress", "127.0.0.1")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark = SparkSession.builder().appName("PartKeyExportSpec").config(sparkConf).getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  private def partKey(ws: String, pod: String, endTime: Long): PartKeyRow =
    PartKeyRow(ws, "ns1", "metric1",
      Map("_ws_" -> ws, "_ns_" -> "ns1", "_metric_" -> "metric1", "pod" -> pod), 1000L, endTime)

  it("should explode every time series into one row per label") {
    val session = spark
    import session.implicits._
    val partKeys = Seq(partKey("ws1", "pod-a", Long.MaxValue), partKey("ws1", "pod-b", 5000L)).toDS()

    lcf.explodeLabels(partKeys).collect().toSet shouldEqual Set(
      LabelValRow("ws1", "ns1", "_ws_", "ws1", Long.MaxValue),
      LabelValRow("ws1", "ns1", "_ns_", "ns1", Long.MaxValue),
      LabelValRow("ws1", "ns1", "_metric_", "metric1", Long.MaxValue),
      LabelValRow("ws1", "ns1", "pod", "pod-a", Long.MaxValue),
      LabelValRow("ws1", "ns1", "_ws_", "ws1", 5000L),
      LabelValRow("ws1", "ns1", "_ns_", "ns1", 5000L),
      LabelValRow("ws1", "ns1", "_metric_", "metric1", 5000L),
      LabelValRow("ws1", "ns1", "pod", "pod-b", 5000L))
  }

  it("should map time series to the columns of the Iceberg table") {
    val session = spark
    import session.implicits._
    val exporter = new PartKeyIcebergExporter(ConfigFactory.parseString(
      """
        |deployment-partition-name = tsdb7
        |labelchurnfinder.iceberg-export {
        |  catalog = mosaic_catalog
        |  database = part_keys_test
        |  table = daily_snapshot
        |  base-path = "s3a://bucket/path"
        |  workspaces = []
        |}
        |""".stripMargin))
    val partKeys = Seq(partKey("ws1", "pod-a", Long.MaxValue)).toDS()
    val snapshotTs = Instant.parse("2025-11-05T23:59:59Z").toEpochMilli

    val rows = exporter.exportRows(partKeys, snapshotTs)

    // the columns and types must match the columns of the CREATE TABLE statement
    rows.schema.fields.map(f => f.name -> f.dataType.simpleString).toSeq shouldEqual Seq(
      "ws" -> "string", "ns" -> "string", "metric" -> "string", "labels" -> "map<string,string>",
      "start_time" -> "bigint", "end_time" -> "bigint", "mosaic_partition" -> "string",
      "snapshot_date" -> "date", "snapshot_ts" -> "bigint")

    val row = rows.collect().head
    row.getAs[String]("ws") shouldEqual "ws1"
    row.getAs[String]("ns") shouldEqual "ns1"
    row.getAs[String]("metric") shouldEqual "metric1"
    row.getAs[scala.collection.Map[String, String]]("labels") shouldEqual
      Map("_ws_" -> "ws1", "_ns_" -> "ns1", "_metric_" -> "metric1", "pod" -> "pod-a")
    row.getAs[Long]("start_time") shouldEqual 1000L
    row.getAs[Long]("end_time") shouldEqual Long.MaxValue
    row.getAs[String]("mosaic_partition") shouldEqual "tsdb7"
    row.getAs[java.sql.Date]("snapshot_date") shouldEqual java.sql.Date.valueOf("2025-11-05")
    row.getAs[Long]("snapshot_ts") shouldEqual snapshotTs
  }
}

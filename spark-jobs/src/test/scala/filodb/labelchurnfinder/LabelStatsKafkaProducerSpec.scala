package filodb.labelchurnfinder

import com.typesafe.config.ConfigFactory
import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/**
 * Unit tests for LabelStatsKafkaProducer.
 */
class LabelStatsKafkaProducerSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  import LabelStatisticsMessage._

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val sparkConf = new SparkConf(loadDefaults = false)
      .setMaster("local[2]")
      .setAppName("LabelStatsKafkaProducerSpec")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.enabled", "false")

    spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  def createTestDataFrame(data: Seq[(String, String, String, Long, Long, Long,
    Array[Byte], Array[Byte], Array[Byte])]): DataFrame = {
    val schema = StructType(Seq(
      StructField(LabelChurnFinder.WsCol, StringType, nullable = false),
      StructField(LabelChurnFinder.NsGroupCol, StringType, nullable = false),
      StructField(LabelChurnFinder.LabelCol, StringType, nullable = false),
      StructField(LabelChurnFinder.Ats1hWithLabelCol, LongType, nullable = false),
      StructField(LabelChurnFinder.Ats3dWithLabelCol, LongType, nullable = false),
      StructField(LabelChurnFinder.Ats7dWithLabelCol, LongType, nullable = false),
      StructField(LabelChurnFinder.LabelSketch1hCol, BinaryType, nullable = false),
      StructField(LabelChurnFinder.LabelSketch3dCol, BinaryType, nullable = false),
      StructField(LabelChurnFinder.LabelSketch7dCol, BinaryType, nullable = false)
    ))

    val rows = data.map { case (ws, nsGroup, label, ats1h, ats3d, ats7d, sketch1h, sketch3d, sketch7d) =>
      Row(ws, nsGroup, label, ats1h, ats3d, ats7d, sketch1h, sketch3d, sketch7d)
    }

    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  describe("LabelStatisticsMessage JSON encoding") {

    it("should encode message with all fields correctly") {

      val msg: LabelStatisticsMessage = LabelStatisticsMessage(
        workspaceId = "test-workspace",
        mosaicPartition = "us-east-1-p1",
        nsGroup = "All",
        jobTimestamp = Instant.parse("2024-01-01T12:00:00Z"),
        labels = Seq(
          LabelStatDto(
            labelName = "pod",
            ats1h = 100L,
            ats3d = 250L,
            ats7d = 400L,
            sketchLabelCard1h = "AQEB",
            sketchLabelCard3d = "AQEC",
            sketchLabelCard7d = "AQED"
          ),
          LabelStatDto(
            labelName = "instance",
            ats1h = 50L,
            ats3d = 150L,
            ats7d = 300L,
            sketchLabelCard1h = "XXYY",
            sketchLabelCard3d = "AABB",
            sketchLabelCard7d = "CCDD"
          )
        )
      )

      val json = msg.asJson.noSpaces
      val parsed = parse(json).right.get

      // Verify all message-level fields exist with correct names
      parsed.hcursor.get[String]("workspaceId").right.get shouldBe "test-workspace"
      parsed.hcursor.get[String]("mosaicPartition").right.get shouldBe "us-east-1-p1"
      parsed.hcursor.get[String]("nsGroup").right.get shouldBe "All"
      parsed.hcursor.get[String]("jobTimestamp").right.get shouldBe "2024-01-01T12:00:00Z"

      // Verify labels array encoding
      val labels = parsed.hcursor.downField("labels").as[List[io.circe.Json]].right.get
      labels should have size 2

      // Verify first label - all fields
      val label1 = labels.head
      label1.hcursor.get[String]("labelName").right.get shouldBe "pod"
      label1.hcursor.get[Long]("ats1h").right.get shouldBe 100L
      label1.hcursor.get[Long]("ats3d").right.get shouldBe 250L
      label1.hcursor.get[Long]("ats7d").right.get shouldBe 400L
      label1.hcursor.get[String]("sketchLabelCard1h").right.get shouldBe "AQEB"
      label1.hcursor.get[String]("sketchLabelCard3d").right.get shouldBe "AQEC"
      label1.hcursor.get[String]("sketchLabelCard7d").right.get shouldBe "AQED"

      // Verify second label - all fields
      val label2 = labels(1)
      label2.hcursor.get[String]("labelName").right.get shouldBe "instance"
      label2.hcursor.get[Long]("ats1h").right.get shouldBe 50L
      label2.hcursor.get[Long]("ats3d").right.get shouldBe 150L
      label2.hcursor.get[Long]("ats7d").right.get shouldBe 300L
      label2.hcursor.get[String]("sketchLabelCard1h").right.get shouldBe "XXYY"
      label2.hcursor.get[String]("sketchLabelCard3d").right.get shouldBe "AABB"
      label2.hcursor.get[String]("sketchLabelCard7d").right.get shouldBe "CCDD"
    }
  }

  describe("DataFrame grouping logic") {

    it("should group labels by workspace and namespace") {
      val testData = Seq(
        ("workspace1", "All", "pod", 100L, 200L, 300L, Array[Byte](1, 2), Array[Byte](3, 4), Array[Byte](5, 6)),
        ("workspace1", "All", "instance", 50L, 100L, 150L, Array[Byte](7, 8), Array[Byte](9, 10), Array[Byte](11, 12)),
        ("workspace1", "All", "container", 500L, 1000L, 1500L,
          Array[Byte](13, 14), Array[Byte](15, 16), Array[Byte](17, 18)),

        ("workspace2", "All", "pod", 50L, 100L, 150L, Array[Byte](4), Array[Byte](5), Array[Byte](6)),

        ("workspace3", "All", "instance", 75L, 125L, 175L, Array[Byte](7), Array[Byte](8), Array[Byte](9))
      )

      val df = createTestDataFrame(testData)
      val config = ConfigFactory.parseString(
        """
          |partition = "us-east-1-p1"
          |labelchurnfinder.kafka {
          |  topic = "test-topic"
          |  bootstrap-servers = "localhost:9092"
          |}
          |""".stripMargin)

      val producer = new LabelStatsKafkaProducer(config)

      val grouped = producer.groupLabelsByWorkspace(df)

      val result = grouped.collect()

      // Should have 3 groups (3 workspaces)
      result should have length 3

      // Workspace1 should have 3 labels
      val ws1 = result.find(_.getAs[String]("ws") == "workspace1").get
      val ws1Labels = ws1.getAs[Seq[Row]]("labels")
      ws1Labels should have length 3

      // Workspace2 should have 1 label
      val ws2 = result.find(_.getAs[String]("ws") == "workspace2").get
      val ws2Labels = ws2.getAs[Seq[Row]]("labels")
      ws2Labels should have length 1

      // Workspace3 should have 1 label
      val ws3 = result.find(_.getAs[String]("ws") == "workspace3").get
      val ws3Labels = ws3.getAs[Seq[Row]]("labels")
      ws3Labels should have length 1
    }
  }

  describe("Helper methods") {

    it("should convert Spark Row to LabelStatDto correctly") {
      // Create a Spark Row simulating the structure from groupBy
      val labelRow = Row(
        "pod",           // labelName (index 0)
        100L,           // ats1h (index 1)
        200L,           // ats3d (index 2)
        300L,           // ats7d (index 3)
        Array[Byte](1, 2, 3),  // sketch1h (index 4)
        Array[Byte](4, 5, 6),  // sketch3d (index 5)
        Array[Byte](7, 8, 9)   // sketch7d (index 6)
      )

      val result = LabelStatsKafkaProducer.buildLabelsFromRows(Seq(labelRow))

      result should have length 1
      val dto = result.head
      dto.labelName shouldBe "pod"
      dto.ats1h shouldBe 100L
      dto.ats3d shouldBe 200L
      dto.ats7d shouldBe 300L
      dto.sketchLabelCard1h shouldBe "AQID"
      dto.sketchLabelCard3d shouldBe "BAUG"
      dto.sketchLabelCard7d shouldBe "BwgJ"
    }
  }

  describe("Producer configuration") {

    it("should use correct Kafka configuration values") {
      val config = ConfigFactory.parseString(
        """
          |partition = "us-east-1-p1"
          |labelchurnfinder.kafka {
          |  topic = "test-topic"
          |  bootstrap-servers = "test-server:9092"
          |}
          |""".stripMargin)

      val producer = new LabelStatsKafkaProducer(config)

      producer.topic shouldBe "test-topic"
      producer.bootstrapServers shouldBe "test-server:9092"
      producer.mosaicPartition shouldBe "us-east-1-p1"
    }
  }
}

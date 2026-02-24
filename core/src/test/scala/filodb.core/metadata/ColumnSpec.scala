package filodb.core.metadata

import com.typesafe.config.ConfigFactory
import filodb.core.memstore.aggregation.AggregationType
import filodb.core.query.ColumnInfo
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ColumnSpec extends AnyFunSpec with Matchers {
  import Column.ColumnType
  import Dataset._

  val firstColumn = DataColumn(0, "first", ColumnType.StringColumn)
  val cumulHistColumn = DataColumn(3, "hist", ColumnType.HistogramColumn,
                              ConfigFactory.parseString("detectDrops = true"))
  val ageColumn = DataColumn(2, "age", ColumnType.IntColumn)
  val histColumnOpts = DataColumn(3, "hist", ColumnType.HistogramColumn,
                                  ConfigFactory.parseString("counter = true"))
  val histColumn2 = DataColumn(4, "h2", ColumnType.HistogramColumn,
                               ConfigFactory.parseString("counter = true\nsize=20000"))
  val deltaCountColumn = DataColumn(5, "dc", ColumnType.HistogramColumn,
    ConfigFactory.parseString("{counter = false, delta = true}"))

  describe("Column validations") {
    it("should check that regular column names don't have : in front") {
      val res1 = Column.validateColumnName(":illegal")
      res1.isBad shouldEqual true
      res1.swap.get.head shouldBe a[BadColumnName]
    }

    it("should return correct value for hasCumulativeIncrement") {
      cumulHistColumn.isCumulativeTemporality shouldEqual true
      deltaCountColumn.isCumulativeTemporality shouldEqual false
      firstColumn.isCumulativeTemporality shouldEqual false
    }

    it("should check that column names cannot contain illegal chars") {
      def checkIsIllegal(name: String): Unit = {
        val res1 = Column.validateColumnName(name)
        res1.isBad shouldEqual true
        res1.swap.get.head shouldBe a[BadColumnName]
      }

      checkIsIllegal("ille gal")
      checkIsIllegal("(illegal)")
      checkIsIllegal("ille\u0001gal")
    }
  }

  describe("Column serialization") {
    it("should serialize and deserialize properly") {
      DataColumn.fromString(firstColumn.toString) should equal (firstColumn)
      DataColumn.fromString(ageColumn.toString) should equal (ageColumn)
      DataColumn.fromString(histColumnOpts.toString) should equal (histColumnOpts)
      DataColumn.fromString(histColumn2.toString) should equal (histColumn2)
      DataColumn.fromString(deltaCountColumn.toString) should equal (deltaCountColumn)
    }
    it("check delta param") {
      ColumnInfo(firstColumn).isCumulative should equal (true)
      ColumnInfo(ageColumn).isCumulative should equal (true)
      ColumnInfo(histColumnOpts).isCumulative should equal (true)
      ColumnInfo(histColumn2).isCumulative should equal (true)
      ColumnInfo(deltaCountColumn).isCumulative should equal (false)
    }
  }

  describe("Aggregation config parsing") {
    it("should parse time strings correctly") {
      Column.parseTimeString("30s") shouldEqual 30000L
      Column.parseTimeString("1m") shouldEqual 60000L
      Column.parseTimeString("5m") shouldEqual 300000L
      Column.parseTimeString("1h") shouldEqual 3600000L
      Column.parseTimeString("1d") shouldEqual 86400000L
    }

    it("should parse multi-digit time values") {
      Column.parseTimeString("90s") shouldEqual 90000L
      Column.parseTimeString("15m") shouldEqual 900000L
      Column.parseTimeString("24h") shouldEqual 86400000L
    }

    it("should throw on invalid time strings") {
      intercept[IllegalArgumentException] {
        Column.parseTimeString("invalid")
      }
      intercept[IllegalArgumentException] {
        Column.parseTimeString("30")
      }
      intercept[IllegalArgumentException] {
        Column.parseTimeString("30x")
      }
    }

    it("should parse aggregation config from params") {
      val params = ConfigFactory.parseString("""
        aggregation = "sum"
        interval = "30s"
        ooo-tolerance = "60s"
      """)

      val config = Column.parseAggregationConfig(params, 1)
      config shouldBe defined
      config.get.columnIndex shouldEqual 1
      config.get.aggType shouldEqual AggregationType.Sum
      config.get.intervalMs shouldEqual 30000L
      config.get.oooToleranceMs shouldEqual 60000L
    }

    it("should parse different aggregation types") {
      def parseAggType(aggType: String): Option[AggregationType] = {
        val params = ConfigFactory.parseString(s"""
          aggregation = "$aggType"
          interval = "30s"
          ooo-tolerance = "60s"
        """)
        Column.parseAggregationConfig(params, 0).map(_.aggType)
      }

      parseAggType("sum") shouldEqual Some(AggregationType.Sum)
      parseAggType("avg") shouldEqual Some(AggregationType.Avg)
      parseAggType("min") shouldEqual Some(AggregationType.Min)
      parseAggType("max") shouldEqual Some(AggregationType.Max)
      parseAggType("last") shouldEqual Some(AggregationType.Last)
      parseAggType("first") shouldEqual Some(AggregationType.First)
      parseAggType("count") shouldEqual Some(AggregationType.Count)
    }

    it("should return None when aggregation params missing") {
      val params1 = ConfigFactory.parseString("""
        interval = "30s"
        ooo-tolerance = "60s"
      """)
      Column.parseAggregationConfig(params1, 0) shouldEqual None

      val params2 = ConfigFactory.parseString("""
        aggregation = "sum"
        ooo-tolerance = "60s"
      """)
      Column.parseAggregationConfig(params2, 0) shouldEqual None

      val params3 = ConfigFactory.parseString("""
        aggregation = "sum"
        interval = "30s"
      """)
      Column.parseAggregationConfig(params3, 0) shouldEqual None
    }

    it("should return None for invalid aggregation type") {
      val params = ConfigFactory.parseString("""
        aggregation = "invalid"
        interval = "30s"
        ooo-tolerance = "60s"
      """)
      Column.parseAggregationConfig(params, 0) shouldEqual None
    }

    it("should create column with aggregation config") {
      val columns = Column.makeColumnsFromNameTypeList(Seq(
        "timestamp:ts",
        "value:double:{aggregation=sum,interval=30s,ooo-tolerance=60s}"
      ))

      columns.isGood shouldEqual true
      val cols = columns.get
      cols.length shouldEqual 2

      val valueCol = cols(1).asInstanceOf[DataColumn]
      valueCol.hasAggregation shouldEqual true
      valueCol.aggregationConfig shouldBe defined

      val aggConfig = valueCol.aggregationConfig.get
      aggConfig.columnIndex shouldEqual 1
      aggConfig.aggType shouldEqual AggregationType.Sum
      aggConfig.intervalMs shouldEqual 30000L
      aggConfig.oooToleranceMs shouldEqual 60000L
    }

    it("should create column without aggregation when not specified") {
      val columns = Column.makeColumnsFromNameTypeList(Seq(
        "timestamp:ts",
        "value:double"
      ))

      columns.isGood shouldEqual true
      val cols = columns.get
      cols.length shouldEqual 2

      val valueCol = cols(1).asInstanceOf[DataColumn]
      valueCol.hasAggregation shouldEqual false
      valueCol.aggregationConfig shouldEqual None
    }

    it("should handle mixed aggregated and non-aggregated columns") {
      val columns = Column.makeColumnsFromNameTypeList(Seq(
        "timestamp:ts",
        "sum:double:{aggregation=sum,interval=1m,ooo-tolerance=2m}",
        "min:double:{aggregation=min,interval=1m,ooo-tolerance=2m}",
        "label:string"
      ))

      columns.isGood shouldEqual true
      val cols = columns.get

      cols(0).asInstanceOf[DataColumn].hasAggregation shouldEqual false
      cols(1).asInstanceOf[DataColumn].hasAggregation shouldEqual true
      cols(2).asInstanceOf[DataColumn].hasAggregation shouldEqual true
      cols(3).asInstanceOf[DataColumn].hasAggregation shouldEqual false
    }
  }
}
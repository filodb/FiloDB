package filodb.core.store

import com.typesafe.config.ConfigFactory
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.memstore.aggregation.{AggregationConfig, AggregationType, ColumnAggregator}

/**
 * Tests for the per-dataset out-of-order `aggregation {}` block: it parses from the ingestion
 * source config (mirroring `store {}` -> StoreConfig), an absent block yields an empty/disabled
 * config, and the aggregator/interval/tolerance validation catches misconfigurations.
 */
class IngestionConfigSpec extends AnyFunSpec with Matchers {

  private val sourceFactory = "filodb.core.NoOpStreamFactory"

  private def sourceConfig(extra: String): String =
    s"""
       |dataset = "prometheus"
       |num-shards = 4
       |min-num-nodes = 2
       |sourceconfig {
       |  store {
       |    flush-interval = 1h
       |    shard-mem-size = 512MB
       |  }
       |  $extra
       |}
     """.stripMargin

  describe("AggregationConfig parsing from an aggregation {} block") {

    it("parses aggregators, interval and ooo-tolerance") {
      val conf = ConfigFactory.parseString(
        """
          |aggregation {
          |  aggregators   = ["dSum(1)", "dSum(2)", "hSum(3)"]
          |  interval      = 1m
          |  ooo-tolerance = 2m
          |}
        """.stripMargin)
      val agg = AggregationConfig.fromSourceConfig(conf)
      agg.nonEmpty shouldEqual true
      agg.aggregators.length shouldEqual 3
      agg.aggregators(0) shouldEqual ColumnAggregator(1, AggregationType.Sum)
      agg.aggregators(1) shouldEqual ColumnAggregator(2, AggregationType.Sum)
      agg.aggregators(2) shouldEqual ColumnAggregator(3, AggregationType.HistogramSum)
      agg.intervalMs shouldEqual 60000L
      agg.oooToleranceMs shouldEqual 120000L
    }

    it("yields an empty config when there is no aggregation block") {
      val conf = ConfigFactory.parseString("""store { flush-interval = 1h }""")
      val agg = AggregationConfig.fromSourceConfig(conf)
      agg shouldEqual AggregationConfig.empty
      agg.nonEmpty shouldEqual false
      agg.isEmpty shouldEqual true
    }

    it("throws on an unknown aggregator name") {
      val conf = ConfigFactory.parseString(
        """aggregation { aggregators = ["bogus(1)"], interval = 1m, ooo-tolerance = 2m }""")
      intercept[IllegalArgumentException] {
        AggregationConfig.fromSourceConfig(conf)
      }
    }
  }

  describe("AggregationConfig.validate") {

    it("accepts a valid config against the schema column count") {
      val agg = AggregationConfig(ColumnAggregator.parseAll(Seq("dSum(1)", "hSum(3)")), 60000L, 120000L)
      noException should be thrownBy agg.validate(numDataColumns = 4)
    }

    it("rejects an aggregator that references an out-of-range column id") {
      val agg = AggregationConfig(ColumnAggregator.parseAll(Seq("dSum(5)")), 60000L, 120000L)
      intercept[IllegalArgumentException] { agg.validate(numDataColumns = 2) }
    }

    it("rejects an aggregator on the timestamp column (id 0)") {
      val agg = AggregationConfig(ColumnAggregator.parseAll(Seq("dSum(0)")), 60000L, 120000L)
      intercept[IllegalArgumentException] { agg.validate(numDataColumns = 2) }
    }

    it("rejects a non-positive interval when aggregators are defined") {
      val agg = AggregationConfig(ColumnAggregator.parseAll(Seq("dSum(1)")), 0L, 120000L)
      intercept[IllegalArgumentException] { agg.validate(numDataColumns = 2) }
    }

    it("rejects a negative ooo-tolerance when aggregators are defined") {
      val agg = AggregationConfig(ColumnAggregator.parseAll(Seq("dSum(1)")), 60000L, -1L)
      intercept[IllegalArgumentException] { agg.validate(numDataColumns = 2) }
    }

    it("is a no-op for an empty config regardless of column count") {
      noException should be thrownBy AggregationConfig.empty.validate(numDataColumns = 1)
    }
  }

  describe("IngestionConfig threading of the aggregation block") {

    it("populates aggregationConfig when the source config has an aggregation block") {
      val ic = IngestionConfig(sourceConfig(
        """
          |aggregation {
          |  aggregators   = ["dSum(1)", "hSum(3)"]
          |  interval      = 1m
          |  ooo-tolerance = 2m
          |}
        """.stripMargin), sourceFactory).get
      ic.aggregationConfig.nonEmpty shouldEqual true
      ic.aggregationConfig.aggregators.length shouldEqual 2
      ic.aggregationConfig.intervalMs shouldEqual 60000L
      ic.aggregationConfig.oooToleranceMs shouldEqual 120000L
    }

    it("leaves aggregationConfig empty when there is no aggregation block") {
      val ic = IngestionConfig(sourceConfig(""), sourceFactory).get
      ic.aggregationConfig shouldEqual AggregationConfig.empty
      ic.aggregationConfig.nonEmpty shouldEqual false
    }
  }
}

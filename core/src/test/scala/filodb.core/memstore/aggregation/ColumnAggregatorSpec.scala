package filodb.core.memstore.aggregation

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ColumnAggregatorSpec extends AnyFunSpec with Matchers {

  describe("ColumnAggregator.parse") {
    it("should parse dSum(1)") {
      val agg = ColumnAggregator.parse("dSum(1)")
      agg.columnId shouldEqual 1
      agg.aggType shouldEqual AggregationType.Sum
    }

    it("should parse hSum(3)") {
      val agg = ColumnAggregator.parse("hSum(3)")
      agg.columnId shouldEqual 3
      agg.aggType shouldEqual AggregationType.HistogramSum
    }

    it("should parse dMin(4)") {
      val agg = ColumnAggregator.parse("dMin(4)")
      agg.columnId shouldEqual 4
      agg.aggType shouldEqual AggregationType.Min
    }

    it("should parse dMax(5)") {
      val agg = ColumnAggregator.parse("dMax(5)")
      agg.columnId shouldEqual 5
      agg.aggType shouldEqual AggregationType.Max
    }

    it("should parse dLast(6)") {
      val agg = ColumnAggregator.parse("dLast(6)")
      agg.columnId shouldEqual 6
      agg.aggType shouldEqual AggregationType.Last
    }

    it("should parse dFirst(2)") {
      val agg = ColumnAggregator.parse("dFirst(2)")
      agg.columnId shouldEqual 2
      agg.aggType shouldEqual AggregationType.First
    }

    it("should parse dAvg(1)") {
      val agg = ColumnAggregator.parse("dAvg(1)")
      agg.columnId shouldEqual 1
      agg.aggType shouldEqual AggregationType.Avg
    }

    it("should parse dCount(1)") {
      val agg = ColumnAggregator.parse("dCount(1)")
      agg.columnId shouldEqual 1
      agg.aggType shouldEqual AggregationType.Count
    }

    it("should parse hLast(3)") {
      val agg = ColumnAggregator.parse("hLast(3)")
      agg.columnId shouldEqual 3
      agg.aggType shouldEqual AggregationType.HistogramLast
    }

    it("should throw on unknown aggregator name") {
      intercept[IllegalArgumentException] {
        ColumnAggregator.parse("unknown(1)")
      }
    }

    it("should throw on missing column id") {
      intercept[IllegalArgumentException] {
        ColumnAggregator.parse("dSum")
      }
    }

    it("should throw on invalid column id") {
      intercept[NumberFormatException] {
        ColumnAggregator.parse("dSum(abc)")
      }
    }
  }

  describe("ColumnAggregator.parseAll") {
    it("should parse a list of aggregator strings") {
      val aggs = ColumnAggregator.parseAll(Seq("dSum(1)", "dSum(2)", "hSum(3)", "dMin(4)", "dMax(5)", "dLast(6)"))
      aggs.length shouldEqual 6
      aggs(0) shouldEqual ColumnAggregator(1, AggregationType.Sum)
      aggs(1) shouldEqual ColumnAggregator(2, AggregationType.Sum)
      aggs(2) shouldEqual ColumnAggregator(3, AggregationType.HistogramSum)
      aggs(3) shouldEqual ColumnAggregator(4, AggregationType.Min)
      aggs(4) shouldEqual ColumnAggregator(5, AggregationType.Max)
      aggs(5) shouldEqual ColumnAggregator(6, AggregationType.Last)
    }

    it("should return empty seq for empty input") {
      val aggs = ColumnAggregator.parseAll(Seq.empty)
      aggs shouldBe empty
    }
  }
}

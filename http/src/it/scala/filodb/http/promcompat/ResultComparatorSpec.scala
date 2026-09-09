package filodb.http.promcompat

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ResultComparatorSpec extends AnyFunSpec with Matchers {

  def vec(metric: Map[String, String], ts: Long, v: Double) =
    PromResult("success", Some("vector"), Seq(PromSeries(metric, Seq(PromPoint(ts, v)))), None)

  def matrix(metric: Map[String, String], points: Seq[PromPoint]) =
    PromResult("success", Some("matrix"), Seq(PromSeries(metric, points)), None)

  it("parses a Prometheus vector response") {
    val json = """{"status":"success","data":{"resultType":"vector",
      |"result":[{"metric":{"__name__":"my_gauge","mode":"idle"},"value":[100,"3.5"]}]}}""".stripMargin
    val r = PromResult.parse(json)
    r.status shouldEqual "success"
    r.resultType shouldEqual Some("vector")
    r.series should have length 1
    r.series.head.metric("mode") shouldEqual "idle"
    r.series.head.points.head shouldEqual PromPoint(100, 3.5)
  }

  it("parses a matrix response") {
    val json = """{"status":"success","data":{"resultType":"matrix",
      |"result":[{"metric":{"mode":"idle"},"values":[[100,"1"],[115,"2"]]}]}}""".stripMargin
    val r = PromResult.parse(json)
    r.series.head.points shouldEqual Seq(PromPoint(100, 1.0), PromPoint(115, 2.0))
  }

  it("Tolerance parses percent, absolute and exact") {
    Tolerance.parse("0.1%") shouldEqual Tolerance(0.0, 0.001)
    Tolerance.parse("0.001") shouldEqual Tolerance(0.001, 0.0)
    Tolerance.parse("") shouldEqual Tolerance(0.0, 0.0)
  }

  it("matches identical series regardless of the name label") {
    val f = vec(Map("_metric_" -> "my_gauge", "mode" -> "idle"), 100, 3.5)
    val p = vec(Map("__name__" -> "my_gauge", "mode" -> "idle"), 100, 3.5)
    ResultComparator.compare(f, p, Tolerance(0, 0), ignoreLabels = false).matched shouldEqual true
  }

  it("matches within absolute tolerance and fails outside it") {
    val f = vec(Map("mode" -> "idle"), 100, 3.5001)
    val p = vec(Map("mode" -> "idle"), 100, 3.5)
    ResultComparator.compare(f, p, Tolerance(0.001, 0), ignoreLabels = false).matched shouldEqual true
    ResultComparator.compare(f, p, Tolerance(0.00001, 0), ignoreLabels = false).matched shouldEqual false
  }

  it("fails on differing timestamps") {
    val f = vec(Map("mode" -> "idle"), 101, 3.5)
    val p = vec(Map("mode" -> "idle"), 100, 3.5)
    ResultComparator.compare(f, p, Tolerance(0, 0), ignoreLabels = false).matched shouldEqual false
  }

  it("fails on differing series counts") {
    val f = PromResult("success", Some("vector"), Seq(
      PromSeries(Map("mode" -> "idle"), Seq(PromPoint(100, 1))),
      PromSeries(Map("mode" -> "user"), Seq(PromPoint(100, 2)))), None)
    val p = vec(Map("mode" -> "idle"), 100, 1)
    ResultComparator.compare(f, p, Tolerance(0, 0), ignoreLabels = false).matched shouldEqual false
  }

  it("propagates an error status as a mismatch") {
    val f = PromResult("error", None, Nil, Some("boom"))
    val p = vec(Map("mode" -> "idle"), 100, 1)
    ResultComparator.compare(f, p, Tolerance(0, 0), ignoreLabels = false).matched shouldEqual false
  }

  it("drops a point where either side is NaN at a shared timestamp, and matches on the rest") {
    val f = matrix(Map("mode" -> "idle"), Seq(PromPoint(100, Double.NaN), PromPoint(115, 2.0)))
    val p = matrix(Map("mode" -> "idle"), Seq(PromPoint(100, 3.5), PromPoint(115, 2.0)))
    ResultComparator.compare(f, p, Tolerance(0, 0), ignoreLabels = false).matched shouldEqual true
  }

  it("fails when a non-NaN timestamp is genuinely missing on one side") {
    val f = matrix(Map("mode" -> "idle"), Seq(PromPoint(100, 1.0), PromPoint(115, 2.0)))
    val p = matrix(Map("mode" -> "idle"), Seq(PromPoint(100, 1.0)))
    ResultComparator.compare(f, p, Tolerance(0, 0), ignoreLabels = false).matched shouldEqual false
  }

  it("matches within percent tolerance and fails outside it") {
    val f = vec(Map("mode" -> "idle"), 100, 100.05)
    val p = vec(Map("mode" -> "idle"), 100, 100.0)
    ResultComparator.compare(f, p, Tolerance.parse("0.1%"), ignoreLabels = false).matched shouldEqual true

    val f2 = vec(Map("mode" -> "idle"), 100, 101.0)
    val p2 = vec(Map("mode" -> "idle"), 100, 100.0)
    ResultComparator.compare(f2, p2, Tolerance.parse("0.1%"), ignoreLabels = false).matched shouldEqual false
  }

  it("ignoreLabels compares single series by value only, even with different labels") {
    val f = vec(Map("mode" -> "idle"), 100, 3.5)
    val p = vec(Map("mode" -> "user"), 100, 3.5)
    ResultComparator.compare(f, p, Tolerance(0, 0), ignoreLabels = true).matched shouldEqual true
  }
}

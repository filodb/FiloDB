package filodb.http.promcompat

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class DataGenSpec extends AnyFunSpec with Matchers {
  val end = 1_700_000_010_000L  // arbitrary fixed ms aligned to 15s

  it("is deterministic") {
    val a = DataGen.dataset(end).gaugeCsv
    val b = DataGen.dataset(end).gaugeCsv
    a shouldEqual b
  }

  it("produces 6 gauge and 6 counter series") {
    val d = DataGen.dataset(end)
    d.series.count(_._1.metric == "my_gauge") shouldEqual 6
    d.series.count(_._1.metric == "my_counter") shouldEqual 6
  }

  it("gauge CSV header and label alignment match FiloDB expectations") {
    val csv = DataGen.dataset(end).gaugeCsv
    csv.linesIterator.next() shouldEqual "_metric_,tags,timestamp,value"
    csv should include ("_ws_=test_ws,_ns_=promcompat,mode=idle,instance=inst-a")
  }

  it("counter values are monotonically non-decreasing per series") {
    val d = DataGen.dataset(end)
    d.series.filter(_._1.metric == "my_counter").foreach { case (_, points) =>
      points.map(_.value).sliding(2).foreach { case Seq(x, y) => y should be >= x; case _ => }
    }
  }

  it("writeRequest carries __name__ and the same 12 series") {
    val wr = DataGen.dataset(end).writeRequest
    wr.getTimeseriesCount shouldEqual 12
    val labels = wr.getTimeseries(0).getLabelsList
    val names = (0 until labels.size()).map(i => labels.get(i).getName).toSet
    names should contain ("__name__")
    names should contain allOf ("_ws_", "_ns_", "mode", "instance")
  }
}

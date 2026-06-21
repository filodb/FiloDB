package filodb.core.memstore.ratelimit

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.DatasetRef

class QuotaBreachEventSpec extends AnyFunSpec with Matchers {

  private val ref = DatasetRef("prometheus")

  describe("QuotaBreachEvent.apply") {
    it("extracts ws and ns from a length-2 prefix") {
      val e = QuotaBreachEvent(ref, Seq("myWs", "myNs"), 5, 100000L, "raw", "p1", 1L)
      e.workspace shouldEqual "myWs"
      e.namespace shouldEqual "myNs"
      e.shardKeyPrefix shouldEqual Seq("myWs", "myNs")
      e.dedupKey shouldEqual (("prometheus", "myWs", "myNs"))
    }

    it("preserves a length-3 (metric-level) prefix while keeping ws/ns extraction") {
      val e = QuotaBreachEvent(ref, Seq("myWs", "myNs", "metricA"), 5, 100L, "raw", "p1", 1L)
      e.workspace shouldEqual "myWs"
      e.namespace shouldEqual "myNs"
      e.shardKeyPrefix shouldEqual Seq("myWs", "myNs", "metricA")
    }

    it("handles a length-1 prefix by leaving namespace empty") {
      val e = QuotaBreachEvent(ref, Seq("myWs"), 5, 100L, "raw", "p1", 1L)
      e.workspace shouldEqual "myWs"
      e.namespace shouldEqual ""
      e.dedupKey shouldEqual (("prometheus", "myWs", ""))
    }

    it("handles an empty prefix") {
      val e = QuotaBreachEvent(ref, Nil, 5, 100L, "raw", "p1", 1L)
      e.workspace shouldEqual ""
      e.namespace shouldEqual ""
      e.dedupKey shouldEqual (("prometheus", "", ""))
    }
  }

  describe("QuotaBreachEvent.toJson") {
    it("emits a syntactically valid JSON object with all fields") {
      val e = QuotaBreachEvent(ref, Seq("ws1", "ns1"), 7, 50000L, "raw", "us-east-1", 1718000000000L)
      val json = e.toJson
      json should startWith("{")
      json should endWith("}")
      json should include(""""schemaVersion":1""")
      json should include(""""eventType":"QUOTA_BREACH"""")
      json should include(""""dataset":"prometheus"""")
      json should include(""""workspace":"ws1"""")
      json should include(""""namespace":"ns1"""")
      json should include(""""shardNum":7""")
      json should include(""""quota":50000""")
      json should include(""""clusterType":"raw"""")
      json should include(""""partition":"us-east-1"""")
      json should include(""""breachedAtMillis":1718000000000""")
      json should include(""""shardKeyPrefix":["ws1","ns1"]""")
    }

    it("escapes control characters and quotes in workspace/namespace") {
      val tricky = "we\"ird\\ws\nname"
      val e = QuotaBreachEvent(ref, Seq(tricky, "ns"), 0, 1L, "raw", "p", 0L)
      val json = e.toJson
      // Round-trip the workspace through the escape function: the JSON should contain the
      // escaped form so the resulting payload is parseable by any compliant JSON consumer.
      json should include("we\\\"ird\\\\ws\\nname")
    }
  }
}

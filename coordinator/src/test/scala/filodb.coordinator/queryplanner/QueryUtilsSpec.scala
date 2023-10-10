package filodb.coordinator.queryplanner

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class QueryUtilsSpec extends AnyFunSpec with Matchers {
  it("should correctly calculate combinations") {
    // Pairs of input choice lists and output combinations.
    val testPairs = Seq(
      (Seq(), Seq()),
      (
        Seq(Seq(1)),
        Seq(Seq(1))
      ),
      (
        Seq(Seq(1, 2)),
        Seq(
          Seq(1),
          Seq(2))
      ),
      (
        Seq(
          Seq(1),
          Seq(2)),
        Seq(Seq(1, 2))
      ),
      (
        Seq(
          Seq(1),
          Seq(2, 3)),
        Seq(
          Seq(1, 2),
          Seq(1, 3))
      ),
      (
        Seq(
          Seq(1, 2),
          Seq(3, 4)),
        Seq(
          Seq(1, 3),
          Seq(1, 4),
          Seq(2, 3),
          Seq(2, 4))
      )
    )
    for ((choices, expected) <- testPairs) {
      val res = QueryUtils.combinations(choices)
      res.toSet shouldEqual expected.toSet
    }
  }
}

package filodb.core.query

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class SerializedRangeVectorSpec  extends AnyFunSpec with Matchers {

  it("should remove NaNs at encoding and add NaNs on decoding") {
    // TODO
  }

  it("should remove Hist.empty at encoding and add Hist.empty on decoding") {
    // TODO
  }

  it("should not bother removing when start == end even if there is NaN. This may contain Raw data") {
    // TODO
  }

}

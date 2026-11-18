package filodb.http.promcompat

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.xerial.snappy.Snappy
import remote.RemoteStorage.WriteRequest

class PromRemoteClientSpec extends AnyFunSpec with Matchers {
  it("builds an instant query URL") {
    PromRemoteClient.instantUrl("http://h:9090", "sum(up)", 100) shouldEqual
      "http://h:9090/api/v1/query?query=sum%28up%29&time=100"
  }

  it("builds a range query URL") {
    PromRemoteClient.rangeUrl("http://h:9090", "up", 100, 200, 15) shouldEqual
      "http://h:9090/api/v1/query_range?query=up&start=100&end=200&step=15"
  }

  it("encodes a WriteRequest as snappy(protobuf) that round-trips") {
    val wr = DataGen.dataset(1_700_000_010_000L).writeRequest
    val bytes = PromRemoteClient.encode(wr)
    val back = WriteRequest.parseFrom(Snappy.uncompress(bytes))
    back.getTimeseriesCount shouldEqual wr.getTimeseriesCount
  }
}

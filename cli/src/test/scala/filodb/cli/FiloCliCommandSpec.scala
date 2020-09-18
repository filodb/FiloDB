package filodb.cli

import org.rogach.scallop.exceptions.ScallopException
import org.scalatest.funspec.AnyFunSpec

class FiloCliCommandSpec extends AnyFunSpec {

  it("Testing for filo cli command to pass scallops options verification"){

    parseSucessFully("--host localhost --command indexnames --dataset prometheus")
    parseSucessFully("--host localhost --port 6564 --command indexvalues --indexname asdasd --dataset prometheus --shards SS")
    parseSucessFully("""--host localhost --port 6564 --dataset "adadasd" --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --start 1212 --step 5555 --end 1212""")
    parseSucessFully("--host localhost --port 6564 --command timeseriesmetadata --matcher a=b --dataset prometheus --start 123123 --end 13123")
    parseSucessFully("--host localhost --port 6564 --command labelvalues --labelnames a --labelfilter a=b --dataset prometheus")
    parseSucessFully("""--command promFilterToPartKeyBR --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --schema prom-counter""")
    parseSucessFully("""--command partKeyBrAsString --hexpk 0x2C0000000F1712000000200000004B8B36940C006D794D65747269634E616D650E00C104006D794E73C004006D795773""")
    parseSucessFully("""--command decodeChunkInfo --hexchunkinfo 0x12e8253a267ea2db060000005046fc896e0100005046fc896e010000""")
    parseSucessFully("""--command decodeVector --hexvector 0x1b000000080800000300000000000000010000000700000006080400109836 --vectortype d""")
  }


  it("Testing for filo cli command to check for failures with invalid options "){
    parserError("""--host localhost --port 6564 --metriccolumn adasdasd --dataset "adadasd" --promql "myMetricName{_ws_='myWs',_ns_='myNs'}" --start 1231673123675123 --step 13131312313123123 --end 5""")
    parserError("""--command partKeyBrAsString --hexPk 0x2C0000000F1712000000200000004B8B36940C006D794D65747269634E616D650E00C104006D794E73C004006D795773""")
    parserError("""--command decodeChunkInfo --hexChunkInfo 0x12e8253a267ea2db060000005046fc896e0100005046fc896e010000""")
    parserError("""--command decodeVector --hexVector 0x1b000000080800000300000000000000010000000700000006080400109836 --vectortype d""")
  }


  def parseSucessFully(commandLine: String): Unit = {
    new Arguments(commandLine.split(" "))
  }

  def parserError(commandLine: String):Unit = {
    intercept[ScallopException]{
      new Arguments(commandLine.split(" "))
    }
  }

}

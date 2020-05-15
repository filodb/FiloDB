package filodb.query.exec

import filodb.query.exec.rangefn.{RawDataWindowingSpec, TimestampChunkedFunction}

import scala.concurrent.duration._

class TimestampFunctionSpec extends RawDataWindowingSpec {
  val w = 5.minutes.toMillis // window size = lookback time

  it("should work for various start times") {
    var data = Seq(1.5, 2.5, 3.5, 4.5, 5.5)
    val rv = timeValueRV(data)
    val list = rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList

    val chunkedIt = new ChunkedWindowIteratorD(rv, 100000, 5000, 120000, w,
      new TimestampChunkedFunction, querySession)

    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    val expectedResult = List((100000, 100), (105000, 100), (110000, 110), (115000, 110), (120000, 120))
    aggregated.sameElements(expectedResult) shouldEqual (true)

  }

  it("should work with NaN") {
    var data = Seq(1.5, 2.5, 3.5)
    val rv = timeValueRV(data)
    val list = rv.rows.map(x => (x.getLong(0), x.getDouble(1))).toList

    val chunkedIt = new ChunkedWindowIteratorD(rv, 95000, 50000, 450000, w,
      new TimestampChunkedFunction, querySession)

    val aggregated = chunkedIt.map(x => (x.getLong(0), x.getDouble(1))).toList
    val expectedResult = List((95000, Double.NaN), (145000, 120d), (195000, 120d), (245000, 120d), (295000, 120d), (345000, 120d), (395000, 120d), (445000, Double.NaN))

    (aggregated zip expectedResult).map {
      case (a, e) => {
        a._1 shouldEqual e._1
        val val1: Double = a._2
        val val2: Double = e._2
        if (val1.isNaN) val2.isNaN shouldEqual true else val1 shouldEqual (val2)
      }
    }
  }
}

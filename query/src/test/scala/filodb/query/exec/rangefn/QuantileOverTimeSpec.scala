package filodb.query.exec.rangefn

import filodb.core.query.TransientRow
import filodb.query.exec.{QueueBasedWindow, StaticFuncArgs}
import filodb.query.util.IndexedArrayQueue

class QuantileOverTimeSpec extends RawDataWindowingSpec {


  it("normal case") {
    val gaugeSamples = Seq(
      8072000L->7419.00,
      8082100L->Double.NaN,
      8092196L->4614.00,
      8102215L->4909.00,
      8112223L->4909.00,
      8122388L->4948.00,
      8132570L->Double.NaN,
      8142822L->Double.NaN,
      8152858L->Double.NaN,
      8162999L->8201.00
    )

    val window = getWindow(gaugeSamples)
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val f = new QuantileOverTimeFunction(Seq(new StaticFuncArgs(0.5,null)))
    f.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 4928.5
  }

  it("should return NaN on empty range") {
    val gaugeSamples = Seq(
      8082100L->Double.NaN,
      8132570L->Double.NaN,
      8142822L->Double.NaN,
      8152858L->Double.NaN
    )

    val window = getWindow(gaugeSamples)
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val f = new QuantileOverTimeFunction(Seq(new StaticFuncArgs(0.5,null)))
    f.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value.isNaN shouldEqual true
  }

  it("all values are the same") {
    val gaugeSamples = Seq(
      8082100L->8201.00,
      8132570L->8201.00,
      8142822L->8201.00,
      8152858L->8201.00
    )

    val window = getWindow(gaugeSamples)
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val f = new QuantileOverTimeFunction(Seq(new StaticFuncArgs(0.2,null)))
    f.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 8201
  }

  it("only one value") {
    val gaugeSamples = Seq(
      8082100L->Double.NaN,
      8132570L->8201.00,
      8082100L->Double.NaN,
      8152858L->Double.NaN
    )

    val window = getWindow(gaugeSamples)
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val f = new QuantileOverTimeFunction(Seq(new StaticFuncArgs(0.2,null)))
    f.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 8201
  }

  it("checking 0.2") {
    val gaugeSamples = Seq(
      8072000L->1.00,
      8082100L->2.00,
      8092196L->3.00,
      8102215L->4.00,
      8112223L->5.00,
      8122388L->6.00,
      8132570L->7.00,
      8142822L->8.00,
      8152858L->9.00,
      8162999L->10.00
    )

    val window = getWindow(gaugeSamples)
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val f = new QuantileOverTimeFunction(Seq(new StaticFuncArgs(0.2,null)))
    f.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 2.8000000000000003
  }

  it("checking 0.9") {
    val gaugeSamples = Seq(
      8072000L->1.00,
      8082100L->2.00,
      8092196L->3.00,
      8102215L->4.00,
      8112223L->5.00,
      8122388L->6.00,
      8132570L->7.00,
      8142822L->8.00,
      8152858L->9.00,
      8162999L->10.00
    )

    val window = getWindow(gaugeSamples)
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    val f = new QuantileOverTimeFunction(Seq(new StaticFuncArgs(0.9,null)))
    f.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 9.1
  }



  def getWindow(samples : Seq[(Long,Double)]) : Window[TransientRow] = {
    val q = new IndexedArrayQueue[TransientRow]()
    samples.foreach { case (t, v) =>
      val s = new TransientRow(t, v)
      q.add(s)
    }
    val window = new QueueBasedWindow(q)
    window
  }

}

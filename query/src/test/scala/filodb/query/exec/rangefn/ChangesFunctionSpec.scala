package filodb.query.exec.rangefn

import filodb.core.query.TransientRow
import filodb.query.exec.QueueBasedWindow
import filodb.query.util.IndexedArrayQueue

class ChangesFunctionSpec extends RawDataWindowingSpec {


  it("should calculate normal case") {
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
    ChangesOverTimeFunction.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 4
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
    ChangesOverTimeFunction.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value.isNaN shouldEqual true
  }

  it("should return 0 when no changes") {
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
    ChangesOverTimeFunction.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 0
  }

  it("should return 0 when padded with NaN") {
    val gaugeSamples = Seq(
      8082100L->Double.NaN,
      8132570L->8201.00,
      8142822L->8201.00,
      8152858L->Double.NaN
    )

    val window = getWindow(gaugeSamples)
    val startTs = 8071950L
    val endTs =   8163070L
    val toEmit = new TransientRow
    ChangesOverTimeFunction.apply(startTs,endTs, window, toEmit, queryConfig)
    toEmit.value shouldEqual 0
  }



  def getWindow(samples : Seq[(Long,Double)]) : Window = {
    val q = new IndexedArrayQueue[TransientRow]()
    samples.foreach { case (t, v) =>
      val s = new TransientRow(t, v)
      q.add(s)
    }
    val window = new QueueBasedWindow(q)
    window
  }

}

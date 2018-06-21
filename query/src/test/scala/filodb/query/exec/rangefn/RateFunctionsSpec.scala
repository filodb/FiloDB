package filodb.query.exec.rangefn

import scala.util.Random

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import filodb.query.QueryConfig
import filodb.query.exec.{QueueBasedWindow, TransientRow}
import filodb.query.util.IndexedArrayQueue

class RateFunctionsSpec extends FunSpec with Matchers {

  val rand = new Random()
  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))

  val counterSamples = Seq(  8072000L->4419.00,
                      8082100L->4511.00,
                      8092196L->4614.00,
                      8102215L->4724.00,
                      8112223L->4909.00,
                      8122388L->4948.00,
                      8132570L->5000.00,
                      8142822L->5095.00,
                      8152858L->5102.00,
                      8163000L->5201.00)

  val q = new IndexedArrayQueue[TransientRow]()
  counterSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    q.add(s)
  }
  val counterWindow = new QueueBasedWindow(q)

  val gaugeSamples = Seq(   8072000L->7419.00,
                            8082100L->5511.00,
                            8092196L->4614.00,
                            8102215L->3724.00,
                            8112223L->4909.00,
                            8122388L->4948.00,
                            8132570L->5000.00,
                            8142822L->3095.00,
                            8152858L->5102.00,
                            8163000L->8201.00)

  val q2 = new IndexedArrayQueue[TransientRow]()
  gaugeSamples.foreach { case (t, v) =>
    val s = new TransientRow(t, v)
    q2.add(s)
  }
  val gaugeWindow = new QueueBasedWindow(q2)

  val errorOk = 0.0000001

  // Basic test cases covered
  // TODO Extrapolation special cases not done

  it ("rate should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q.last.value - q.head.value) / (q.last.timestamp - q.head.timestamp) * 1000
    val toEmit = new TransientRow
    RateFunction.apply(startTs,endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("increase should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q.last.value - q.head.value) / (q.last.timestamp - q.head.timestamp) * (endTs - startTs)
    val toEmit = new TransientRow
    IncreaseFunction.apply(startTs,endTs, counterWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

  it ("delta should work when start and end are outside window") {
    val startTs = 8071950L
    val endTs =   8163070L
    val expected = (q2.last.value - q2.head.value) / (q2.last.timestamp - q2.head.timestamp) * (endTs - startTs)
    val toEmit = new TransientRow
    DeltaFunction.apply(startTs,endTs, gaugeWindow, toEmit, queryConfig)
    Math.abs(toEmit.value - expected) should be < errorOk
  }

}

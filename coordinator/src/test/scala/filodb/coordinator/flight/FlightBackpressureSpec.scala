package filodb.coordinator.flight

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration._

import monix.execution.Scheduler
import org.apache.arrow.flight.{FlightRuntimeException, FlightProducer}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.QueryTimeoutException
import filodb.core.query.{PlannerParams, QueryContext}

class FlightBackpressureSpec extends AnyFunSpec with Matchers with ScalaFutures {

  implicit val scheduler: Scheduler = Scheduler.global
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(20, Millis))

  /** Stub listener built via a dynamic proxy so only the flow-control methods need handling. */
  class StubListener {
    val ready = new AtomicBoolean(false)
    val cancelled = new AtomicBoolean(false)
    @volatile var onReady: Runnable = _
    @volatile var onCancel: Runnable = _
    val listener: FlightProducer.ServerStreamListener = Proxy.newProxyInstance(
      getClass.getClassLoader, Array(classOf[FlightProducer.ServerStreamListener]),
      new InvocationHandler {
        override def invoke(proxy: Any, m: Method, args: Array[AnyRef]): AnyRef = m.getName match {
          case "isReady" => Boolean.box(ready.get)
          case "isCancelled" => Boolean.box(cancelled.get)
          case "setOnReadyHandler" => onReady = args(0).asInstanceOf[Runnable]; null
          case "setOnCancelHandler" => onCancel = args(0).asInstanceOf[Runnable]; null
          case other => throw new UnsupportedOperationException(other)
        }
      }).asInstanceOf[FlightProducer.ServerStreamListener]
    def setReady(): Unit = { ready.set(true); onReady.run() }
    def cancel(): Unit = { cancelled.set(true); onCancel.run() }
  }

  private def qc(timeoutMs: Int = 60000) = QueryContext(plannerParams = PlannerParams(queryTimeoutMillis = timeoutMs))

  it("should complete immediately when listener is ready") {
    val s = new StubListener
    s.ready.set(true)
    new FlightBackpressure(s.listener, qc()).awaitReady.runToFuture.futureValue shouldEqual (())
  }

  it("should suspend until listener becomes ready") {
    val s = new StubListener
    val bp = new FlightBackpressure(s.listener, qc())
    val f = bp.awaitReady.runToFuture
    Thread.sleep(200)
    f.isCompleted shouldEqual false
    s.setReady()
    f.futureValue shouldEqual (())
  }

  it("should fail with CANCELLED when client cancels while waiting") {
    val s = new StubListener
    val bp = new FlightBackpressure(s.listener, qc())
    val f = bp.awaitReady.runToFuture
    Thread.sleep(100)
    s.cancel()
    f.failed.futureValue shouldBe a[FlightRuntimeException]
    bp.awaitReady.runToFuture.failed.futureValue shouldBe a[FlightRuntimeException]
  }

  it("should fail with QueryTimeoutException when deadline passes while waiting") {
    val s = new StubListener
    val bp = new FlightBackpressure(s.listener, qc(timeoutMs = 300))
    bp.awaitReady.runToFuture.failed.futureValue shouldBe a[QueryTimeoutException]
  }
}

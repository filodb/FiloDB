package filodb.http

import com.typesafe.config.ConfigFactory
import io.grpc.Metadata
import kamon.Kamon
import kamon.context.{BinaryPropagation, Context}
import kamon.trace.{Identifier, Span}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class TracingUtilSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  // Production runs with 16 byte (double) trace identifiers
  override def beforeAll(): Unit =
    Kamon.init(ConfigFactory.parseString("kamon.trace.identifier-scheme = double").withFallback(ConfigFactory.load()))

  override def afterAll(): Unit = Kamon.stop()

  private def b3Headers(traceId: String): Metadata = {
    val md = new Metadata()
    def put(k: String, v: String): Unit = md.put(Metadata.Key.of(k, Metadata.ASCII_STRING_MARSHALLER), v)
    put("X-B3-TraceId", traceId)
    put("X-B3-SpanId", "1234567890abcdef")
    put("X-B3-Sampled", "1")
    md
  }

  // Simulates the akka remoting hop from the node receiving the gRPC call to the node running the plan
  private def viaBinaryPropagation(span: Span): Span = {
    val out = new java.io.ByteArrayOutputStream()
    Kamon.defaultBinaryPropagation().write(Context.of(Span.Key, span), BinaryPropagation.ByteStreamWriter.of(out))
    Kamon.defaultBinaryPropagation().read(BinaryPropagation.ByteStreamReader.of(out.toByteArray)).get(Span.Key)
  }

  it("should keep a 128 bit trace id across the akka hop") {
    val traceId = "0af7651916cd43dd8448eb211c80319c"
    val span = TracingUtil.startAndGetCurrentSpan(b3Headers(traceId))
    span.trace.id.string shouldEqual traceId
    viaBinaryPropagation(span).trace.id.string shouldEqual traceId
  }

  it("should left pad a 64 bit trace id to 128 bits") {
    val span = TracingUtil.startAndGetCurrentSpan(b3Headers("8448eb211c80319c"))
    span.trace.id.string shouldEqual "00000000000000008448eb211c80319c"
    viaBinaryPropagation(span).trace.id.string shouldEqual "00000000000000008448eb211c80319c"
  }

  it("should start a new trace when the trace id header cannot be parsed") {
    Seq("0AF7651916CD43DD8448EB211C80319C", "448eb211c80319c", "not-a-trace-id", "").foreach { traceId =>
      val span = TracingUtil.startAndGetCurrentSpan(b3Headers(traceId))
      span.trace.id shouldNot equal(Identifier.Empty)
      viaBinaryPropagation(span).trace.id shouldEqual span.trace.id
    }
  }
}

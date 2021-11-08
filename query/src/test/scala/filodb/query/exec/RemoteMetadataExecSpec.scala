package filodb.query.exec

import com.softwaremill.sttp.{Response, StatusCodes, SttpBackend}
import com.softwaremill.sttp.testing.SttpBackendStub
import com.typesafe.config.ConfigFactory
import filodb.core.MetricsTestData._
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, TimeSeriesMemStore}
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.query._
import filodb.query.exec.RemoteHttpClient.configBuilder
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class RemoteMetadataExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val jobQueryResult1 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03BCtag"))
  val jobQueryResult2 = Array("http_req_total", "http_resp_time")
  val jobQueryResult3 = Array("job", "__name__", "unicode_tag", "instance")

  implicit val testingBackend: SttpBackend[Future, Nothing] = SttpBackendStub.asynchronousFuture
    .whenRequestMatches(request =>
      request.uri.path.startsWith(List("api","v1","label")) && request.uri.path.last == "values"
    )
    .thenRespondWrapped(Future {
      Response(Right(Right(MetadataSuccessResponse(Seq(LabelSampl(Seq("http_req_total", "http_resp_time"))), "success", Option.empty, Option.empty))), StatusCodes.PartialContent, "", Nil, Nil)
    })
    .whenRequestMatches(request =>
      request.uri.path.startsWith(List("api","v1","series"))
    )
    .thenRespondWrapped(Future {
      Response(Right(Right(MetadataSuccessResponse(Seq(MetadataSampl(Map(("job" -> "myCoolService"), ("unicode_tag" -> "uniμtag")))), "success", Option.empty, Option.empty))), StatusCodes.PartialContent, "", Nil, Nil)
    })
    .whenRequestMatches(_.uri.path.startsWith(List("api","v1","labels"))
    )
    .thenRespondWrapped(Future {
      Response(Right(Right(MetadataSuccessResponse(Seq(LabelSampl(Seq("job", "__name__", "unicode_tag", "instance"))), "success", Option.empty, Option.empty))), StatusCodes.PartialContent, "", Nil, Nil)
    })

  it ("series matcher remote exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/series", 10000L, Map("filter" -> "a=b,c=d"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/series"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val resp = exec.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next.asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
      }
    }
    result shouldEqual jobQueryResult1
  }

  it ("label values remote metadata exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/label/__name__/values", 10000L, Map("filter" -> "a=b,c=d"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/label"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val resp = exec.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 2
        rv.rows.map(row => {
          val record = row.asInstanceOf[BinaryRecordRowReader]
          rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset).head._2
        })
      }
    }
    result.toArray shouldEqual jobQueryResult2
  }

  it ("labels metadata remote exec") {
    val exec: MetadataRemoteExec = MetadataRemoteExec("http://localhost:31007/api/v1/labels", 10000L, Map("filter" -> "a=b,c=d"),
      QueryContext(origQueryParams=PromQlQueryParams("test", 123L, 234L, 15L, Option("http://localhost:31007/api/v1/labels"))),
      InProcessPlanDispatcher(queryConfig), timeseriesDataset.ref, RemoteHttpClient(configBuilder.build(), testingBackend))

    val resp = exec.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 4
        rv.rows.map(row => {
          val record = row.asInstanceOf[BinaryRecordRowReader]
          rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset).head._2
        })
      }
    }
    result.toArray shouldEqual jobQueryResult3
  }

}


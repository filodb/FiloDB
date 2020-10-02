package filodb.query.exec

import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query.{PromQlQueryParams, QueryContext}
import filodb.memory.format.vectors.{MutableHistogram}
import filodb.query
import filodb.query.{Data, HistSampl, QueryResponse, QueryResult, Sampl}


class PromQlRemoteExecSpec extends AnyFunSpec with Matchers with ScalaFutures {
  val timeseriesDataset = Dataset.make("timeseries",
    Seq("tags:map"),
    Seq("timestamp:ts", "value:double:detectDrops=true"),
    options = DatasetOptions(Seq("__name__", "job"), "__name__")).get

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???
  }

  val params = PromQlQueryParams("", 0, 0 , 0)
  val queryContext = QueryContext(origQueryParams = params)
  it ("should convert matrix Data to QueryResponse ") {
    val expectedResult = List((1000000, 1.0), (2000000, 2.0), (3000000, 3.0))
    val exec = PromQlRemoteExec("", 60000, queryContext, dummyDispatcher, timeseriesDataset.ref)
    val result = query.Result (Map("instance" -> "inst1"), Some(Seq(Sampl(1000, 1), Sampl(2000, 2), Sampl(3000, 3))),
      None)
    val res = exec.toQueryResponse(Data("vector", Seq(result)), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(3)
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => (r.getLong(0) , r.getDouble(1)) }.toList)
    data.shouldEqual(expectedResult)
  }

  it ("should convert vector Data to QueryResponse ") {
    val expectedResult = List((1000000, 1.0))
    val exec = PromQlRemoteExec("", 60000, queryContext, dummyDispatcher, timeseriesDataset.ref)
    val result = query.Result (Map("instance" -> "inst1"), None, Some(Sampl(1000, 1)))
    val res = exec.toQueryResponse(Data("vector", Seq(result)), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(1)
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => (r.getLong(0) , r.getDouble(1)) }.toList)
    data.shouldEqual(expectedResult)

  }

  it ("should convert vector Data to QueryResponse for MetadataQuery") {
    val exec = MetadataRemoteExec("", 60000, Map.empty,
      queryContext, dummyDispatcher, timeseriesDataset.ref)
    val map1 = Map("instance" -> "inst-1", "last-sample" -> "6377838" )
    val map2 = Map("instance" -> "inst-2", "last-sample" -> "6377834" )
    val res = exec.toQueryResponse(Seq(map1, map2), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => r.getAny(0) }.toList)
    data(0) shouldEqual(map1)
    data(1) shouldEqual(map2)
  }

  it ("should convert vector Data to QueryResponse for Metadata series query") {
    val exec = MetadataRemoteExec("", 60000, Map.empty, queryContext, dummyDispatcher, timeseriesDataset.ref)
    val map1 = Map("instance" -> "inst-1", "last-sample" -> "6377838" )
    val map2 = Map("instance" -> "inst-2", "last-sample" -> "6377834" )
    val res = exec.toQueryResponse(Seq(map1, map2), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    val data = queryResult.result.flatMap(x => x.rows.map(r => r.getAny(0)).toList)
    data(0) shouldEqual(map1)
    data(1) shouldEqual(map2)
  }

  it ("should convert histogram to QueryResponse ") {
    val exec = PromQlRemoteExec("", 60000, queryContext, dummyDispatcher, timeseriesDataset.ref)
    val result = query.Result (Map("instance" -> "inst1"), None, Some(HistSampl(1000, Map("1" -> 2, "+Inf" -> 3))))
    val res = exec.toQueryResponse(Data("vector", Seq(result)), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(1)
    val data = queryResult.result.flatMap(x => x.rows.map{r => (r.getLong(0) , r.getHistogram(1))}.toList)
    val hist = data.head._2.asInstanceOf[MutableHistogram]
    data.head._1 shouldEqual 1000000
    hist.buckets.allBucketTops shouldEqual Array(1, Double.PositiveInfinity)
    hist.numBuckets shouldEqual(2)
    hist.bucketValue(0) shouldEqual(2.0)
    hist.bucketValue(1) shouldEqual(3)
  }
}

package filodb.query.exec

import com.typesafe.config.ConfigFactory
import kamon.Kamon
import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.core.query.{PromQlQueryParams, QueryContext}
import filodb.query
import filodb.query.{Data, QueryResponse, QueryResult, Sampl}

class PromQlExecSpec extends FunSpec with Matchers with ScalaFutures {
  val timeseriesDataset = Dataset.make("timeseries",
    Seq("tags:map"),
    Seq("timestamp:ts", "value:double:detectDrops=true"),
    options = DatasetOptions(Seq("__name__", "job"), "__name__")).get

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = ???
  }

  val queryContext = QueryContext(origQueryParams = PromQlQueryParams(ConfigFactory.empty(), "", 0, 0 , 0))
  it ("should convert matrix Data to QueryResponse ") {
    val expectedResult = List((1000000, 1.0), (2000000, 2.0), (3000000, 3.0))
    val exec = PromQlExec(queryContext, dummyDispatcher, timeseriesDataset.ref)
    val result = query.Result (Map("instance" ->"inst1"), Some(Seq(Sampl(1000, 1), Sampl(2000, 2), Sampl(3000, 3))), None)
    val res = exec.toQueryResponse(Data("vector", Seq(result)), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(3)
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => (r.getLong(0) , r.getDouble(1)) }.toList)
    data.shouldEqual(expectedResult)

  }

  it ("should convert vector Data to QueryResponse ") {
    val expectedResult = List((1000000, 1.0))
    val exec = PromQlExec(queryContext, dummyDispatcher, timeseriesDataset.ref)
    val result = query.Result (Map("instance" ->"inst1"), None, Some(Sampl(1000, 1)))
    val res = exec.toQueryResponse(Data("vector", Seq(result)), "id", Kamon.currentSpan())
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(1)
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => (r.getLong(0) , r.getDouble(1)) }.toList)
    data.shouldEqual(expectedResult)

  }

}

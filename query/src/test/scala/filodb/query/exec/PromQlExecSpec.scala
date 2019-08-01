package filodb.query.exec

import scala.concurrent.duration.FiniteDuration

import monix.eval.Task
import monix.execution.Scheduler
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.concurrent.ScalaFutures

import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.query
import filodb.query.{Data, PromQlInvocationParams, QueryResponse, QueryResult, Sampl}

class PromQlExecSpec extends FunSpec with Matchers with ScalaFutures {
  
  val timeseriesDataset = Dataset.make("timeseries",
    Seq("tags:map"),
    Seq("timestamp:ts", "value:double:detectDrops=true"),
    Seq("timestamp"),
    Seq.empty,
    DatasetOptions(Seq("__name__", "job"), "__name__", "value")).get

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler,
                          timeout: FiniteDuration): Task[QueryResponse] = ???
  }

  it ("should convert Data to QueryResponse ") {
    val expectedResult = List((1000000, 1.0), (2000000, 2.0), (3000000, 3.0))
    val exec = PromQlExec("test", dummyDispatcher, timeseriesDataset.ref, PromQlInvocationParams("", "", 0, 0 , 0))
    val result = query.Result (Map("instance" ->"inst1"), Seq(Sampl(1000, 1), Sampl(2000, 2), Sampl(3000, 3)))
    val res = exec.toQueryResponse(Data("vector", Seq(result)), "id")
    res.isInstanceOf[QueryResult] shouldEqual true
    val queryResult = res.asInstanceOf[QueryResult]
    queryResult.result(0).numRows.get shouldEqual(3)
    val data = queryResult.result.flatMap(x=>x.rows.map{ r => (r.getLong(0) , r.getDouble(1)) }.toList)
    data.shouldEqual(expectedResult)

  }
}

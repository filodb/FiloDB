package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import filodb.core.MetricsTestData._
import filodb.core.TestData
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class MetadataExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  import ZeroCopyUTF8String._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = new QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val partKeyLabelValues = Seq(
    ("http_req_total", Map("instance" -> "someHost:8787", "job" -> "myCoolService", "unicode_tag" -> "uni\u03C0tag")),
    ("http_resp_time", Map("instance" -> "someHost:8787", "job" -> "myCoolService", "unicode_tag" -> "uni\u03BCtag"))
  )

  val addlLabels = Map("_type_" -> "prom-counter")
  val expectedLabelValues = partKeyLabelValues.map { case (metric, tags) =>
    tags + ("_metric_" -> metric) ++ addlLabels
  }

  val jobQueryResult1 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03C0tag"))
  val jobQueryResult2 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03BCtag"))

  val partTagsUTF8s = partKeyLabelValues.map { case (m, t) => (m, t.map { case (k, v) => (k.utf8, v.utf8) }) }
  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val limit = 2
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
  // Be sure to reset the builder; it is in an Object so static and shared amongst tests
  builder.reset()
  partTagsUTF8s.map { case (metric, partTagsUTF8) =>
    tuples.map { t => SeqRowReader(Seq(t._1, t._2, metric, partTagsUTF8)) }
      .foreach(builder.addFromReader(_, Schemas.promCounter))
  }
  val container = builder.allContainers.head

  implicit val execTimeout = 5.seconds

  override def beforeAll(): Unit = {
    memStore.setup(timeseriesDataset.ref, Schemas(Schemas.promCounter), 0, TestData.storeConf)
    memStore.ingest(timeseriesDataset.ref, 0, SomeData(container, 0))
    memStore.refreshIndexForTesting(timeseriesDataset.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlan)
                         (implicit sched: Scheduler): Task[QueryResponse] = plan.execute(memStore,
      QuerySession(QueryContext(), queryConfig))(sched)

    override def clusterName: String = ???

    override def isLocalCall: Boolean = ???
  }

  it("should read the job names from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = LabelValuesExec(QueryContext(), dummyDispatcher,
      timeseriesDataset.ref, 0, filters, Seq("job", "unicode_tag"), now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next().asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
      }
    }
    result shouldEqual jobQueryResult1
  }

  it("should not return any rows for wrong column filters") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("__name__", Filter.Equals("http_req_total1".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = PartKeysExec(QueryContext(), dummyDispatcher,
      timeseriesDataset.ref, 0, filters, false, now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    (resp: @unchecked) match {
      case QueryResult(_, _, results, _, _, _) => results.size shouldEqual 1
        results(0).rows.size shouldEqual 0
    }
  }

  it("should read the label names/values from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val execPlan = PartKeysExec(QueryContext(), dummyDispatcher,
      timeseriesDataset.ref, 0, filters, false, now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) =>
        response.size shouldEqual 1
        response(0).rows.map { row =>
          val r = row.asInstanceOf[BinaryRecordRowReader]
          response(0).asInstanceOf[SerializedRangeVector]
            .schema.toStringPairs(r.recordBase, r.recordOffset).toMap
        }.toList
    }
    result shouldEqual expectedLabelValues
  }

  it("should return one matching row (limit 1)") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    //Reducing limit results in truncated metadata response
    val execPlan = PartKeysExec(QueryContext(plannerParams = PlannerParams(sampleLimit = limit - 1)), dummyDispatcher,
      timeseriesDataset.ref, 0, filters, false, now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        response.size shouldEqual 1
        response(0).rows.map { row =>
          val r = row.asInstanceOf[BinaryRecordRowReader]
          response(0).asInstanceOf[SerializedRangeVector]
            .schema.toStringPairs(r.recordBase, r.recordOffset).toMap
        }.toList
      }
    }
    result shouldEqual List(expectedLabelValues(0))
  }

  it("should be able to query labels with filter") {
    val expectedLabels = Array("job", "_metric_", "unicode_tag", "instance")
    val filters = Seq(ColumnFilter("job", Filter.Equals("myCoolService".utf8)))
    val execPlan = LabelNamesExec(QueryContext(), dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 4
        rv.rows.map(row => {
          val br = row.asInstanceOf[BinaryRecordRowReader]
          br.schema.colValues(br.recordBase, br.recordOffset, br.schema.colNames).head
        })
      }
    }
    result.toArray shouldEqual expectedLabels
  }

  it("should be able to query with unicode filter") {
    val filters = Seq(ColumnFilter("unicode_tag", Filter.Equals("uni\u03BCtag".utf8)))
    val execPlan = LabelValuesExec(QueryContext(), dummyDispatcher,
      timeseriesDataset.ref, 0, filters, Seq("job", "unicode_tag"), now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next().asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset)
      }
    }
    result shouldEqual jobQueryResult2
  }

  it("should be able to query label cardinality") {
    // Tests all, LabelCardinalityExec, LabelCardinalityDistConcatExec and LabelCardinalityPresenter
    // Though we will search by ns, ws and metric name, technically we can search by any label in index
    val filters = Seq(ColumnFilter("instance", Filter.Equals("someHost:8787".utf8)))
    val qContext = QueryContext()

    val leafExecPlan = LabelCardinalityExec(qContext, dummyDispatcher,
      timeseriesDataset.ref, 0, filters, now - 5000, now)

    val execPlan = LabelCardinalityReduceExec(qContext, dummyDispatcher, leafExecPlan :: Nil)
    execPlan.addRangeVectorTransformer(new LabelCardinalityPresenter())

    val resp = execPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) => {
        response.size shouldEqual 1
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next().asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset).toMap
      }
    }
    result shouldEqual Map("unicode_tag" -> "2",
                           "_type_" -> "1",
                           "job" -> "1",
                           "instance" -> "1",
                           "_metric_" -> "2")
  }

  it ("should correctly execute metric cardinality query") {
    val k = 2
    val queryContext = QueryContext()
    val dispatcher = new PlanDispatcher {
      override def isLocalCall: Boolean = ???
      override def clusterName: String = ???
      override def dispatch(plan: ExecPlan)
                           (implicit sched: Scheduler): Task[QueryResponse] = {
        plan.execute(memStore, querySession)(sched)
      }
    }

    val leafExecPlan = MetricCardTopkExec(queryContext, dispatcher,
      timeseriesDataset.ref, 0, Seq(/* TODO(a_theimer): shardKeyPrefix? */), 1)

    val reducerExecPlan = MetricCardTopkReduceExec(queryContext, dispatcher, Seq(leafExecPlan), k)
    reducerExecPlan.addRangeVectorTransformer(MetricCardTopkPresenter(k))

    val resp = reducerExecPlan.execute(memStore, querySession).runAsync.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _) =>
        // should only have a single RowVector
        response.size shouldEqual 1

        val rows = response(0).rows().toList
        // should only contain two rows (1 for each of the above metrics)
        rows.size shouldEqual 2
        rows.map(_.getAny(0).asInstanceOf[ZeroCopyUTF8String])
          .toSet shouldEqual Set("http_req_total".utf8, "http_resp_time".utf8)
        // each metric should only have cardinality 1
        rows.foreach(_.getLong(1) shouldEqual 1)
    }
  }
}

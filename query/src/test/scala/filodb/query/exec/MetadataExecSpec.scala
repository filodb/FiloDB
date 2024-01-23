package filodb.query.exec

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.Scheduler.Implicits.global
import monix.reactive.Observable
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import filodb.core.MetricsTestData._
import filodb.core.TestData
import filodb.core.binaryrecord2.BinaryRecordRowReader
import filodb.core.memstore.ratelimit.CardinalityStore
import filodb.core.memstore.{FixedMaxPartitionsEvictionPolicy, SomeData, TimeSeriesMemStore}
import filodb.core.metadata.Schemas
import filodb.core.query._
import filodb.core.store.{ChunkSource, InMemoryMetaStore, NullColumnStore}
import filodb.memory.format.{SeqRowReader, ZeroCopyUTF8String}
import filodb.query._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class MetadataExecSpec extends AnyFunSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  import ZeroCopyUTF8String._

  implicit val defaultPatience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(250, Millis))

  val config = ConfigFactory.load("application_test.conf").getConfig("filodb")
  val queryConfig = QueryConfig(config.getConfig("query"))
  val querySession = QuerySession(QueryContext(), queryConfig)

  val policy = new FixedMaxPartitionsEvictionPolicy(20)
  val memStore = new TimeSeriesMemStore(config, new NullColumnStore, new InMemoryMetaStore(), Some(policy))

  val now = System.currentTimeMillis()
  val numRawSamples = 1000
  val reportingInterval = 10000
  val limit = 2
  val tuples = (numRawSamples until 0).by(-1).map { n =>
    (now - n * reportingInterval, n.toDouble)
  }

  val shardPartKeyLabelValues = Seq(
    Seq(  // shard 0
      ("http_req_total", Map("instance"->"someHost:8787", "job"->"myCoolService",
                             "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_foo_total", Map("instance"->"someHost:8787", "job"->"myCoolService",
                             "unicode_tag" -> "uni\u03BCtag", "_ws_" -> "demo", "_ns_" -> "App-0"))
    ),
    Seq (  // shard 1
      ("http_req_total", Map("instance"->"someHost:9090", "job"->"myCoolService",
                             "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_bar_total", Map("instance"->"someHost:8787", "job"->"myCoolService",
                             "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo", "_ns_" -> "App-0")),
      ("http_req_total-A", Map("instance"->"someHost:9090", "job"->"myCoolService",
                             "unicode_tag" -> "uni\u03C0tag", "_ws_" -> "demo-A", "_ns_" -> "App-A")),
    )
  )

  val addlLabels = Map("_type_" -> "prom-counter")
  val expectedLabelValues = shardPartKeyLabelValues.flatMap { shardSeq =>
    shardSeq.map(pair => pair._2 + ("_metric_" -> pair._1) ++ addlLabels)
  }

  val jobQueryResult1 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03C0tag"))
  val jobQueryResult2 = ArrayBuffer(("job", "myCoolService"), ("unicode_tag", "uni\u03BCtag"))

  implicit val execTimeout = 5.seconds

  // Create one container greater than 8K map, we will create one metric with 50 labels, each label of 200 chars
  val commonLabels = Map("_ws_" -> "testws", "_ns_" -> "testns", "job" ->  "myUniqueService")
  val longLabels = ((0 to 50).map(i => (s"label$i", s"$i" * 200)).toMap ++ commonLabels)
    .map{ case (k, v) => (k.utf8, v.utf8)}

  def initShard(memStore: TimeSeriesMemStore,
                partKeyLabelValues: Seq[Tuple2[String, Map[String, String]]],
                ishard: Int): Unit = {
    val partTagsUTF8s = partKeyLabelValues.map{case (m, t) => (m,  t.map { case (k, v) => (k.utf8, v.utf8)})}
    // NOTE: due to max-chunk-size in storeConf = 100, this will make (numRawSamples / 100) chunks
    // Be sure to reset the builder; it is in an Object so static and shared amongst tests
    builder.reset()
    partTagsUTF8s.map { case (metric, partTagsUTF8) =>
      tuples.map { t => SeqRowReader(Seq(t._1, t._2, metric, partTagsUTF8)) }
        .foreach(builder.addFromReader(_, Schemas.promCounter))
    }
    memStore.setup(timeseriesDatasetMultipleShardKeys.ref, Schemas(Schemas.promCounter), ishard, TestData.storeConf, 1)
    memStore.ingest(timeseriesDatasetMultipleShardKeys.ref, ishard, SomeData(builder.allContainers.head, 0))

    builder.reset()
    tuples.take(10).foreach {
      case (timeStamp: Long, value: Double) =>
        builder.addFromReader(SeqRowReader(Seq(timeStamp, value, "long_labels_metric", longLabels)),
          Schemas.promCounter)
    }
    memStore.ingest(timeseriesDatasetMultipleShardKeys.ref, ishard, SomeData(builder.allContainers.head, 0))

  }

  override def beforeAll(): Unit = {
    for (ishard <- 0 until shardPartKeyLabelValues.size) {
      initShard(memStore, shardPartKeyLabelValues(ishard), ishard)
    }
    memStore.refreshIndexForTesting(timeseriesDatasetMultipleShardKeys.ref)
  }

  override def afterAll(): Unit = {
    memStore.shutdown()
  }

  val dummyDispatcher = new PlanDispatcher {
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = plan.execPlan.execute(memStore,
      QuerySession(QueryContext(), queryConfig))(sched)

    override def clusterName: String = ???

    override def isLocalCall: Boolean = ???

    override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                   source: ChunkSource)(implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  val executeDispatcher = new PlanDispatcher {
    override def isLocalCall: Boolean = ???
    override def clusterName: String = ???
    override def dispatch(plan: ExecPlanWithClientParams, source: ChunkSource)
                         (implicit sched: Scheduler): Task[QueryResponse] = {
      plan.execPlan.execute(memStore, querySession)(sched)
    }

    override def dispatchStreaming(plan: ExecPlanWithClientParams,
                                   source: ChunkSource)(implicit sched: Scheduler): Observable[StreamQueryResponse] = ???
  }

  it ("should read the job names from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("_metric_", Filter.Equals("http_req_total".utf8)),
      ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      LabelValuesExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                      ishard, filters, Seq("job", "unicode_tag"), now-5000, now)
    }.toSeq

    val execPlan = LabelValuesDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _,  _, _, _) => {
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
    val filters = Seq (ColumnFilter("__name__", Filter.Equals("http_req_total1".utf8)),
                       ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map { ishard =>
      PartKeysExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                   ishard, filters, false, now-5000, now)
    }.toSeq

    val execPlan = PartKeysDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    (resp: @unchecked) match {
      case QueryResult(_, _, results, _, _, _, _) => results.size shouldEqual 0
    }
  }

  it("should read the label names/values from timeseriesindex matching the columnfilters") {
    import ZeroCopyUTF8String._
    val filters = Seq (ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      PartKeysExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                   ishard, filters, false, now-5000, now)
    }.toSeq

    val execPlan = PartKeysDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _, _) =>
        response.size shouldEqual 1
        response(0).rows.map { row =>
          val r = row.asInstanceOf[BinaryRecordRowReader]
          response(0).asInstanceOf[SerializedRangeVector]
            .schema.toStringPairs(r.recordBase, r.recordOffset).toMap
        }.toSet
      }
    result shouldEqual expectedLabelValues.toSet
  }

  it("should return one matching row (limit 1)") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    // Reducing limit results in truncated metadata response
    val execPlan = PartKeysExec(
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = limit -1))),
      executeDispatcher,
      timeseriesDatasetMultipleShardKeys.ref, 0, filters, false, now - 5000, now)

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _, _) => {
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

  it("should fail to deserialize a long map that doesn't fit the max container size") {
    import ZeroCopyUTF8String._
    val filters = Seq(ColumnFilter("job", Filter.Equals("myUniqueService".utf8)))

    // Reducing limit results in truncated metadata response
    val execPlan = PartKeysExec(
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = limit -1))),
      executeDispatcher,
      timeseriesDatasetMultipleShardKeys.ref, 0, filters, false, now - 5000, now)

    val queryConfigOverridden = QueryConfig(config.getConfig("query"
    ).withValue("container-size-overrides.filodb-query-exec-metadataexec",
      ConfigValueFactory.fromAnyRef(8 * 1024)))
    val querySessionOverridden = QuerySession(QueryContext(), queryConfigOverridden)
    execPlan.maxRecordContainerSize(queryConfigOverridden) shouldEqual 8192
    val resp = execPlan.execute(memStore, querySessionOverridden).runToFuture.futureValue
    resp match {
      case QueryError(_, _, ex: IllegalArgumentException)  =>
                                          ex.getMessage shouldEqual "requirement failed:" +
                                            " The intermediate or final result is too big. For queries, please try to" +
                                            " add more query filters or time range."
      case _                                                   =>
                                            fail(s"Expected to see an exception for exceeding the default " +
                                              s"container limit of ${execPlan.maxRecordContainerSize(queryConfig)}")
    }


    // Default one with 64K Record container
    val execPlan1 = PartKeysExec(
      QueryContext(plannerParams = PlannerParams(enforcedLimits = PerQueryLimits(execPlanSamples = limit -1))),
      executeDispatcher,
      timeseriesDatasetMultipleShardKeys.ref, 0, filters, false, now - 5000, now)
    execPlan1.maxRecordContainerSize(querySession.queryConfig) shouldEqual 65536
    val resp1 = execPlan1.execute(memStore, querySession).runToFuture.futureValue
    val result = resp1 match {
      case QueryResult(id, _, response, _, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual 1
        val record = rv.rows.next().asInstanceOf[BinaryRecordRowReader]
        rv.asInstanceOf[SerializedRangeVector].schema.toStringPairs(record.recordBase, record.recordOffset).map {
          case (k, v) => (k.utf8, v.utf8)
        }
      }
      case _                                     => fail("Expected to see a QueryResult")
    }
    result.toMap shouldEqual (longLabels
      ++ Map("_metric_".utf8 -> "long_labels_metric".utf8, "_type_".utf8 -> "prom-counter".utf8))
  }

  it ("should be able to query labels with filter") {
    val expectedLabels = Set("job", "_metric_", "unicode_tag", "instance", "_ws_", "_ns_")
    val filters = Seq (ColumnFilter("job", Filter.Equals("myCoolService".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      LabelNamesExec(QueryContext(), executeDispatcher,
        timeseriesDatasetMultipleShardKeys.ref, ishard, filters, now-5000, now)
    }.toSeq

    val execPlan = LabelNamesDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _, _) => {
        val rv = response(0)
        rv.rows.size shouldEqual expectedLabels.size
        rv.rows.map(row => {
          val br = row.asInstanceOf[BinaryRecordRowReader]
          br.schema.colValues(br.recordBase, br.recordOffset, br.schema.colNames).head
        })
      }
    }
    result.toSet shouldEqual expectedLabels
  }

  it ("should be able to query with unicode filter") {
    val filters = Seq (ColumnFilter("unicode_tag", Filter.Equals("uni\u03BCtag".utf8)))

    val leaves = (0 until shardPartKeyLabelValues.size).map{ ishard =>
      LabelValuesExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
                      ishard, filters, Seq("job", "unicode_tag"), now-5000, now)
    }.toSeq

    val execPlan = LabelValuesDistConcatExec(QueryContext(), executeDispatcher, leaves)

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    val result = (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _, _) => {
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
    val filters = Seq(ColumnFilter("_ws_", Filter.Equals("demo")),
                      ColumnFilter("_ns_", Filter.Equals("App-0")),
                      ColumnFilter("_metric_", Filter.Equals("http_req_total")),
    )

    val qContext = QueryContext()

    val leaves = (0 until shardPartKeyLabelValues.size).map { ishard =>
      LabelCardinalityExec(qContext, dummyDispatcher,
        timeseriesDatasetMultipleShardKeys.ref, ishard, filters, now - 5000, now)
    }

    val execPlan = LabelCardinalityReduceExec(qContext, dummyDispatcher, leaves)
    execPlan.addRangeVectorTransformer(new LabelCardinalityPresenter())

    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _, _) =>
        response.size shouldEqual 1
        val rv1 = response(0)
        rv1.rows.size shouldEqual 1
        val record1 = rv1.rows.next().asInstanceOf[BinaryRecordRowReader]
        val result1 = rv1.asInstanceOf[SerializedRangeVector]
                          .schema.toStringPairs(record1.recordBase, record1.recordOffset).toMap

        result1 shouldEqual Map("_ns_" -> "1",
          "unicode_tag" -> "1",
          "job" -> "1",
          "instance" -> "2",
          "_metric_" -> "1",
          "_ws_" -> "1")
    }
  }

  it ("should correctly execute TsCardExec") {
    import filodb.query.exec.TsCardExec._

    case class TestSpec(shardKeyPrefix: Seq[String], numGroupByFields: Int, exp: Seq[(Seq[String], CardCounts)])

    // Note: expected strings are eventually concatenated with a delimiter
    //   and converted to ZeroCopyUTF8Strings.
    Seq(
      TestSpec(Seq(), 1, Seq(
        Seq("demo", "timeseries") -> CardCounts(4,4,4),
        Seq("testws", "timeseries") -> CardCounts(1,1,1),
        Seq("demo-A", "timeseries") -> CardCounts(1,1,1)
        )),
      TestSpec(Seq(), 2, Seq(
        Seq("demo", "App-0", "timeseries") -> CardCounts(4,4,4),
        Seq("testws", "testns", "timeseries") -> CardCounts(1,1,1),
        Seq("demo-A", "App-A", "timeseries") -> CardCounts(1,1,1)
      )),
      TestSpec(Seq(), 3, Seq(
        Seq("demo", "App-0", "http_req_total", "timeseries") -> CardCounts(2,2,2),
        Seq("demo", "App-0", "http_bar_total", "timeseries") -> CardCounts(1,1,1),
        Seq("demo", "App-0", "http_foo_total", "timeseries") -> CardCounts(1,1,1),
        Seq("demo-A", "App-A", "http_req_total-A", "timeseries") -> CardCounts(1,1,1),
        Seq("testws", "testns", "long_labels_metric", "timeseries") -> CardCounts(1,1,1)
      )),
      TestSpec(Seq("demo"), 1, Seq(
        Seq("demo", "timeseries") -> CardCounts(4,4,4))),
      TestSpec(Seq("demo"), 2, Seq(
        Seq("demo", "App-0", "timeseries") -> CardCounts(4,4,4))),
      TestSpec(Seq("demo"), 3, Seq(
        Seq("demo", "App-0", "http_req_total", "timeseries") -> CardCounts(2,2,2),
        Seq("demo", "App-0", "http_bar_total", "timeseries") -> CardCounts(1,1,1),
        Seq("demo", "App-0", "http_foo_total", "timeseries") -> CardCounts(1,1,1)
      )),
      TestSpec(Seq("demo", "App-0"), 2, Seq(
        Seq("demo", "App-0", "timeseries") -> CardCounts(4,4,4)
      )),
      TestSpec(Seq("demo", "App-0"), 3, Seq(
        Seq("demo", "App-0", "http_req_total", "timeseries") -> CardCounts(2,2,2),
        Seq("demo", "App-0", "http_bar_total", "timeseries") -> CardCounts(1,1,1),
        Seq("demo", "App-0", "http_foo_total", "timeseries") -> CardCounts(1,1,1)
      )),
      TestSpec(Seq("demo", "App-0", "http_req_total"), 3, Seq(
        Seq("demo", "App-0", "http_req_total", "timeseries") -> CardCounts(2,2,2)))
    ).foreach{ testSpec =>

      val leavesRaw = (0 until shardPartKeyLabelValues.size).map{ ishard =>
        new TsCardExec(QueryContext(), executeDispatcher,timeseriesDatasetMultipleShardKeys.ref,
          ishard, testSpec.shardKeyPrefix, testSpec.numGroupByFields, "raw", 2)
      }.toSeq
      // UPDATE: Simulating the call to downsample cluster to get longterm metrics as well
      val leavesDownsample = (0 until shardPartKeyLabelValues.size).map { ishard =>
        new TsCardExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
          ishard, testSpec.shardKeyPrefix, testSpec.numGroupByFields, "downsample", 2)
      }.toSeq

      val allLeaves = leavesRaw ++ leavesDownsample
      val execPlan = TsCardReduceExec(QueryContext(), executeDispatcher, allLeaves)

      val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
      val result = (resp: @unchecked) match {
        case QueryResult(id, _, response, _, _, _, _) =>
          // should only have a single RangeVector
          response.size shouldEqual 1

          val resultMap = response(0).rows().map{r =>
            val data = RowData.fromRowReader(r)
            data.group -> data.counts
          }.toSeq

          resultMap shouldEqual testSpec.exp.map { case (prefix, counts) =>
            prefixToGroup(prefix) -> counts
          }
      }
    }
  }

  it ("should add overflow group") {
    import filodb.query.exec.TsCardExec._

    // create a new memstore for the records needed and ingest it
    case class TestSpec(shardKeyPrefix: Seq[String], numGroupByFields: Int, exp: Seq[(Seq[String], CardCounts)])

   val testSpec = TestSpec(Seq(), 3, Seq(
     Seq("demo", "App-0", "http_req_total", "timeseries") -> CardCounts(2, 2, 2),
     Seq("demo", "App-0", "http_bar_total", "timeseries") -> CardCounts(1, 1, 1),
     Seq("demo", "App-0", "http_foo_total", "timeseries") -> CardCounts(1, 1, 1),
     Seq("demo-A", "App-A", "http_req_total-A", "timeseries") -> CardCounts(1, 1, 1),
     Seq("testws", "testns", "long_labels_metric", "timeseries") -> CardCounts(1, 1, 1)
   ))

    val leavesRaw = (0 until shardPartKeyLabelValues.size).map { ishard =>
      new TsCardExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
        ishard, testSpec.shardKeyPrefix, testSpec.numGroupByFields, "raw", 2)
    }

    val leavesDownsample = (0 until shardPartKeyLabelValues.size).map { ishard =>
      new TsCardExec(QueryContext(), executeDispatcher, timeseriesDatasetMultipleShardKeys.ref,
        ishard, testSpec.shardKeyPrefix, testSpec.numGroupByFields, "downsample", 2)
    }

    val allLeaves = leavesRaw ++ leavesDownsample
    val execPlan = TsCardReduceExec(QueryContext(), executeDispatcher, allLeaves, 1)
    val resp = execPlan.execute(memStore, querySession).runToFuture.futureValue
    (resp: @unchecked) match {
      case QueryResult(id, _, response, _, _, _, _) =>
        // should only have a single RangeVector
        response.size shouldEqual 1

        val respRows = response(0).rows().map(x => RowData.fromRowReader(x)).toList
        respRows.size shouldEqual 2
        // should have one overflow prefix
        val overFlowRow = respRows.filter(x => x.group.toString.startsWith(CardinalityStore.OVERFLOW_PREFIX(0)))(0)

        val expectedOverflowGroup = prefixToGroupWithDataset(CardinalityStore.OVERFLOW_PREFIX,
          timeseriesDatasetMultipleShardKeys.ref.dataset)
        overFlowRow.group shouldEqual expectedOverflowGroup

        val nonOverflowRow = respRows.filter(x => !x.group.toString.startsWith(CardinalityStore.OVERFLOW_PREFIX(0)))(0)
        // now check for the count

        val testMap = testSpec.exp.map { case (prefix, counts) =>
          prefixToGroup(prefix) -> counts
        }.toMap

        testMap.contains(nonOverflowRow.group) shouldEqual true
        testMap.get(nonOverflowRow.group).get shouldEqual nonOverflowRow.counts

        var totalCardCountWithoutOverflow = CardCounts(0, 0, 0)
        val cardCountsWithoutNonOverflow = testMap.filter(x => (x._1 != nonOverflowRow.group)).toList

        cardCountsWithoutNonOverflow.map(x => totalCardCountWithoutOverflow = totalCardCountWithoutOverflow.add(x._2))

        totalCardCountWithoutOverflow shouldEqual overFlowRow.counts
    }
  }
}

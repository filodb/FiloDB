package filodb.query.exec

import scala.concurrent.Future

import kamon.Kamon
import kamon.trace.Span
import monix.execution.Scheduler

import filodb.core.DatasetRef
import filodb.core.metadata.Column.ColumnType
import filodb.core.query._
import filodb.memory.format.ZeroCopyUTF8String._
import filodb.memory.format.vectors.{CustomBuckets, MutableHistogram}
import filodb.query._
import filodb.query.AggregationOperator.Avg

case class PromQlRemoteExec(queryEndpoint: String,
                            requestTimeoutMs: Long,
                            queryContext: QueryContext,
                            dispatcher: PlanDispatcher,
                            dataset: DatasetRef,
                            remoteExecHttpClient: RemoteExecHttpClient) extends RemoteExec {
  private val defaultColumns = Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("value", ColumnType.DoubleColumn))

//TODO Don't use PromQL API to talk across clusters
  val columns= Map("histogram" -> Seq(ColumnInfo("timestamp", ColumnType.TimestampColumn),
    ColumnInfo("h", ColumnType.HistogramColumn)),
  Avg.entryName -> (defaultColumns :+  ColumnInfo("count", ColumnType.LongColumn)) ,
  "default" -> defaultColumns, QueryFunctionConstants.stdVal -> (defaultColumns ++
      Seq(ColumnInfo("mean", ColumnType.DoubleColumn), ColumnInfo("count", ColumnType.LongColumn))))

  val recordSchema = Map("histogram" -> SerializedRangeVector.toSchema(columns.get("histogram").get),
    Avg.entryName -> SerializedRangeVector.toSchema(columns.get(Avg.entryName).get),
    "default" -> SerializedRangeVector.toSchema(columns.get("default").get),
    QueryFunctionConstants.stdVal -> SerializedRangeVector.toSchema(columns.get(QueryFunctionConstants.stdVal).get))

  val resultSchema = Map("histogram" -> ResultSchema(columns.get("histogram").get, 1),
    Avg.entryName -> ResultSchema(columns.get(Avg.entryName).get, 1),
    "default" -> ResultSchema(columns.get("default").get, 1),
    QueryFunctionConstants.stdVal -> ResultSchema(columns.get(QueryFunctionConstants.stdVal).get, 1))

  private val builder = SerializedRangeVector.newBuilder()

  override val urlParams = Map("query" -> promQlQueryParams.promQl)

  override def sendHttpRequest(execPlan2Span: Span, httpTimeoutMs: Long)
                              (implicit sched: Scheduler): Future[QueryResponse] = {
    remoteExecHttpClient.httpGet(queryContext.plannerParams.applicationId, queryEndpoint,
      requestTimeoutMs, queryContext.submitTime, getUrlParams())
      .map { response =>
        response.unsafeBody match {
          case Left(error) => QueryError(queryContext.queryId, error.error)
          case Right(successResponse) => toQueryResponse(successResponse.data, queryContext.queryId, execPlan2Span)
        }
      }
  }

  // TODO: Set histogramMap=true and parse histogram maps.  The problem is that code below assumes normal double
  //   schema.  Would need to detect ahead of time to use TransientHistRow(), so we'd need to add schema to output,
  //   and detect it in execute() above.  Need to discuss compatibility issues with Prometheus.
  def toQueryResponse(data: Data, id: String, parentSpan: kamon.trace.Span): QueryResponse = {
    val span = Kamon.spanBuilder(s"create-queryresponse-${getClass.getSimpleName}")
      .asChildOf(parentSpan)
      .tag("query-id", id)
      .start()

    val queryResponse = if (data.result.isEmpty) {
      logger.debug("PromQlRemoteExec generating empty QueryResult as result is empty")
      QueryResult(id, ResultSchema.empty, Seq.empty)
    } else {
      if (data.result.head.aggregateResponse.isDefined) genAggregateResult(data, id)
      else {
        val samples = data.result.head.values.getOrElse(Seq(data.result.head.value.get))
        if (samples.isEmpty) {
          logger.debug("PromQlRemoteExec generating empty QueryResult as samples is empty")
          QueryResult(id, ResultSchema.empty, Seq.empty)
        } else {
          samples.head match {
            // Passing histogramMap = true so DataSampl will be HistSampl for histograms
            case HistSampl(timestamp, buckets) => genHistQueryResult(data, id)
            case _ => genDefaultQueryResult(data, id)
          }
        }
      }
    }
    span.finish()
    queryResponse
  }

  def genAggregateResult(data: Data, id: String): QueryResult = {

    val aggregateResponse = data.result.head.aggregateResponse.get
    if (aggregateResponse.aggregateSampl.isEmpty) QueryResult(id, ResultSchema.empty, Seq.empty)
    else {
      aggregateResponse.aggregateSampl.head match {
        case AvgSampl(timestamp, value, count)           => genAvgQueryResult(data, id)
        case StdValSampl(timestamp, stddev, mean, count) => genStdValQueryResult(data, id)
      }
    }
  }

  def genDefaultQueryResult(data: Data, id: String): QueryResult = {
    val rangeVectors = data.result.map { r =>
      val samples = r.values.getOrElse(Seq(r.value.get))

      val rv = new RangeVector {
        val row = new TransientRow()

        override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map(m => m._1.utf8 -> m._2.utf8))

        override def rows(): RangeVectorCursor = {
          import NoCloseCursor._
          samples.iterator.collect { case v: Sampl =>
            row.setLong(0, v.timestamp * 1000)
            row.setDouble(1, v.value)
            row
          }
        }
        override def numRows: Option[Int] = Option(samples.size)

      }
      SerializedRangeVector(rv, builder, recordSchema.get("default").get, printTree(false))
      // TODO: Handle stitching with verbose flag
    }
    QueryResult(id, resultSchema.get("default").get, rangeVectors)
  }

  def genHistQueryResult(data: Data, id: String): QueryResult = {

    val rangeVectors = data.result.map { r =>
      val samples = r.values.getOrElse(Seq(r.value.get))

      val rv = new RangeVector {
        val row = new TransientHistRow()

        override def key: RangeVectorKey = CustomRangeVectorKey(r.metric.map(m => m._1.utf8 -> m._2.utf8))

        override def rows(): RangeVectorCursor = {
          import NoCloseCursor._

          samples.iterator.collect { case v: HistSampl =>
            row.setLong(0, v.timestamp * 1000)
            val sortedBucketsWithValues = v.buckets.toArray.map { h =>
              if (h._1.toLowerCase.equals("+inf")) (Double.PositiveInfinity, h._2) else (h._1.toDouble, h._2)
            }.sortBy(_._1)
            val hist = MutableHistogram(CustomBuckets(sortedBucketsWithValues.map(_._1)),
              sortedBucketsWithValues.map(_._2))
            row.setValues(v.timestamp * 1000, hist)
            row
          }
        }

        override def numRows: Option[Int] = Option(samples.size)

      }
      SerializedRangeVector(rv, builder, recordSchema.get("histogram").get, printTree(false))
      // TODO: Handle stitching with verbose flag
    }
    QueryResult(id, resultSchema.get("histogram").get, rangeVectors)
  }

  def genAvgQueryResult(data: Data, id: String): QueryResult = {
    val rangeVectors = data.result.map { d =>
      val rv = new RangeVector {
          val row = new AvgAggTransientRow()

          override def key: RangeVectorKey = CustomRangeVectorKey(d.metric.map(m => m._1.utf8 -> m._2.utf8))

          override def rows(): RangeVectorCursor = {
            import NoCloseCursor._
            d.aggregateResponse.get.aggregateSampl.iterator.collect { case a: AvgSampl =>
              row.setLong(0, a.timestamp * 1000)
              row.setDouble(1, a.value)
              row.setLong(2, a.count)
              row
            }
          }
          override def numRows: Option[Int] = Option(d.aggregateResponse.get.aggregateSampl.size)
        }
      SerializedRangeVector(rv, builder, recordSchema.get(Avg.entryName).get, printTree(false))
    }

    // TODO: Handle stitching with verbose flag
    QueryResult(id, resultSchema.get(Avg.entryName).get, rangeVectors)
  }

  def genStdValQueryResult(data: Data, id: String): QueryResult = {
    val rangeVectors = data.result.map { d =>
      val rv = new RangeVector {
        val row = new StdValAggTransientRow()

        override def key: RangeVectorKey = CustomRangeVectorKey(d.metric.map(m => m._1.utf8 -> m._2.utf8))

        override def rows(): RangeVectorCursor = {
          import NoCloseCursor._
          d.aggregateResponse.get.aggregateSampl.iterator.collect { case a: StdValSampl =>
            row.setLong(0, a.timestamp * 1000)
            row.setDouble(1, a.stddev)
            row.setDouble(2, a.mean)
            row.setLong(3, a.count)
            row
          }
        }
        override def numRows: Option[Int] = Option(d.aggregateResponse.get.aggregateSampl.size)
      }
      SerializedRangeVector(rv, builder, recordSchema.get(QueryFunctionConstants.stdVal).get)
    }

    // TODO: Handle stitching with verbose flag
    QueryResult(id, resultSchema.get("stdval").get, rangeVectors)
  }
}

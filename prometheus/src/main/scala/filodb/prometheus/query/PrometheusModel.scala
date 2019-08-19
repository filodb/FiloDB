package filodb.prometheus.query

import remote.RemoteStorage._

import filodb.core.query.{ColumnFilter, Filter, RangeVector}
import filodb.query.{Data, ErrorResponse, ExplainPlanResponse, IntervalSelector, LogicalPlan, QueryResultType,
  RawSeries, Result, Sampl, SuccessResponse}
import filodb.query.exec.ExecPlan

object PrometheusModel {

  /**
    * Converts a prometheus read request to a Seq[LogicalPlan]
    */
  def toFiloDBLogicalPlans(readRequest: ReadRequest): Seq[LogicalPlan] = {
    for { i <- 0 until readRequest.getQueriesCount } yield {
      val q = readRequest.getQueries(i)
      val interval = IntervalSelector(q.getStartTimestampMs, q.getEndTimestampMs)
      val filters = for { j <- 0 until q.getMatchersCount } yield {
        val m = q.getMatchers(j)
        val filter = m.getType match {
          case MatchType.EQUAL => Filter.Equals(m.getValue)
          case MatchType.NOT_EQUAL => Filter.NotEquals(m.getValue)
          case MatchType.REGEX_MATCH => Filter.EqualsRegex(m.getValue)
          case MatchType.REGEX_NO_MATCH => Filter.NotEqualsRegex(m.getValue)
        }
        ColumnFilter(m.getName, filter)
      }
      RawSeries(interval, filters, Nil)
    }
  }

  def toPromReadResponse(qrs: Seq[filodb.query.QueryResult]): Array[Byte] = {
    val b = ReadResponse.newBuilder()
    qrs.foreach(r => b.addResults(toPromQueryResult(r)))
    b.build().toByteArray()
  }

  def toPromQueryResult(qr: filodb.query.QueryResult): QueryResult = {
    val b = QueryResult.newBuilder()
    qr.result.foreach{ srv =>
      b.addTimeseries(toPromTimeSeries(srv))
    }
    b.build()
  }

  /**
    * Used to send out raw data
    */
  def toPromTimeSeries(srv: RangeVector): TimeSeries = {
    val b = TimeSeries.newBuilder()
    srv.key.labelValues.foreach {lv =>
      b.addLabels(LabelPair.newBuilder().setName(lv._1.toString).setValue(lv._2.toString))
    }
    srv.rows.foreach { row =>
      // no need to remove NaN here.
      b.addSamples(Sample.newBuilder().setTimestampMs(row.getLong(0)).setValue(row.getDouble(1)))
    }
    b.build()
  }

  def toPromSuccessResponse(qr: filodb.query.QueryResult, verbose: Boolean): SuccessResponse = {
    SuccessResponse(Data(toPromResultType(qr.resultType),
      qr.result.map(toPromResult(_, verbose)).filter(_.values.nonEmpty)))
  }

  def toPromExplainPlanResponse(ex: ExecPlan): ExplainPlanResponse = {
    ExplainPlanResponse(ex.getPlan())
  }

  def toPromResultType(r: QueryResultType): String = {
    r match {
      case QueryResultType.RangeVectors => "matrix"
      case QueryResultType.InstantVector => "vector"
      case QueryResultType.Scalar => "scalar"
    }
  }

  /**
    * Used to send out HTTP response
    */
  def toPromResult(srv: RangeVector, verbose: Boolean): Result = {
    val tags = srv.key.labelValues.map { case (k, v) => (k.toString, v.toString)} ++
                (if (verbose) Map("_shards_" -> srv.key.sourceShards.mkString(","),
                                  "_partIds_" -> srv.key.partIds.mkString(","))
                else Map.empty)

    Result(tags,
      // remove NaN in HTTP results
      // Known Issue: Until we support NA in our vectors, we may not be able to return NaN as an end-of-time-series
      // in HTTP raw query results.
      Some(
        srv.rows.filter(!_.getDouble(1).isNaN).map { r =>
          Sampl(r.getLong(0) / 1000, r.getDouble(1))
        }.toSeq
      ),
      None
    )
  }

  def toPromErrorResponse(qe: filodb.query.QueryError): ErrorResponse = {
    ErrorResponse(qe.t.getClass.getSimpleName, qe.t.getMessage)
  }

}
package filodb.prometheus.query

import remote.RemoteStorage._

import filodb.core.query.{ColumnFilter, Filter, SerializableRangeVector}
import filodb.query.{IntervalSelector, LogicalPlan, RawSeries}

object PrometheusModel {

  sealed trait PromQueryResponse {
    def status: String
  }

  final case class ErrorResponse(errorType: String, error: String) extends PromQueryResponse {
    val status = "error"
  }

  final case class SuccessResponse(data: Data) extends PromQueryResponse {
    val status = "success"
  }

  final case class Data(resultType: String, result: Seq[Result])

  final case class Result(metric: Map[String, String], values: Seq[Sampl])

  final case class Sampl(timestamp: Long, value: Double)

  /**
    * Converts a prometheus read request to a Seq[LogicalPlan]
    */
  def toFiloDBLogicalPlans(readRequest: ReadRequest): Seq[LogicalPlan] = {
    for { i <- 0 until readRequest.getQueriesCount } yield {
      val q = readRequest.getQueries(i)
      val interval = IntervalSelector(Seq(q.getStartTimestampMs), Seq(q.getEndTimestampMs))
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

  def toPromTimeSeries(srv: SerializableRangeVector): TimeSeries = {
    val b = TimeSeries.newBuilder()
    srv.key.labelValues.foreach {lv =>
      b.addLabels(LabelPair.newBuilder().setName(lv._1.toString).setValue(lv._2.toString))
    }
    srv.rows.foreach { row =>
      b.addSamples(Sample.newBuilder().setTimestampMs(row.getLong(0)).setValue(row.getDouble(1)))
    }
    b.build()
  }

  def toPromSuccessResponse(qr: filodb.query.QueryResult): SuccessResponse = {
    SuccessResponse(Data(qr.resultType.toString, qr.result.map(toPromResult(_))))
  }

  def toPromResult(srv: SerializableRangeVector): Result = {
    Result(srv.key.labelValues.map { case (k, v) => (k.toString, v.toString)},
      srv.rows.map(r => Sampl(r.getLong(0), r.getDouble(1))).toSeq)
  }

  def toPromErrorResponse(qe: filodb.query.QueryError): ErrorResponse = {
    ErrorResponse(qe.t.getClass.getSimpleName, qe.t.getMessage)
  }

}
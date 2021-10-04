package filodb.query

sealed trait PromQueryResponse {
  def status: String
}

final case class ErrorResponse(errorType: String, error: String, status: String = "error",
                               queryStats: Option[Seq[QueryStatistics]]) extends PromQueryResponse

final case class SuccessResponse(data: Data, status: String = "success",
                                 partial: Option[Boolean] = None,
                                 message: Option[String] = None,
                                 queryStats: Option[Seq[QueryStatistics]]) extends PromQueryResponse

final case class ExplainPlanResponse(debugInfo: Seq[String], status: String = "success",
                                     partial: Option[Boolean]= None,
                                     message: Option[String]= None) extends PromQueryResponse

final case class QueryStatistics(group: Seq[String], timeSeriesScanned: Long,
                                 dataBytesScanned: Long, resultBytes: Long)

final case class Data(resultType: String, result: Seq[Result])

final case class MetadataSuccessResponse(data: Seq[Map[String, String]],
                                         status: String = "success",
                                         partial: Option[Boolean]= None,
                                         message: Option[String]= None) extends PromQueryResponse

final case class Result(metric: Map[String, String], values: Option[Seq[DataSampl]], value: Option[DataSampl] = None,
                        aggregateResponse: Option[AggregateResponse] = None)

sealed trait DataSampl

sealed trait AggregateSampl

case class AggregateResponse(function: String, aggregateSampl: Seq[AggregateSampl])

/**
  * Metric value for a given timestamp
  * @param timestamp in seconds since epoch
  * @param value value of metric
  */
final case class Sampl(timestamp: Long, value: Double) extends DataSampl

final case class HistSampl(timestamp: Long, buckets: Map[String, Double]) extends DataSampl

final case class MetadataSampl(values: Map[String, String]) extends DataSampl

final case class LabelSampl(values: Seq[String]) extends DataSampl

final case class AvgSampl(timestamp: Long, value: Double, count: Long) extends AggregateSampl

final case class StdValSampl(timestamp: Long, stddev: Double, mean: Double, count: Long) extends AggregateSampl
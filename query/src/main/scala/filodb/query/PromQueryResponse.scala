package filodb.query

sealed trait PromQueryResponse {
  def status: String
}

final case class ErrorResponse(errorType: String, error: String, status: String = "error",
                               queryStats: Option[Seq[QueryStatistics]]) extends PromQueryResponse

final case class SuccessResponse(data: Data, status: String = "success",
                                 partial: Option[Boolean] = None,
                                 message: Option[String] = None,
                                 queryStats: Option[Seq[QueryStatistics]],
                                 queryWarnings: Option[QueryWarningsResponse]) extends PromQueryResponse

final case class ExplainPlanResponse(debugInfo: Seq[String], status: String = "success",
                                     partial: Option[Boolean]= None,
                                     message: Option[String]= None) extends PromQueryResponse

final case class QueryStatistics(group: Seq[String], timeSeriesScanned: Long,
                                 dataBytesScanned: Long, samplesScanned: Long, resultBytes: Long, cpuNanos: Long)
final case class QueryWarningsResponse(
  execPlanSamples: Int = 0,
  execPlanLeafSamples: Int = 0,
  execPlanResultBytes: Long  = 0,
  groupByCardinality: Int = 0,
  joinQueryCardinality: Int = 0,
  timeSeriesSamplesScannedBytes: Long = 0,
  timeSeriesScanned: Int = 0,
  rawScannedBytes: Long = 0
)

final case class Data(resultType: String, result: Seq[Result])

final case class MetadataSuccessResponse(data: Seq[MetadataSampl],
                                         status: String = "success",
                                         partial: Option[Boolean]= None,
                                         message: Option[String]= None) extends PromQueryResponse

final case class Result(metric: Map[String, String], values: Option[Seq[DataSampl]], value: Option[DataSampl] = None,
                        aggregateResponse: Option[AggregateResponse] = None)

sealed trait DataSampl

sealed trait MetadataSampl

sealed trait AggregateSampl

case class AggregateResponse(function: String, aggregateSampl: Seq[AggregateSampl])

/**
  * Metric value for a given timestamp
  * @param timestamp in seconds since epoch
  * @param value value of metric
  */
final case class Sampl(timestamp: Long, value: Double) extends DataSampl

final case class HistSampl(timestamp: Long, buckets: Map[String, Double]) extends DataSampl

final case class MetadataMapSampl(value: Map[String, String]) extends MetadataSampl

final case class LabelSampl(value: String) extends MetadataSampl

final case class AvgSampl(timestamp: Long, value: Double, count: Long) extends AggregateSampl

final case class StdValSampl(timestamp: Long, stddev: Double, mean: Double, count: Long) extends AggregateSampl

final case class LabelCardinalitySampl(metric: Map[String, String],
                                       cardinality: Seq[Map[String, String]]) extends MetadataSampl

/**
 * @param group map of shardKeyPrefix and its values. Key includes - _ws_, _ns_, __name__
 * @param cardinality map of string and int (cardinality count). Key includes - active, shortTerm, longTerm
 * @param dataset This is the user specified dataset for the cureent row.
 *                Valid value includes - raw, aggregated, recordingrules
 * @param _type This is the internal filodb dataset. Example values - prometheus, prometheus_preagg,
 *              prometheus_rules_longterm, prometheus_rules_1m
 */
final case class TsCardinalitiesSamplV2(group: Map[String, String],
                                        cardinality: Map[String, Long],
                                        dataset: String,
                                        _type: String) extends MetadataSampl
package filodb.query

sealed trait PromQueryResponse {
  def status: String
}

final case class ErrorResponse(errorType: String, error: String, status: String = "error") extends PromQueryResponse

final case class SuccessResponse(data: Data, status: String = "success") extends PromQueryResponse

final case class ExplainPlanResponse(debugInfo: Seq[String], status: String = "success") extends PromQueryResponse

final case class Data(resultType: String, result: Seq[Result])

final case class MetadataSuccessResponse(data: Seq[Map[String, String]],
                                         status: String = "success") extends PromQueryResponse

final case class Result(metric: Map[String, String], values: Option[Seq[DataSampl]], value: Option[DataSampl] = None)

sealed trait DataSampl

/**
  * Metric value for a given timestamp
  * @param timestamp in seconds since epoch
  * @param value value of metric
  */
final case class Sampl(timestamp: Long, value: Double) extends DataSampl

final case class HistSampl(timestamp: Long, buckets: Map[String, Double]) extends DataSampl

final case class MetadataSampl(values: Map[String, String]) extends DataSampl

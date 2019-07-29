package filodb.query

trait TsdbQueryParams

case class PromQlQueryParams(promEndPoint: String, promQl: String, start: Long, step: Long, end: Long, limit: Int,
                             spread: Option[Int] = None, processFailure: Boolean = true) extends TsdbQueryParams

object DummyPromQlQueryParams extends PromQlQueryParams("", "", 0, 0,
  0, 0)